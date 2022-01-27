package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	pb "sassoftware.io/convoy/arke/api"
	"sassoftware.io/convoy/arke/pkg/provider"

	// Import the connectors so their init functions are executed
	_ "sassoftware.io/convoy/arke/pkg/provider/connectors"
	"sassoftware.io/convoy/arke/pkg/util"
)

// streamMaxSubscribeAttempts maximum number of times to attempt subscribing before we give up
const streamMaxSubscribeAttempts = 10

var GetClientAddr = util.GetClientAddr
var GetClientIdentifier = util.GetClientIdentifier
var SetClientIdentifier = util.SetClientIdentifier
var RemoveClientIdentifier = util.RemoveClientIdentifier
var GetProcessStats = util.GetProcessStats

var NewTimestampPB = util.NewTimestampPB

// Map that tracks our connection information
var connectionMap = util.NewConcurrentMap()
var healthNotifiers = util.NewConcurrentMap()

func init() {
	if !strings.HasSuffix(os.Args[0], ".test") {
		util.Logger.Debugf("Starting server connection watcher")
		go connectionWatcher()
		go util.MonitorProcessStats()
		util.Logger.InfoI("info.hpamonitor")
		healthChan := make(chan pb.HealthStatus_Code)
		go util.MonitorHPA(healthChan)
		go monitorHealthChan(healthChan)

	}
}

func connectionWatcher() {
	// watch connection map
	ticker := time.NewTicker(30 * time.Second)
	for {
		<-ticker.C
		for _, connID := range connectionMap.GetList() {
			if connConf, ok := connectionMap.Get(connID); ok {
				providerType := connConf.(*pb.ConnectionConfiguration).GetProvider()
				if prov, err := provider.GetProvider(providerType); err == nil {
					// if the provider says the client doesn't exists, clean up this dead client
					if !prov.ClientExists(connID) {
						util.Logger.Debugf("Provider says client %s does not exist. Cleaning up dead client.", connID)
						connectionMap.Delete(connID)
					}
				}
			} else {
				// We had it in the list but then couldn't retrieve it, delete it.
				connectionMap.Delete(connID)
			}
		}
	}
}

type consumeRecv struct {
	err error
	msg *pb.Consume
}

// ProducerServer producer server struct
type ProducerServer struct {
	pb.UnimplementedProducerServer
	TLSSkipVerify bool
}

// ConsumerServer consumer server struct
type ConsumerServer struct {
	pb.UnimplementedConsumerServer
	TLSSkipVerify bool
}

type HealthzServer struct {
	pb.UnimplementedHealthzServer
}

// Connect Connect for the producer server
func (s *ProducerServer) Connect(ctx context.Context, cf *pb.ConnectionConfiguration) (*pb.ConnectResponse, error) {
	resp, err := brokerConnect(ctx, cf, s.TLSSkipVerify)
	return resp, err
}

// Connect Connect for the consumer server
func (s *ConsumerServer) Connect(ctx context.Context, cf *pb.ConnectionConfiguration) (*pb.ConnectResponse, error) {
	resp, err := brokerConnect(ctx, cf, s.TLSSkipVerify)
	return resp, err
}

type streamSender struct {
	sync.Mutex
	stream pb.Consumer_ConsumeServer
}

func newStreamSender(stream pb.Consumer_ConsumeServer) *streamSender {
	snd := &streamSender{stream: stream}
	return snd
}

func (snd *streamSender) Send(cr *pb.ConsumeResponse) error {
	snd.Lock()
	defer snd.Unlock()
	err := snd.stream.Send(cr)
	return err
}

// Consume Receives a stream of messages (source, ack, nack) and returns a message (message, ackresponse, nackresponse)
func (s *ConsumerServer) Consume(stream pb.Consumer_ConsumeServer) error {
	sender := newStreamSender(stream)

	ctx := stream.Context()
	prov, findErr := findProvider(ctx)
	if prov == nil {
		ftlError := errors.New(findErr.Message)
		cnsmResp := &pb.ConsumeResponse{Resp: &pb.ConsumeResponse_Error{Error: findErr}}
		_ = sender.Send(cnsmResp)
		util.Logger.DebugI("error.subscribe", findErr.Message)
		return ftlError
	}

	clientIdentifier, err := GetClientIdentifier(ctx)
	if err != nil {
		ciErr := &pb.Error{Message: err.Error(), IsFatal: true}
		cnsmResp := &pb.ConsumeResponse{Resp: &pb.ConsumeResponse_Error{Error: ciErr}}
		_ = sender.Send(cnsmResp)
		util.Logger.DebugI("error.subscribe", ciErr.Message)
		return err
	}

	var returnError error
	isSubscribing := false

	stopChan := make(chan bool)
	stopChanClosed := false
	defer func() {
		if !stopChanClosed {
			close(stopChan)
		}
	}()

	// stopForLoop is used for errors that require the exiting of Consume
	stopForLoop := make(chan bool)
	defer func() {
		close(stopForLoop)
		stopForLoop = nil
	}()

	// messageChannel is used for sending messages from the provider back to Consume
	// for sending back to the client
	messageChannel := make(chan *pb.Message)
	defer close(messageChannel)

	var source *pb.Source

consumeLoop:
	for {
		// stream.Recv in a goroutine so we can send
		// received messages back on a channel and we can
		// use select on that channel and the stopForLoop channel
		// in case of an error
		recvChan := make(chan consumeRecv)
		go func(strm pb.Consumer_ConsumeServer, rchan chan consumeRecv) {
			msg, errer := strm.Recv()
			cnsmRecv := consumeRecv{err: errer, msg: msg}
			timer := time.NewTimer(10 * time.Second)
			select {
			case rchan <- cnsmRecv:
				timer.Stop()
				return
			case <-timer.C:
				timer.Stop()
				return
			}
		}(stream, recvChan)

		select {
		case <-ctx.Done():
			util.Logger.Debugf("Client %v went away.", clientIdentifier)
			stopChanClosed = true
			close(stopChan)
			break consumeLoop
		case cnsmRecv := <-recvChan:

			if cnsmRecv.err != nil {
				if cnsmRecv.err == io.EOF {
					util.Logger.DebugI("error.consumerecvchan", clientIdentifier, cnsmRecv.err.Error())
				} else {
					util.Logger.WarnI("error.consumerecvchan", clientIdentifier, cnsmRecv.err.Error())
				}
				returnError = cnsmRecv.err
				break consumeLoop
			}

			if cnsmRecv.msg == nil {
				continue
			}

			if cnsmRecv.msg.GetSrc() != nil {

				if isSubscribing {
					errMsg := "Only one source message allowed per subscribe"
					_ = sender.Send(&pb.ConsumeResponse{Resp: &pb.ConsumeResponse_Msg{Msg: &pb.Message{Error: &pb.Error{Message: errMsg}}}})
					// return errors.New(errMsg)
					continue
				}

				source = cnsmRecv.msg.GetSrc()

				if source.GetPrefetchCount() < 1 {
					source.PrefetchCount = 1
				}

				// verify source.SourceOptions
				validOptions := prov.SupportedSourceOptions()
				unsupported := make([]string, 0)
				options := source.GetOptions()
				for option := range options {
					if _, ok := validOptions[option]; !ok {
						util.Logger.InfoI("info.unsupportedsourceoption", option)
						unsupported = append(unsupported, option)
					}
				}

				if len(unsupported) > 0 {
					errMsg := fmt.Sprintf("provider does not support the following source options: %s", unsupported)
					_ = sender.Send(&pb.ConsumeResponse{Resp: &pb.ConsumeResponse_Msg{Msg: &pb.Message{Error: &pb.Error{Message: errMsg}}}})
					return errors.New(errMsg)
				}

				isSubscribing = true

				go func(mc <-chan *pb.Message, prov provider.Provider, ctx *context.Context, stopChan chan bool, stopFor *chan bool, returnErr *error) {
					for {
						select {
						case stop, ok := <-stopChan:
							if !ok || stop {
								return
							}
						case message, ok := <-mc:
							if !ok {
								return
							}

							if message.GetAddress() == nil {
								message.Address = source.GetAddress()
							}

							resp := &pb.ConsumeResponse{Resp: &pb.ConsumeResponse_Msg{Msg: message}}
							err := sender.Send(resp)
							if err != nil {
								util.Logger.WarnI("error.streamsend", err.Error(), clientIdentifier)
								*returnErr = err
								if *stopFor != nil {
									*stopFor <- true
								}
								return
							}
						}
					}
				}(messageChannel, prov, &ctx, stopChan, &stopForLoop, &returnError)

				go func(mc chan<- *pb.Message, prov provider.Provider, ctx *context.Context, stopChan chan bool, stopFor *chan bool, returnErr *error) {
					subscribeAttempts := 0
					for {
						defer func() {
							if err := recover(); err != nil {
								util.Logger.Warn(fmt.Sprintf("%v", err))
								// returnError = err
								return
							}
						}()
						// If subscribe ever stops because of a broker error, restart it if the client still exists
						// unless the stream was closed
						select {
						case stop, ok := <-stopChan:
							if !ok || stop {
								return
							}
						default:
							subscribeAttempts++
							// Prevent a subscribe to the provider from being attempted too many times
							if subscribeAttempts == streamMaxSubscribeAttempts {
								util.Logger.WarnI("error.streamsubscribemax", clientIdentifier, streamMaxSubscribeAttempts)
								if *stopFor != nil {
									*stopFor <- true
								}
								*returnErr = fmt.Errorf("stream reached max subscribe attempts %d", streamMaxSubscribeAttempts)
								return
							}
							err := prov.Subscribe(ctx, source, mc, stopChan)
							if err != nil {
								if clientExists(*ctx) {
									util.Logger.InfoI("info.subscribefailbutclientexists", clientIdentifier, err.Message)
									connected := prov.WaitForConnect(ctx)
									if connected {
										continue
									}
									util.Logger.WarnI("error.brokerconnect", err.Message)
								} else {
									util.Logger.Debugf("Client no longer exists. Stopping subcribe.")
								}
								*returnErr = fmt.Errorf(err.GetMessage())
								if *stopFor != nil {
									*stopFor <- true
								}
								return
							}
						}
					}
				}(messageChannel, prov, &ctx, stopChan, &stopForLoop, &returnError)

			} else if cnsmRecv.msg.GetAck() != nil { // Ack or Nack the message
				go func() {
					ackmsg := cnsmRecv.msg.GetAck()
					mcr := &pb.MessageConsumedResponse{Success: true, Uuid: ackmsg.GetUuid()}

					var ackerr *pb.Error

					if ackmsg.GetUuid() == "" {
						ackerr = &pb.Error{Message: "Uuid not set when acking/nacking"}
					} else if ackmsg.GetNack() && ackmsg.GetRequeueDelay() > 0 { // delayed retry
						ackerr = prov.Retry(&ctx, source, ackmsg.GetUuid(), ackmsg.GetRequeueDelay())
					} else if ackmsg.GetNack() { // Nack
						ackerr = prov.Nack(&ctx, ackmsg.GetUuid())
					} else { // Ack
						ackerr = prov.Ack(&ctx, ackmsg.GetUuid())
					}

					if ackerr != nil {
						mcr.Success = false
						mcr.Error = ackerr
					}

					_ = sender.Send(&pb.ConsumeResponse{Resp: &pb.ConsumeResponse_ConsumedResponse{ConsumedResponse: mcr}})
				}()
			}
		case <-stopForLoop:
			break consumeLoop
		}
	}

	return returnError
}

// Publish sends message to the server
func (s *ProducerServer) Publish(stream pb.Producer_PublishServer) error {
	ctx := stream.Context()
	prov, findErr := findProvider(ctx)
	if prov == nil {
		ftlError := errors.New(findErr.Message)
		msgResp := &pb.MessageResponse{Success: false, Error: findErr}
		stream.Send(msgResp)
		return ftlError
	}

	var err error
	var msg *pb.Message

	clientIdentifier, err := GetClientIdentifier(ctx)
	if err != nil {
		ciError := &pb.Error{Message: err.Error(), IsFatal: true}
		msgResp := &pb.MessageResponse{Success: false, Error: ciError}
		stream.Send(msgResp)
		return err
	}

	var returnError error
	// stopPublish should only be set to true if:
	// - the client disconnects
	// - we determine the client to be dead
	// - the client stops the stream
	stopPublish := false

	// loop until either the client no longer exists or the stream is dead
	for {
		if !clientExists(ctx) {
			util.Logger.Debugf("client %v no longer exists, stopping publish", clientIdentifier)
			break
		}
		messageChannel := make(chan *pb.Message)
		errChan := make(chan *pb.Error)
		go func(mc chan<- *pb.Message, ec chan<- *pb.Error) {
			// close the channel so the prov.Publish knows to stop
			defer close(mc)
			stopPubFunc := false

			for {
				if stopPubFunc {
					return
				}
				msg, err = stream.Recv()
				if err == io.EOF {
					returnError = nil
					stopPublish = true
					return
				}
				if err != nil {
					util.Logger.Debugf("Error on producer stream for client %s: %v", clientIdentifier, err)
					returnError = err
					stopPublish = true
					return
				}

				var resp *pb.MessageResponse

				if len(msg.GetAddress().GetSubjects()) != 1 {

					errMsg := &pb.Error{
						Message: "exactly one subject allowed in an Address with Publish",
						IsFatal: false,
					}

					resp = &pb.MessageResponse{Success: false, Error: errMsg}

				} else {
					timer := time.NewTimer(30 * time.Second)
					select {
					case errChanErr := <-errChan:
						resp = &pb.MessageResponse{Success: false, Error: errChanErr}
						stopPubFunc = true
					case mc <- msg:
						pubErr := <-errChan
						if pubErr != nil {
							resp = &pb.MessageResponse{Success: false, Error: pubErr}
						} else {
							resp = &pb.MessageResponse{Success: true}
						}
					case <-timer.C:
						errMsg := &pb.Error{Message: "failed to send message to provider for publishing", IsFatal: false}
						resp = &pb.MessageResponse{Success: false, Error: errMsg}
						stopPubFunc = true
					}
					timer.Stop()

				}

				err = stream.Send(resp)
				if err == io.EOF {
					stopPublish = true
					return
				}

				if err != nil {
					util.Logger.WarnI("error.streamsend", err.Error(), clientIdentifier)
					returnError = err
					stopPublish = true
					return
				}

				if stopPubFunc {
					return
				}
			}
		}(messageChannel, errChan)

		err := prov.Publish(&ctx, messageChannel, errChan)
		if err != nil {
			errChan <- err
			if clientExists(ctx) {
				connected := prov.WaitForConnect(&ctx)
				if connected {
					continue
				}
				util.Logger.WarnI("error.brokerconnect", err.Message)
			} else {
				util.Logger.Debugf("Client no longer exists. Stopping publish.")
				stopPublish = true
			}
		}
		if stopPublish {
			break
		}
	}

	// We must try to send a response to the client or the stream will stay open
	// with no messages being consumed
	if returnError != nil {
		errMsg := &pb.Error{Message: returnError.Error()}

		resp := &pb.MessageResponse{Success: false, Error: errMsg}

		// we don't care if the send fails here
		stream.Send(resp)
	}

	return returnError
}

// Disconnect disconnect from the consumer server
func (s *ConsumerServer) Disconnect(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	retVal, _ := brokerDisconnect(ctx, empty)
	return retVal, nil
}

// Disconnect disconnect from the producer server
func (s *ProducerServer) Disconnect(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	retVal, _ := brokerDisconnect(ctx, empty)
	return retVal, nil
}

func brokerConnect(ctx context.Context, cf *pb.ConnectionConfiguration, tlsSkipVerify bool) (*pb.ConnectResponse, error) {
	prov, er := provider.GetProvider(cf.GetProvider())
	if er != nil {
		errMsg := &pb.Error{
			Message: er.Error(),
			IsFatal: true,
		}
		return &pb.ConnectResponse{Success: false, Error: errMsg}, er
	}

	// Check for a client address
	clientAddr, clientAddrErr := GetClientAddr(ctx)
	if clientAddrErr != nil {
		err := errors.New(clientAddrErr.Error())
		return &pb.ConnectResponse{Success: false, Error: nil}, err
	}

	clientName := cf.GetClientName()
	// If we have a client identifier at this point then we've likely already connected
	clientIdentifier, clientErr := GetClientIdentifier(ctx)
	if clientErr != nil {
		if clientName == "" {
			clientName = clientAddr
		}
		clientIdentifier, clientErr = SetClientIdentifier(ctx, clientName)
		if clientErr != nil {
			err := errors.New(clientErr.Error())
			return &pb.ConnectResponse{Success: false, Error: nil}, err
		}
	}
	// If we find a clientIdentifier, then we are already connected
	// and don't need to connect twice.
	_, exists := connectionMap.Get(clientIdentifier)
	if exists {
		util.Logger.Debugf("Client %s called Connect more than once.", clientIdentifier)
		return &pb.ConnectResponse{Success: true, Error: nil}, nil
	}
	errMsg := prov.Connect(&ctx, cf, tlsSkipVerify)
	success := true
	var err error
	if errMsg != nil && errMsg.GetMessage() != "" {
		success = false
		err = errors.New(errMsg.GetMessage())
	}
	if success {
		connectionMap.Add(clientIdentifier, cf)
	}

	return &pb.ConnectResponse{Success: success, Error: errMsg}, err
}

func brokerDisconnect(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	clientIdentifier, err := GetClientIdentifier(ctx)
	if err != nil {
		return &pb.Empty{}, nil
	}
	cf, found := connectionMap.Get(clientIdentifier)
	if found {
		providerType := cf.(*pb.ConnectionConfiguration).GetProvider()
		prov, _ := provider.GetProvider(providerType)
		prov.Disconnect(&ctx)
		connectionMap.Delete(clientIdentifier)
		util.Logger.InfoI("info.clientdisconnect", clientIdentifier)
		return &pb.Empty{}, nil
	}

	util.Logger.Debugf("Disconnect called for client %s, but no connection information found.", clientIdentifier)
	return &pb.Empty{}, nil
}

func findProvider(ctx context.Context) (provider.Provider, *pb.Error) {
	clientIdentifier, err := GetClientIdentifier(ctx)
	if err != nil {
		errMsg := &pb.Error{
			Message: err.Error(),
			IsFatal: true,
		}
		util.Logger.WarnI("error.clientfailedidentifier", clientIdentifier, err.Error())
		return nil, errMsg
	}

	cf, found := connectionMap.Get(clientIdentifier)
	if !found {
		errMsg := &pb.Error{
			Message: "Failed to find connection information.",
			IsFatal: true,
		}
		util.Logger.WarnI("error.clientnoprovider", clientIdentifier)
		return nil, errMsg
	}

	providerType := cf.(*pb.ConnectionConfiguration).GetProvider()
	prov, _ := provider.GetProvider(providerType)
	return prov, nil
}

func clientExists(ctx context.Context) bool {
	// We don't allow a client to call Connect more than once
	clientIdentifier, clientErr := GetClientIdentifier(ctx)
	if clientErr != nil {
		return false
	}
	_, exists := connectionMap.Get(clientIdentifier)

	return exists
}

func notifyHealth(clientAddr string, receiver chan pb.HealthStatus_Code) {
	// only allow one notifier per client
	if recInt, ok := healthNotifiers.Get(clientAddr); ok {
		rec := recInt.(chan pb.HealthStatus_Code)
		close(rec)
		healthNotifiers.Delete(clientAddr)
	}
	healthNotifiers.Add(clientAddr, receiver)
}

func monitorHealthChan(receiver chan pb.HealthStatus_Code) {
	for code := range receiver {
		for _, clientAddr := range healthNotifiers.GetList() {
			if notifierInt, ok := healthNotifiers.Get(clientAddr); ok {
				notifier := notifierInt.(chan pb.HealthStatus_Code)
				notifier <- code
			}
		}
	}
}

func (s *HealthzServer) Check(stream pb.Healthz_CheckServer) error {

	ctx := stream.Context()

	clientAddr, err := GetClientAddr(ctx)
	if err != nil {
		return err
	}

	notifyHealthChan := make(chan pb.HealthStatus_Code)
	notifyHealth(clientAddr, notifyHealthChan)
	defer func() {
		healthNotifiers.Delete(clientAddr)
		close(notifyHealthChan)
	}()

	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				break
			}

			if check := msg.GetCheck(); check != nil {
				// client asked for a health check
				util.Logger.Debugf("healthz check requested for %s with uuid %s", clientAddr, check.GetUuid())
				hlth := &pb.Health{}
				hs := &pb.Health_Status{}
				hs.Status = &pb.HealthStatus{}
				hs.Status.Uuid = check.GetUuid()
				hs.Status.Code = pb.HealthStatus_OK

				processStats := GetProcessStats()
				// if mem usage > 90% or cpu usage has been high for an extended period then report unhealthy
				if processStats.MaxMemory > 0 && (processStats.MemoryAverage)/float64(processStats.MaxMemory) > 0.9 {
					hs.Status.Code = pb.HealthStatus_UNHEALTHY
				} else if processStats.CPUUsageAverage/float64(runtime.NumCPU()) > 90 { // cpu usage > 90% per cpu
					hs.Status.Code = pb.HealthStatus_UNHEALTHY
				}

				// set the time right before sending the response
				hs.Status.Time = NewTimestampPB()
				hlth.Resp = hs
				stream.Send(hlth)
			} else if status := msg.GetStatus(); status != nil {
				// TODO: we are going to do nothing for now, but we need to determine
				// if there are any actual scenarios for us caring about a status response
				// from a client. Possible arke->arke status messages in the future?
				util.Logger.Debugf("client %s status is %v", clientAddr, status.GetCode())
			}
		}
	}()

	for {
		done := false

		select {
		case code := <-notifyHealthChan:
			util.Logger.Debugf("Internal health notification received. Sending %s to %s", code.String(), clientAddr)
			hlth := &pb.Health{}
			hs := &pb.Health_Status{}
			hs.Status = &pb.HealthStatus{}
			hs.Status.Code = code
			hs.Status.Time = NewTimestampPB()
			hlth.Resp = hs
			stream.Send(hlth)
		case <-ctx.Done():
			done = true
		}

		if done {
			break
		}
	}
	return nil
}
