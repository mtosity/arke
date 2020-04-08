package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	pb "sassoftware.io/convoy/arke/api"
	"sassoftware.io/convoy/arke/pkg/provider"

	// Import the connectors so their init functions are executed
	_ "sassoftware.io/convoy/arke/pkg/provider/connectors"
	"sassoftware.io/convoy/arke/pkg/util"
)

// streamMaxSubscribeAttempts maximum number of times to attempt subscribing before we give up
const streamMaxSubscribeAttempts = 10

// GetClientUUID Set function as a variable so we can replace the util.GetClientUUID method in unit tests
var GetClientUUID = util.GetClientUUID

// Map that tracks our connection information
// TODO: This leaks, we need a way to prune this map
// if a client goes away without calling Disconnect().
var connectionMap = util.NewConcurrentMap()

type consumeRecv struct {
	err error
	msg *pb.Consume
}

// ProducerServer producer server struct
type ProducerServer struct {
	TLSSkipVerify bool
}

// ConsumerServer consumer server struct
type ConsumerServer struct {
	TLSSkipVerify bool
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

// Consume Receives a stream of messages (source, ack, nack) and returns a message (message, ackresponse, nackresponse)
func (s *ConsumerServer) Consume(stream pb.Consumer_ConsumeServer) error {
	ctx := stream.Context()
	prov, findErr := findProvider(ctx)
	if prov == nil {
		ftlError := errors.New(findErr.Message)
		util.Logger.ErrorI("error.subscribe", findErr.Message)
		return ftlError
	}

	clientUUID, err := GetClientUUID(ctx)
	if err != nil {
		return err
	}

	var returnError error
	isSubscribing := false

	// the only place stopChan should be closed is in a defer
	// it is used to stop the goroutines for subscribe and sending messages back to the client
	stopChan := make(chan bool)
	defer close(stopChan)

	// stopForLoop is used for errors that require the exiting of Consume
	stopForLoop := make(chan bool)
	defer close(stopForLoop)

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
			rchan <- cnsmRecv
		}(stream, recvChan)

		select {
		case cnsmRecv := <-recvChan:

			if cnsmRecv.err != nil {
				util.Logger.ErrorI("error.consumerecvchan", clientUUID, cnsmRecv.err.Error())
				returnError = err
				break consumeLoop
			}

			if cnsmRecv.msg == nil {
				continue
			}

			if cnsmRecv.msg.GetSrc() != nil {

				if isSubscribing {
					errMsg := fmt.Sprintf("Only one source message allowed per subscribe")
					_ = stream.Send(&pb.ConsumeResponse{Resp: &pb.ConsumeResponse_Msg{Msg: &pb.Message{Error: &pb.Error{Message: errMsg}}}})
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
					_ = stream.Send(&pb.ConsumeResponse{Resp: &pb.ConsumeResponse_Msg{Msg: &pb.Message{Error: &pb.Error{Message: errMsg}}}})
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
							err := stream.Send(resp)
							if err != nil {
								util.Logger.ErrorI("error.streamsend", err.Error())
								*returnErr = err
								*stopFor <- true
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
								util.Logger.Error(fmt.Sprintf("%v", err))
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
								util.Logger.ErrorI("error.streamsubscribemax", clientUUID, streamMaxSubscribeAttempts)
								*stopFor <- true
								*returnErr = fmt.Errorf("Stream reached max subscribe attempts %d", streamMaxSubscribeAttempts)
								return
							}
							err := prov.Subscribe(ctx, source, mc, stopChan)
							if err != nil {
								if clientExists(*ctx) {
									util.Logger.InfoI("info.subscribefailbutclientexists", clientUUID, err.Message)
									connected := prov.WaitForConnect(ctx)
									if connected {
										continue
									}
									util.Logger.ErrorI("error.brokerconnect", err.Message)
								} else {
									util.Logger.Debugf("Client no longer exists. Stopping subcribe.")
								}
								*returnErr = fmt.Errorf(err.GetMessage())
								*stopFor <- true
								return
							}
						}
					}
				}(messageChannel, prov, &ctx, stopChan, &stopForLoop, &returnError)

			} else if cnsmRecv.msg.GetAck() != nil { // Ack or Nack the message
				go func() {
					ackmsg := cnsmRecv.msg.GetAck()
					mcr := &pb.MessageConsumedResponse{Success: true}

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

					stream.Send(&pb.ConsumeResponse{Resp: &pb.ConsumeResponse_ConsumedResponse{ConsumedResponse: mcr}})
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
		return ftlError
	}

	var err error
	var msg *pb.Message
	messageChannel := make(chan *pb.Message)
	errChan := make(chan *pb.Error)

	clientUUID, err := GetClientUUID(ctx)
	if err != nil {
		return err
	}

	var returnError error
	endLoop := false

	for {
		go func(mc chan<- *pb.Message, ec chan<- *pb.Error) {
			for {
				msg, err = stream.Recv()
				if err == io.EOF {
					returnError = nil
					endLoop = true
					break
				}
				if err != nil {
					util.Logger.Debugf("Error on producer stream for client %s: %v", clientUUID, err)
					returnError = err
					endLoop = true
					break
				}

				var resp *pb.MessageResponse

				if len(msg.GetAddress().GetSubjects()) != 1 {

					errMsg := &pb.Error{
						Message: "exactly one subject allowed in an Address with Publish",
						IsFatal: false,
					}

					resp = &pb.MessageResponse{Success: false, Error: errMsg}

				} else {

					mc <- msg
					pubErr := <-errChan
					if pubErr != nil {
						resp = &pb.MessageResponse{Success: false, Error: pubErr}
					} else {
						resp = &pb.MessageResponse{Success: true}
					}
				}
				err = stream.Send(resp)
				if err == io.EOF {
					log.Print(err)
					break
				}
				if err != nil {
					util.Logger.ErrorI("error.streamsend", err.Error())
					returnError = err
					endLoop = true
					break
				}
			}
		}(messageChannel, errChan)

		err := prov.Publish(&ctx, messageChannel, errChan)
		if err != nil {
			if clientExists(ctx) {
				connected := prov.WaitForConnect(&ctx)
				if connected {
					continue
				}
				util.Logger.ErrorI("error.brokerconnect", err.Message)
			} else {
				util.Logger.Debugf("Client no longer exists. Stopping publish.")
			}
			returnError = fmt.Errorf(err.GetMessage())
			break
		}
		if endLoop {
			break
		}
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
	// We don't allow a client to call Connect more than once
	clientUUID, clientErr := GetClientUUID(ctx)
	if clientErr != nil {
		err := errors.New(clientErr.Error())
		return &pb.ConnectResponse{Success: false, Error: nil}, err
	}
	_, exists := connectionMap.Get(clientUUID)
	if exists {
		err := errors.New("can not call Connect more than once. Call Disconnect and try again")
		return &pb.ConnectResponse{Success: false, Error: nil}, err
	}
	errMsg := prov.Connect(&ctx, cf, tlsSkipVerify)
	success := true
	var err error
	if errMsg != nil && errMsg.GetMessage() != "" {
		success = false
		err = errors.New(errMsg.GetMessage())
	}
	if success {
		connectionMap.Add(clientUUID, cf)
	}

	return &pb.ConnectResponse{Success: success, Error: errMsg}, err
}

func brokerDisconnect(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	clientUUID, err := GetClientUUID(ctx)
	if err != nil {
		return &pb.Empty{}, nil
	}
	cf, found := connectionMap.Get(clientUUID)
	if found == true {
		providerType := cf.(*pb.ConnectionConfiguration).GetProvider()
		prov, _ := provider.GetProvider(providerType)
		prov.Disconnect(&ctx)
		connectionMap.Delete(clientUUID)
		util.Logger.InfoI("info.clientdisconnect", clientUUID)
		return &pb.Empty{}, nil
	}

	util.Logger.Debugf("Disconnect called for client %s, but no connection information found.", clientUUID)
	return &pb.Empty{}, nil
}

func findProvider(ctx context.Context) (provider.Provider, *pb.Error) {
	clientUUID, _ := GetClientUUID(ctx)
	cf, found := connectionMap.Get(clientUUID)
	if !found {
		errMsg := &pb.Error{
			Message: "Failed to find connection information.",
			IsFatal: true,
		}
		util.Logger.ErrorI("error.clientnoprovider", clientUUID)
		return nil, errMsg
	}

	providerType := cf.(*pb.ConnectionConfiguration).GetProvider()
	prov, _ := provider.GetProvider(providerType)
	return prov, nil
}

func clientExists(ctx context.Context) bool {
	// We don't allow a client to call Connect more than once
	clientUUID, clientErr := GetClientUUID(ctx)
	if clientErr != nil {
		return false
	}
	_, exists := connectionMap.Get(clientUUID)
	if exists {
		return true
	}
	return false
}
