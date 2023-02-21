package azure

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	azadmin "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	pb "sassoftware.io/convoy/arke/api"
	"sassoftware.io/convoy/arke/pkg/provider"
	"sassoftware.io/convoy/arke/pkg/util"
)

const providerName string = "azure"

// GetClientIdentifier Set function as a variable so we can replace the GetClientIdentifier method in unit tests
var GetClientIdentifier = util.GetClientIdentifier

var supportedSourceOptionsList = []string{"MessageTTL", "DeadLetterAddress", "DeadLetterSubject", "Expires"}

var supportedSourceOptions map[string]bool

// NewAZClient allow overriding the connection for mocking in tests
var NewAZClient = NewAzureClient

// NewAzureMsg allow overriding the connection for mocking in tests
// var NewAzureMsg = NewAzureMessage
var NewAzureMsgWithSender = NewAzureMessageWithSender

// var NewAzureMsgWithReceiver = NewAzureMessageWithReceiver

func init() {
	// Register this provider with the Provider factory.
	provider.Register(providerName, NewAzureProvider)

	supportedSourceOptions = make(map[string]bool)
	for _, option := range supportedSourceOptionsList {
		supportedSourceOptions[option] = true
	}
	if !strings.HasSuffix(os.Args[0], ".test") {
		go connectionCleaner()
	}
}

func connectionCleaner() {
	provy, _ := provider.GetProvider("azure")
	prov := provy.(*azureprovider)
	ticker := time.NewTicker(30 * time.Second)
	for {
		<-ticker.C
		for _, connID := range prov.connections.GetList() {
			if conn, ok := prov.connections.Get(connID); ok {
				bd := conn.(*BrokerDetails)
				util.Logger.Debugf("Client %v has %d open streams", connID, bd.ActiveStreams)
				lastKnown := time.Since(bd.lastPubSubEvent)
				if bd.ActiveStreams < 1 && lastKnown > 30*time.Second {
					util.Logger.Debugf("Client %v has had no streams open for %v. Assuming dead. Disconnecting.", connID, lastKnown)
					prov.disconnectClientByIdentifier(connID)
				}
			}
		}
	}
}

type azureprovider struct {
	provider.Provider
	connections *util.ConcurrentMap
}

type BrokerDetails struct {
	sync.Mutex
	azure            azureClientShim
	ClientIdentifier string
	knownTopics      *util.ConcurrentMap
	activeMessages   *util.ConcurrentMap
	connectionConfig *pb.ConnectionConfiguration
	ActiveStreams    int
	consumed         int
	produced         int
	clientDisconnect bool
	lastPubSubEvent  time.Time
	senders          *util.ConcurrentMap
}

func (prov *azureprovider) getBrokerDetails(ctx context.Context) (*BrokerDetails, error) {
	clientIdentifier, err := GetClientIdentifier(ctx)
	if err != nil {
		util.Logger.WarnI("error.noclientuuid", err.Error())
		return &BrokerDetails{}, err
	}

	if bd := prov.getBrokerDetailsByIdentifier(clientIdentifier); bd != nil {
		return bd, nil
	}

	return &BrokerDetails{}, fmt.Errorf("could not retrieve broker details for this connection: %s", clientIdentifier)
}

func (prov *azureprovider) getBrokerDetailsByIdentifier(clientIdentifier string) *BrokerDetails {
	if bd, ok := prov.connections.Get(clientIdentifier); ok {
		return bd.(*BrokerDetails)
	}
	return nil
}

func (prov *azureprovider) ClientExists(clientIdentifier string) bool {
	if bd := prov.getBrokerDetailsByIdentifier(clientIdentifier); bd != nil {
		return true
	}
	return false
}

// NewAzureProvider returns a new amqp091 provider
func NewAzureProvider() provider.Provider {
	connections := util.NewConcurrentMap()
	prov := &azureprovider{connections: connections}
	return prov
}

// Ack ack a message
func (prov *azureprovider) Ack(ctx *context.Context, msgid string) *pb.Error {
	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	// util.Logger.Printf("Ack message with UUID : %s", msg.GetUuid())
	if rmu, ok := bd.activeMessages.Get(msgid); ok {
		rm := rmu.(azureMessageShim)
		util.Logger.Debugf("Client %s acking message %s", bd.ClientIdentifier, msgid)
		err = rm.Ack(*ctx)

		elapsed := time.Since(rm.ClientSentTime()).Microseconds()
		util.DebugNoFormat("method:ack,client:%s,elapsed:%v,time:%v\n",
			bd.ClientIdentifier,
			elapsed,
			time.Now().UnixNano())

		if err != nil {
			util.Logger.WarnI("error.ack", err.Error())

			bd.activeMessages.Delete(msgid)
			errMsg := &pb.Error{
				Message: err.Error(),
				IsFatal: true,
			}
			return errMsg
		}
	} else {
		util.Logger.DebugI("debug.acknomessage", bd.ClientIdentifier, msgid)
		return &pb.Error{Message: fmt.Sprintf("No message with uuid %s", msgid)}
	}

	util.Logger.DebugI("debug.ackmessage", bd.ClientIdentifier, msgid)
	bd.activeMessages.Delete(msgid)
	return nil
}

func (prov *azureprovider) Nack(ctx *context.Context, msgid string) *pb.Error {
	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	if rmu, ok := bd.activeMessages.Get(msgid); ok {
		rm := rmu.(azureMessageShim)

		err := rm.Nack(*ctx, false)
		if err != nil {
			util.Logger.WarnI("error.nack", err.Error())

			bd.activeMessages.Delete(msgid)
			elapsed := time.Since(rm.ClientSentTime()).Microseconds()
			fmt.Printf("method:nack,client:%s,elapsed:%v,time:%v\n",
				bd.ClientIdentifier,
				elapsed,
				time.Now().UnixNano())
			errMsg := &pb.Error{
				Message: err.Error(),
				IsFatal: true,
			}
			return errMsg
		}
	} else {
		util.Logger.DebugI("debug.nacknomessage", bd.ClientIdentifier, msgid)
		return &pb.Error{Message: fmt.Sprintf("No message with uuid %s", msgid)}
	}

	util.Logger.DebugI("debug.nackmessage", bd.ClientIdentifier, msgid)
	bd.activeMessages.Delete(msgid)
	return nil
}

func (prov *azureprovider) Retry(ctx *context.Context, origSource *pb.Source, msgid string, delay int32) *pb.Error {
	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	if rmu, ok := bd.activeMessages.Get(msgid); ok {
		rm := rmu.(azureMessageShim)
		scheduleTime := time.Now().Add(time.Second * time.Duration(delay))
		util.Logger.Debugf("Retry message[%s](%v)(%v) at %s for %s", rm.ID(), delay, rm.DeliveryCount(), time.Now(), scheduleTime)

		err = bd.azure.CreateTopic(*ctx, origSource.Address.GetName())
		if err != nil {
			util.Logger.Debugf("Failed to publish retry message [%s], requeueing instead [%v]", msgid, err.Error())
			_ = rm.Nack(*ctx, true)
			return &pb.Error{Message: fmt.Sprintf("Failed to publish retry message [%s]. Create topic failed. Requeueing instead", msgid)}
		}

		// Set or update the x-death header which tracks our retry attempts
		if xDeath, ok := rm.Property("x-death"); ok {
			var count int
			fmt.Sscanf(xDeath.(string), "[map[count:%d", &count)
			count++
			util.Logger.Debugf("Updating x-death to %d", count)
			rm.SetProperty("x-death", fmt.Sprintf("[map[count:%d ]]", count))
		} else {
			rm.SetProperty("x-death", "[map[count:1 ]]")
		}

		sender, err := bd.createOrGetSenderForAddress(origSource.GetAddress().GetName())
		if err != nil {
			_ = rm.Nack(*ctx, true)
			return &pb.Error{Message: fmt.Sprintf("Failed to schedule retry message [%s], requeueing instead", msgid)}
		}
		msgToSend := NewAzureMsgWithSender(sender)
		msgToSend.SetContentType(rm.ContentType())
		msgToSend.SetData(rm.Data())
		msgToSend.SetProperties(rm.Properties())

		sErr := msgToSend.Schedule(*ctx, scheduleTime)
		if sErr != nil {
			util.Logger.Debugf("Failed to schedule retry message [%s], requeueing instead [%v]", msgid, sErr.Error())
			_ = rm.Nack(*ctx, true)
			return &pb.Error{Message: fmt.Sprintf("Failed to schedule retry message [%s], requeueing instead", msgid)}
		}

		_ = rm.Ack(*ctx)
		util.Logger.DebugI("debug.retrymessage", bd.ClientIdentifier, msgid, delay)
		bd.activeMessages.Delete(msgid)
	} else {
		util.Logger.DebugI("debug.nacknomessage", bd.ClientIdentifier, msgid)
		return &pb.Error{Message: fmt.Sprintf("No message with uuid %s", msgid)}
	}
	return nil
}

// Connect connect to rabbitmq
func (prov *azureprovider) Connect(ctx *context.Context, cf *pb.ConnectionConfiguration, tlsSkipVerify bool) *pb.Error {
	clientIdentifier, err := GetClientIdentifier(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	activeMessages := util.NewConcurrentMap()

	bd := BrokerDetails{
		connectionConfig: cf,
		ClientIdentifier: clientIdentifier,
		activeMessages:   activeMessages,
		produced:         0,
		consumed:         0,
		ActiveStreams:    0,
		clientDisconnect: false,
		lastPubSubEvent:  time.Now(),
		senders:          util.NewConcurrentMap(),
		knownTopics:      util.NewConcurrentMap(),
	}

	bd.azure = NewAZClient(bd.connectionConfig.Host, bd.connectionConfig.GetCredentials().GetUsername(),
		bd.connectionConfig.GetCredentials().GetPassword())

	err = bd.azure.Connect()

	if err != nil {
		util.Logger.WarnI("error.brokerconnect", err.Error())
		return &pb.Error{Message: err.Error()}
	}

	prov.connections.Add(bd.ClientIdentifier, &bd)
	util.Logger.InfoI("info.clientconnect", bd.ClientIdentifier, cf.GetHost())

	return nil
}

// DeadLetter routes the message to a dead letter Address because all retries have failed
func (prov *azureprovider) DeadLetter(ctx *context.Context, origSource *pb.Source, msgid string) *pb.Error {
	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	if rmu, ok := bd.activeMessages.Get(msgid); ok {
		rm := rmu.(azureMessageShim)
		util.Logger.Debugf("DeadLetter message with id [%s].", msgid)
		_ = rm.DeadLetter(*ctx) // Requeue set to false will cause the message to DeadLetter
		bd.activeMessages.Delete(msgid)
	} else {
		util.Logger.Debugf("DeadLetter message with id [%s] failed, message not found in active messages.", msgid)
		return &pb.Error{Message: fmt.Sprintf("DeadLetter message with id [%s] failed, message not found in active messages.", msgid)}
	}

	return nil
}

func (prov *azureprovider) Subscribe(ctx *context.Context, source *pb.Source, messageChannel chan<- *pb.Message, stopChannel <-chan bool) *pb.Error {

	if source.GetAddress().GetName() == "" {
		return &pb.Error{Message: "address name not defined"}
	}

	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	if bd.clientDisconnect {
		return &pb.Error{Message: "client disconnected"}
	}

	newCtx, cancel := context.WithCancel(*ctx)
	defer cancel()

	bd.updateLastPubSubEvent()

	topicName, topicErr := declareExchange(source.GetAddress(), bd)
	if topicErr != nil {
		return &pb.Error{Message: topicErr.Error()}
	}

	opts := source.GetOptions()
	deadLetterEnabled := false
	if _, ok := opts["DeadLetterAddress"]; ok {
		deadLetterEnabled = true
		deadLetterError := prov.setupDeadLetter(newCtx, source)
		if deadLetterError != nil {
			return &pb.Error{Message: deadLetterError.Error()}
		}
	}

	subName, suberr := declareSubscription(source, bd, topicName)
	if suberr != nil {
		return &pb.Error{Message: suberr.Error()}
	}

	util.Logger.InfoI("info.azureclientsubscribe", bd.ClientIdentifier, subName, source.GetAddress().GetName())

	bd.incrementStreamCount()
	defer bd.decrementStreamCount()

	messages := make(chan azureMessageShim)
	// stopChan := make(chan bool)

	// TODO: Need to handle lock expiration, the max we can set is 5 minutes
	// and we have some handlers that run for much longer.
	go func(msgChan chan azureMessageShim) {
		err := bd.azure.ReceiveMessages(newCtx, topicName, subName, int(source.PrefetchCount), msgChan, deadLetterEnabled)
		if err != nil {
			return
		}
		close(msgChan)
	}(messages)

	// 4 minute ticker for renewing locks because that is less than our LockDuration
	ticker := time.NewTicker(4 * time.Minute)
	for {
		select {
		case <-ticker.C:
			inFlightMsgs := bd.activeMessages.GetList()

			for _, msgID := range inFlightMsgs {
				msgRaw, ok := bd.activeMessages.Get(msgID)
				if ok {
					inFlight := msgRaw.(azureMessageShim)
					// keep locking if the client was sent this message less
					// than 30 minutes ago
					if time.Since(inFlight.ClientSentTime()) < 30*time.Minute {
						lockContext, lockCancel := context.WithTimeout(*ctx, 5*time.Second)
						util.Logger.Debugf("Renewing lock for %s for message %s", bd.ClientIdentifier, msgID)
						_ = inFlight.RenewLock(lockContext)
						lockCancel()
					}
				}
			}
		case <-newCtx.Done():
			return nil
		case stop, ok := <-stopChannel:
			if !ok || stop {
				// channel is closed, so stop
				return nil
			}
		case msg, ok := <-messages:
			if !ok {
				// message chan closed
				return nil
			}
			messageUUID := util.GenUUID()
			headers := make(map[string]string)
			for header, value := range msg.Properties() {
				// make everything a string
				headers[header] = fmt.Sprintf("%v", value)
			}
			if msg.ContentType() != "" {
				headers["Content-Type"] = msg.ContentType()
			}

			message := &pb.Message{Uuid: messageUUID, Body: msg.Data(), Headers: headers, Address: source.GetAddress()}
			msg.SetClientSentTime()
			bd.activeMessages.Add(messageUUID, msg)

			messageChannel <- message
			bd.consumed++
		}
	}
}

// Disconnect disconnect from the broker
func (prov *azureprovider) Disconnect(ctx *context.Context) {
	clientIdentifier, err := GetClientIdentifier(*ctx)
	if err != nil {
		return
	}

	prov.disconnectClientByIdentifier(clientIdentifier)
}

func (prov *azureprovider) Stats() *provider.Stats {

	stats := &provider.Stats{}
	stats.Clients = make([]*provider.ClientStats, 0)
	for _, connID := range prov.connections.GetList() {
		clientStat := &provider.ClientStats{}
		connRaw, exists := prov.connections.Get(connID)
		if !exists {
			continue
		}
		conn := connRaw.(*BrokerDetails)
		clientStat.ID = conn.ClientIdentifier
		clientStat.ActiveMessages = conn.activeMessages.Length()
		clientStat.Streams = conn.ActiveStreams
		clientStat.Produced = conn.produced
		clientStat.Consumed = conn.consumed
		stats.Clients = append(stats.Clients, clientStat)
	}
	return stats
}

func declareExchange(address *pb.Address, bd *BrokerDetails) (string, error) {
	// make sure an invalid address type is not sent
	ctx := context.Background()
	addressType := address.GetType()
	switch address.GetType() {
	case pb.Address_TOPIC:
	case pb.Address_FILTER:
	case pb.Address_QUEUE:
	default:
		util.Logger.WarnI("error.addresstype", addressType)
		return "", fmt.Errorf("%s is not a valid address type", addressType)
	}

	var topicName string

	topicNameRaw, known := bd.exchangeKnown(address.GetName())
	// var topic azureTopicShim
	var err error
	if !known {
		topicName = address.GetName()

		err = bd.azure.CreateTopic(ctx, topicName)
		if err != nil {
			return "", err
		}

		bd.knownTopics.Add(topicName, address.GetName())
	} else {
		topicName = topicNameRaw.(string)
	}

	if parent := address.GetParentAddress(); parent != nil {
		_, known = bd.exchangeKnown(parent.GetName())
		if !known {
			parentTopic, err := declareExchange(parent, bd)
			if err != nil {
				util.Logger.WarnI("error.exchangedeclare", err.Error())
			}

			forwardTopicName := bd.azure.GenerateForwardToName(address.GetName())

			subOpts := &azadmin.CreateSubscriptionOptions{}
			subOpts.Properties = &azadmin.SubscriptionProperties{}
			subOpts.Properties.ForwardTo = &forwardTopicName

			util.Logger.Debugf("declaring subscription with auto forward for parent %s to child %s\n", parent.GetName(), forwardTopicName)

			fakeSource := &pb.Source{}
			fakeSource.Name = fmt.Sprintf("%s-foward-%s", address.GetName(), parent.GetName())
			fakeSource.Address = &pb.Address{}
			fakeSource.Address.Subjects = address.GetSubjects()

			// TODO: should we return the error from this declaration?
			declareSubscriptionWithOptions(fakeSource, bd, parentTopic, subOpts) //nolint errcheck
			bd.knownTopics.Add(parent.GetName(), parentTopic)
		}
	}

	return topicName, nil
}

func durationTo8601(dur time.Duration) string {
	minutes := dur / time.Minute
	seconds := (dur % time.Minute) / time.Second
	return fmt.Sprintf("PT%dM%dS", minutes, seconds)
}

func (prov *azureprovider) setupDeadLetter(ctx context.Context, origSource *pb.Source) error {

	opts := origSource.GetOptions()
	if _, ok := opts["DeadLetterAddress"]; !ok {
		return nil
	}

	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return err
	}

	dlqAddress := &pb.Address{
		Subjects: origSource.Address.GetSubjects(),
		Type:     pb.Address_TOPIC,
		Name:     opts["DeadLetterAddress"],
	}

	topicName, err := declareExchange(dlqAddress, bd)
	if err != nil {
		return err
	}

	// setup exchange/queue/binding
	// sourceName := fmt.Sprintf("%s.dlq", origSource.GetName())

	// FYI: we cannot change the subject of messages with DL in azure
	source := &pb.Source{
		Name:    origSource.GetName(),
		Address: dlqAddress,
		// Options: make(map[string]string),
	}

	_, err = declareSubscription(source, bd, topicName)
	if err != nil {
		return err
	}

	return nil

}

func declareSubscription(source *pb.Source, bd *BrokerDetails, topicName string) (string, error) {

	subOpts := &azadmin.CreateSubscriptionOptions{}
	subOpts.Properties = &azadmin.SubscriptionProperties{}
	lockDuration := durationTo8601(5 * time.Minute) // ASB has a 5 minute maximum LockDuration
	subOpts.Properties.LockDuration = &lockDuration
	setAutoDeleteTimeout := true

	for option, value := range source.GetOptions() {
		switch option {
		case "MessageTTL":
			val, err := strconv.Atoi(value)
			if err != nil {
				return "", errors.New("value for MessageTTL option must be a quoted integer")
			}

			ttlString := durationTo8601(time.Duration(val) * time.Millisecond)
			subOpts.Properties.DefaultMessageTimeToLive = &ttlString
		case "Expires":
			val, err := strconv.Atoi(value)
			if err != nil {
				return "", errors.New("value for Expires option must be a quoted integer")
			}
			// 5 minutes is the minimum AutoDeleteOnIdle
			minimumTime := 5 * 60 * 1000
			if val > 0 && val < minimumTime {
				util.Logger.WarnI("warn.azureMinimumExpiresTime", bd.ClientIdentifier, source.GetName(), val)
				val = minimumTime
			}
			if val > 0 {
				expString := durationTo8601(time.Duration(val) * time.Millisecond)
				subOpts.Properties.AutoDeleteOnIdle = &expString
				setAutoDeleteTimeout = false
			}
		case "DeadLetterAddress":
			deadLetter := true
			subOpts.Properties.DeadLetteringOnMessageExpiration = &deadLetter

			forwardTopicName := bd.azure.GenerateForwardToName(value)
			subOpts.Properties.ForwardDeadLetteredMessagesTo = &forwardTopicName
		case "DeadLetterSubject":
			// args["x-dead-letter-routing-key"] = value
		default:
			return "", fmt.Errorf("%s is an unsupported source option", option)
		}
	}

	// TODO: We may want to set this on Topics and not Subscriptions
	// because of DLQ forward issues.
	if setAutoDeleteTimeout {
		// Our default delete timeout will remove an idle queue after 15 days
		// autoDeleteTimeout := time.Hour * 24 * 15
		autoDeleteTimeout := "P15D" // ISO8601 for 15 days
		if source.AutoDelete {
			// If the Source is an AutoDelete, then we remove it after 5 minutes of idle
			autoDeleteTimeout = "PT5M" // ISO8601 for 5 minutes
		}
		subOpts.Properties.AutoDeleteOnIdle = &autoDeleteTimeout
	}

	return declareSubscriptionWithOptions(source, bd, topicName, subOpts)
}

func sourceNameToSubName(name string) string {
	subHash := fmt.Sprintf("%x", sha1.Sum([]byte(name)))
	srcPart := name
	if len(name) > 25 {
		srcPart = srcPart[:25]
	}
	if len(subHash) > 20 {
		subHash = subHash[:20]
	}
	return fmt.Sprintf("%s-%s", srcPart, subHash)
}

func declareSubscriptionWithOptions(source *pb.Source, bd *BrokerDetails, topicName string, subOpts *azadmin.CreateSubscriptionOptions) (string, error) {
	// create subscription
	subName := sourceNameToSubName(source.GetName())

	ctx := context.Background()

	err := bd.azure.CreateSubscription(ctx, topicName, subName, subOpts)

	if err != nil {
		return "", err
	}

	routingAndFilterRuleName := "RoutingAndFilterRule"
	routingAndFilterRuleExists := false

	sqlFilter := &azadmin.SQLFilter{}

	existingRules, err := bd.azure.ListRules(topicName, subName)
	if err != nil {
		util.Logger.InfoI("info.rulelist", subName, bd.ClientIdentifier, err.Error())
	}
	for _, rule := range existingRules {
		if rule.Name == "$Default" {
			// Ignore any errors, we will try again when we subscribe again
			bd.azure.DeleteRule(ctx, topicName, subName, rule.Name) //nolint errcheck
			continue
		} else if rule.Name == routingAndFilterRuleName {
			routingAndFilterRuleExists = true
			sqlFilter = rule.Filter.(*azadmin.SQLFilter)
		}
	}

	var rules []string

	// add rules for routing keys
	// for subjects (routing keys), we need to replace # and * with %. % is the only wildcard in ASB and matches multiple words
	// the subjects then need to be combined in as an OR
	var routingRules []string
	routingKeys := source.GetAddress().GetSubjects()
	for _, key := range routingKeys {
		rule := fmt.Sprintf("user.RoutingKey = '%s'", key)
		if strings.ContainsAny(key, "*#") {
			key = strings.ReplaceAll(strings.ReplaceAll(key, "#", "%"), "*", "%")
			rule = fmt.Sprintf("user.RoutingKey like '%s'", key)
		}
		routingRules = append(routingRules, rule)
	}

	if len(routingRules) > 0 {
		routingRule := fmt.Sprintf("(%s)", strings.Join(routingRules, " OR "))
		rules = append(rules, routingRule)
	}

	if len(source.GetFilters()) > 0 {
		var filterRules []string
		// loop through filters, if any, and OR them together
		for _, filter := range source.GetFilters() {
			var fRules []string
			// loop through mathces and AND or OR them together based on filter type
			for _, match := range filter.GetMatches() {
				header := match.GetName()
				value := match.GetValue()
				rule := fmt.Sprintf(`"%s" = '%s'`, header, value)
				fRules = append(fRules, rule)
			}
			var op string
			if filter.GetType() == pb.Filter_ALL {
				op = " AND "
			} else {
				op = " OR "
			}
			if len(fRules) > 0 {
				filterRules = append(filterRules, fmt.Sprintf("(%s)", strings.Join(fRules, op)))
			}
		}
		if len(filterRules) > 0 {
			compiledFilterRules := fmt.Sprintf("(%s)", strings.Join(filterRules, " OR "))
			rules = append(rules, compiledFilterRules)
		}
	}

	actualRule := strings.Join(rules, " AND ")
	tmpRuleName := routingAndFilterRuleName + ".tmp"
	// we need to recreate the RoutingAndFilterRule rule if it exists, but we need to prevent message loss
	// so we:
	// If rule exists and does not match what we expect: add temp rule, delete RoutingAndFilterRule, create new RoutingAndFilterRule, delete temp rule
	// If rule does not exist, just create the rule
	if actualRule != "" {
		if routingAndFilterRuleExists {
			if sqlFilter.Expression != actualRule {
				// create a temporary rule
				// delete the existing rule
				err = bd.azure.CreateRule(ctx, topicName, subName, tmpRuleName, actualRule)
				if err != nil {
					if !strings.Contains(err.Error(), "409 Conflict") {
						util.Logger.WarnI("error.ruleadd", subName, bd.ClientIdentifier, actualRule, err.Error())
					}
				}
				err = bd.azure.DeleteRule(ctx, topicName, subName, routingAndFilterRuleName)
				if err != nil {
					if !strings.Contains(err.Error(), "404 Not Found") {
						util.Logger.WarnI("error.ruledel", subName, bd.ClientIdentifier, actualRule, err.Error())
					}
				}
			}
		}

		// create the 'real' new rule
		err = bd.azure.CreateRule(ctx, topicName, subName, routingAndFilterRuleName, actualRule)
		if err != nil {
			if !strings.Contains(err.Error(), "409 Conflict") {
				util.Logger.WarnI("error.ruleadd", subName, bd.ClientIdentifier, actualRule, err.Error())
			}
		}
		// delete the temporary rule
		if routingAndFilterRuleExists {
			err = bd.azure.DeleteRule(ctx, topicName, subName, tmpRuleName)
			if err != nil {
				if !strings.Contains(err.Error(), "404 Not Found") {
					util.Logger.WarnI("error.ruledel", subName, bd.ClientIdentifier, actualRule, err.Error())
				}
			}
		}
	}
	// }

	err = bd.azure.CreateSubscription(ctx, topicName, subName, subOpts)

	if err != nil {
		util.Logger.WarnI("error.clientsubscribe", bd.ClientIdentifier, subName, err.Error())
		return subName, err
	}

	return subName, nil
}

func (bd *BrokerDetails) createOrGetSenderForAddress(topicName string) (azureSenderShim, error) {
	var sender azureSenderShim
	var err error
	if sndr, ok := bd.senders.Get(topicName); ok {
		sender = sndr.(azureSenderShim)
	} else {
		sender, err = bd.azure.NewSender(topicName)
		if err == nil {
			bd.senders.Add(topicName, sender)
		}
	}
	return sender, err
}

func (prov *azureprovider) Publish(ctx *context.Context, messageChannel <-chan *pb.Message, errChan chan<- *pb.Error) *pb.Error {
	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	bd.updateLastPubSubEvent()
	bd.incrementStreamCount()
	defer bd.decrementStreamCount()

	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	for {
		message := <-messageChannel
		if message == nil {
			// nil message means shut it down
			return nil
		}

		topicName, err := declareExchange(message.GetAddress(), bd)
		if err != nil {
			errChan <- &pb.Error{
				Message: err.Error(),
				IsFatal: true,
			}
			continue
		}

		sender, err := bd.createOrGetSenderForAddress(topicName)
		if err != nil {
			errChan <- &pb.Error{
				Message: err.Error(),
				IsFatal: true,
			}
			continue
		}

		azureMessage := NewAzureMsgWithSender(sender)
		azureMessage.SetData(message.GetBody())
		headers := make(map[string]interface{})

		for headerName, headerValue := range message.GetHeaders() {
			headers[headerName] = headerValue
			switch headerName {
			case "Content-Type":
				azureMessage.SetContentType(headerValue)
			case "Content-Encoding":
				headers["Content-Encoding"] = headerValue
			}
		}

		for _, key := range message.GetAddress().GetSubjects() {
			headers["RoutingKey"] = key
		}

		azureMessage.SetProperties(headers)

		err = azureMessage.Send(*ctx)

		if err != nil {
			util.Logger.WarnI("error.publish", err.Error())

			errMsg := &pb.Error{
				Message: err.Error(),
				IsFatal: true,
			}
			errChan <- errMsg
		} else {
			util.Logger.DebugI("debug.clientpublished", bd.ClientIdentifier)
			bd.produced++
		}
		errChan <- nil
	}
}

// SupportSourceOptions returns a map[string]bool of support options for Source.Options
func (prov *azureprovider) SupportedSourceOptions() map[string]bool {
	return supportedSourceOptions
}

// WaitForConnect will always return true for this provider if the broker details exist
func (prov *azureprovider) WaitForConnect(ctx *context.Context) bool {
	_, err := prov.getBrokerDetails(*ctx)

	return err == nil
}

func (bd *BrokerDetails) updateLastPubSubEvent() {
	bd.lastPubSubEvent = time.Now()
}

func (bd *BrokerDetails) incrementStreamCount() {
	bd.ActiveStreams++
	bd.updateLastPubSubEvent()
}

func (bd *BrokerDetails) decrementStreamCount() {
	bd.ActiveStreams--
	bd.updateLastPubSubEvent()
}

func (bd *BrokerDetails) exchangeKnown(name string) (interface{}, bool) {

	val, ok := bd.knownTopics.Get(name)
	return val, ok
}

func (prov *azureprovider) disconnectClientByIdentifier(clientIdentifier string) {
	var bd *BrokerDetails
	if bdu, ok := prov.connections.Get(clientIdentifier); ok {
		bd = bdu.(*BrokerDetails)
	} else {
		return
	}

	bd.clientDisconnect = true

	prov.connections.Delete(clientIdentifier)
}
