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

	pb "sassoftware.io/convoy/arke/api"
	"sassoftware.io/convoy/arke/pkg/provider"
	"sassoftware.io/convoy/arke/pkg/util"

	servicebus "github.com/Azure/azure-service-bus-go"
)

const providerName string = "azure"

// GetClientIdentifier Set function as a variable so we can replace the GetClientIdentifier method in unit tests
var GetClientIdentifier = util.GetClientIdentifier

var supportedSourceOptionsList = []string{"MessageTTL", "DeadLetterAddress", "DeadLetterSubject", "Expires"}

var supportedSourceOptions map[string]bool

// NewAzureNS allow overriding the connection for mocking in tests
var NewAzureNS = NewAzureNamespace

// NewAzureMsg allow overriding the connection for mocking in tests
var NewAzureMsg = NewAzureMessage

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
				if bd.ActiveStreams < 1 && lastKnown > 10*time.Second {
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
	azure            azureNamespaceShim
	ClientIdentifier string
	knownTopics      *util.ConcurrentMap
	activeMessages   *util.ConcurrentMap
	connectionConfig *pb.ConnectionConfiguration
	ActiveStreams    int
	consumed         int
	produced         int
	clientDisconnect bool
	lastPubSubEvent  time.Time
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
		util.Logger.Debugf("Acking message %s", msgid)
		err = rm.Complete()

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
		//TODO: Abandon will requeue the message, I don't think that is what
		// we want to do in this case.
		err = rm.Abandon()
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
		timeDelay := time.Now().Add(time.Second * time.Duration(delay))
		util.Logger.Debugf("Retry message[%s](%v)(%v) at %s for %s", rm.GetID(), delay, rm.GetDeliveryCount(), time.Now(), timeDelay)

		// topicEntity, tmErr := bd.topicManager.Get(*ctx, origSource.Address.GetName())
		// topic, _ := bd.namespace.NewTopic(topicEntity.Name)
		topic, tmErr := bd.azure.NewTopic(origSource.Address.GetName())
		if tmErr != nil {
			util.Logger.Debugf("Failed to publish retry message [%s], requeueing instead [%v]", msgid, tmErr.Error())
			_ = rm.Abandon()
			return &pb.Error{Message: fmt.Sprintf("Failed to publish retry message %s, requeueing instead", msgid)}
		}

		// We need to nil the LockToken because we get an unmarshal error
		rm.SetLockToken(nil)
		// Set or update the x-death header which tracks our retry attempts
		if xDeath, ok := rm.GetUserProperty("x-death"); ok {
			var count int
			fmt.Sscanf(xDeath.(string), "[map[count:%d", &count)
			count++
			util.Logger.Debugf("Updating x-death to %d", count)
			rm.SetUserProperty("x-death", fmt.Sprintf("[map[count:%d ]]", count))
		} else {
			rm.SetUserProperty("x-death", "[map[count:1 ]]")
		}
		_, sErr := topic.ScheduleAt(timeDelay, rm)
		if sErr != nil {
			util.Logger.Debugf("Failed to schedule retry message [%s], requeueing instead [%v]", msgid, sErr.Error())
			_ = rm.Abandon()
			return &pb.Error{Message: fmt.Sprintf("Failed to schedule retry message [%s], requeueing instead", msgid)}
		}

		_ = rm.Complete()
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
	}

	connStr := fmt.Sprintf("Endpoint=sb://%s/;SharedAccessKeyName=%s;SharedAccessKey=%s",
		bd.connectionConfig.Host, bd.connectionConfig.GetCredentials().GetUsername(),
		bd.connectionConfig.GetCredentials().GetPassword())

	bd.azure = NewAzureNS(connStr)

	err = bd.azure.Connect()

	if err != nil {
		util.Logger.WarnI("error.brokerconnect", err.Error())
		return &pb.Error{Message: err.Error()}
	}

	bd.knownTopics = util.NewConcurrentMap()

	prov.connections.Add(bd.ClientIdentifier, &bd)

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

	bd.updateLastPubSubEvent()

	topic, topicErr := declareExchange(source.GetAddress(), bd)
	if topicErr != nil {
		return &pb.Error{Message: topicErr.Error()}
	}

	defer func() {
		topic.Close()
	}()

	var suberr error
	subscription, suberr := declareSubscription(source, bd, topic)
	if suberr != nil {
		return &pb.Error{Message: suberr.Error()}
	}

	defer func() {
		subscription.Close()
	}()

	util.Logger.InfoI("info.azureclientsubscribe", bd.ClientIdentifier, subscription.Name(), source.GetAddress().GetName())

	bd.incrementStreamCount()
	defer bd.decrementStreamCount()

	messages := make(chan azureMessageShim)
	// stopChan := make(chan bool)

	// TODO: Need to handle lock expiration, the max we can set is 5 minutes
	// and we have some handlers that run for much longer.
	go func(msgChan chan azureMessageShim, sub azureSubscriptionShim) {
		err := sub.Receive(*ctx, msgChan)
		if err != nil {
			close(msgChan)
		}

		close(msgChan)
	}(messages, subscription)

	for {
		select {
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
			for header, value := range msg.GetUserProperties() {
				// make everything a string
				headers[header] = fmt.Sprintf("%v", value)
			}
			if msg.GetContentType() != "" {
				headers["Content-Type"] = msg.GetContentType()
			}

			message := &pb.Message{Uuid: messageUUID, Body: msg.GetData(), Headers: headers, Address: source.GetAddress()}
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

func declareExchange(address *pb.Address, bd *BrokerDetails) (azureTopicShim, error) {
	// make sure an invalid address type is not sent
	addressType := address.GetType()
	switch address.GetType() {
	case pb.Address_TOPIC:
	case pb.Address_FILTER:
	case pb.Address_QUEUE:
	default:
		util.Logger.WarnI("error.addresstype", addressType)
		return nil, fmt.Errorf("%s is not a valid address type", addressType)
	}

	topicInt, known := bd.exchangeKnown(address.GetName())
	var topic azureTopicShim
	var err error
	if !known {

		topic, err = bd.azure.NewTopic(address.GetName())
		if err != nil {
			return nil, err
		}

		bd.knownTopics.Add(address.GetName(), topic)

	} else {
		topic = topicInt.(azureTopicShim)
	}

	if parent := address.GetParentAddress(); parent != nil {
		_, known = bd.exchangeKnown(parent.GetName())
		if !known {
			parentTopic, err := declareExchange(parent, bd)
			if err != nil {
				util.Logger.WarnI("error.exchangedeclare", err.Error())
			}

			var smOpts []servicebus.SubscriptionManagementOption
			var sOpts []servicebus.SubscriptionOption

			smOpts = append(smOpts, servicebus.SubscriptionWithAutoForward(topic.GetEntity()))

			util.Logger.Debugf("declaring subscription with auto forward for parent %s to child %s\n", parent.GetName(), address.GetName())

			fakeSource := &pb.Source{}
			fakeSource.Name = fmt.Sprintf("%s-foward-%s", address.GetName(), parent.GetName())
			fakeSource.Address = &pb.Address{}
			fakeSource.Address.Subjects = address.GetSubjects()

			// TODO: should we return the error from this declaration?
			declareSubscriptionWithOptions(fakeSource, bd, parentTopic, smOpts, sOpts) //nolint errcheck
			bd.knownTopics.Add(parent.GetName(), parentTopic)
		}
	}

	return topic, nil
}

func declareSubscription(source *pb.Source, bd *BrokerDetails, topic azureTopicShim) (azureSubscriptionShim, error) {

	var smOpts []servicebus.SubscriptionManagementOption
	var sOpts []servicebus.SubscriptionOption
	needDelete := 1

	for option, value := range source.GetOptions() {
		switch option {
		case "MessageTTL":
			val, err := strconv.Atoi(value)
			if err != nil {
				return nil, errors.New("value for MessageTTL option must be a quoted integer")
			}
			ttl := time.Millisecond * time.Duration(val)
			smOpts = append(smOpts, servicebus.SubscriptionWithMessageTimeToLive(&ttl))
		case "Expires":
			val, err := strconv.Atoi(value)
			if err != nil {
				return nil, errors.New("value for Expires option must be a quoted integer")
			}
			exp := time.Millisecond * time.Duration(val)
			smOpts = append(smOpts, servicebus.SubscriptionWithAutoDeleteOnIdle(&exp))
			needDelete = 0
		case "DeadLetterAddress":
			// // TODO: declare topic
			// // TODO: declare subscription
			// // topicEntity, err := bd.topicManager.Get(*ctx, value)
			// // if err != nil {
			// // 	return nil, fmt.Errorf("%s is an invalid DeadLetterAddress", value)
			// // }
			// dlt, err := bd.azure.NewTopic(ctx, value)
			// if err != nil {
			// 	return nil, fmt.Errorf("%s is an invalid DeadLetterAddress", value)
			// }

			// smOpts = append(smOpts, servicebus.SubscriptionWithDeadLetteringOnMessageExpiration())
			// smOpts = append(smOpts, servicebus.SubscriptionWithForwardDeadLetteredMessagesTo(dlt.GetEntity()))
			// // We can't use an auto-delete policy and a Forward DLQ policy, not permited.
			// needDelete = 0
		case "DeadLetterSubject":
			// args["x-dead-letter-routing-key"] = value
		default:
			return nil, fmt.Errorf("%s is an unsupported source option", option)
		}
	}

	// TODO: We may want to set this on Topics and not Subscriptions
	// because of DLQ forward issues.
	if needDelete > 0 {
		// Our default delete timeout will remove an idle queue after 15 days
		autoDeleteTimeout := time.Hour * 24 * 15
		if source.AutoDelete {
			// If the Source is an AutoDelete, then we remove it after 5 minutes of idle
			autoDeleteTimeout = time.Minute * 5
		}
		smOpts = append(smOpts, servicebus.SubscriptionWithAutoDeleteOnIdle(&autoDeleteTimeout))
	}

	sOpts = append(sOpts, servicebus.SubscriptionWithPrefetchCount(uint32(source.GetPrefetchCount())))

	return declareSubscriptionWithOptions(source, bd, topic, smOpts, sOpts)
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

func declareSubscriptionWithOptions(source *pb.Source, bd *BrokerDetails, topic azureTopicShim,
	smOpts []servicebus.SubscriptionManagementOption, sOpts []servicebus.SubscriptionOption) (azureSubscriptionShim, error) {

	// create subscription
	subName := sourceNameToSubName(source.GetName())
	sm, err := bd.azure.NewSubscriptionManager(topic.GetName())
	if err != nil {
		util.Logger.WarnI("error.clientsubscribe", bd.ClientIdentifier, subName, err.Error())
	}

	err = sm.Create(subName, smOpts...)
	if err != nil {
		return nil, err
	}

	routingAndFilterRuleName := "RoutingAndFilterRule"
	routingAndFilterRuleExists := false

	existingRules, err := sm.ListRules(subName)
	if err != nil {
		util.Logger.InfoI("info.rulelist", subName, bd.ClientIdentifier)
	}
	for _, rule := range existingRules {
		if rule.Name == "$Default" {
			// Ignore any errors, we will try again when we subscribe again
			sm.DeleteRule(subName, rule.Name) //nolint errcheck
			continue
		} else if rule.Name == routingAndFilterRuleName {
			routingAndFilterRuleExists = true
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
	// If rule exists: add temp rule, delete RoutingAndFilterRule, create new RoutingAndFilterRule, delete temp rule
	// If rule does not exist, just create the rule
	if actualRule != "" {
		if routingAndFilterRuleExists {
			_, err = sm.PutRule(subName, tmpRuleName, actualRule)
			if err != nil {
				util.Logger.WarnI("error.ruleadd", subName, bd.ClientIdentifier, actualRule, err.Error())
			}
			err = sm.DeleteRule(subName, routingAndFilterRuleName)
			if err != nil {
				util.Logger.WarnI("error.ruledel", subName, bd.ClientIdentifier, actualRule, err.Error())
			}
		}
		_, err = sm.PutRule(subName, routingAndFilterRuleName, actualRule)
		if err != nil {
			util.Logger.WarnI("error.ruleadd", subName, bd.ClientIdentifier, actualRule, err.Error())
		}
		// delete the temporary rule
		if routingAndFilterRuleExists {
			err = sm.DeleteRule(subName, tmpRuleName)
			if err != nil {
				util.Logger.WarnI("error.ruledel", subName, bd.ClientIdentifier, actualRule, err.Error())
			}
		}
	}
	// }

	subscription, err := topic.NewSubscription(subName, sOpts...)

	if err != nil {
		util.Logger.WarnI("error.clientsubscribe", bd.ClientIdentifier, subName, err.Error())
		return nil, err
	}

	return subscription, nil
}

func (prov *azureprovider) Publish(ctx *context.Context, messageChannel <-chan *pb.Message, errChan chan<- *pb.Error) *pb.Error {
	bd, err := prov.getBrokerDetails(*ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	bd.updateLastPubSubEvent()
	bd.incrementStreamCount()
	defer bd.decrementStreamCount()

	for {
		message := <-messageChannel
		if message == nil {
			// nil message means shut it down
			return nil
		}

		topic, topicErr := declareExchange(message.GetAddress(), bd)
		if topicErr != nil {
			errChan <- &pb.Error{
				Message: topicErr.Error(),
				IsFatal: true,
			}
			continue
		}

		azureMessage := NewAzureMsg()
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

		azureMessage.SetUserProperties(headers)

		err = topic.Send(*ctx, azureMessage)

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
	for _, topicName := range bd.knownTopics.GetList() {
		topicInt, ok := bd.knownTopics.Get(topicName)
		if ok {
			topic := topicInt.(azureTopicShim)
			topic.Close()
		}
	}

	prov.connections.Delete(clientIdentifier)
}
