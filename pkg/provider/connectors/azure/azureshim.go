package azure

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	azadmin "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"sassoftware.io/convoy/arke/pkg/util"
)

const STANDARD = "Standard"

// azureClientShim interface for ASB client
type azureClientShim interface {
	Connect() error
	NewSender(string) (azureSenderShim, error)
	// NewReceiver(string, string, int) (azureReceiverShim, error)
	ReceiveMessages(context.Context, string, string, int, chan azureMessageShim, bool) error
	CreateSubscription(context.Context, string, string, *azadmin.CreateSubscriptionOptions) error
	CreateTopic(context.Context, string) error
	CreateRule(context.Context, string, string, string, string) error
	UpdateRule(context.Context, string, string, string, string) error
	DeleteRule(context.Context, string, string, string) error
	ListRules(string, string) ([]azadmin.RuleProperties, error)
	GenerateForwardToName(string) string
}

// azureSenderShim interface for sender
type azureSenderShim interface {
	Close() error
	ScheduleMessage(context.Context, *azservicebus.Message, time.Time) ([]int64, error)
	SendMessage(context.Context, *azservicebus.Message) error
}

// azureMessageShim interface for messages
type azureMessageShim interface {
	Ack(context.Context) error
	Nack(context.Context, bool) error

	ID() string

	SetProperties(map[string]interface{})
	SetProperty(string, interface{})
	Properties() map[string]interface{}
	Property(string) (interface{}, bool)

	SetData([]byte)
	Data() []byte
	DeliveryCount() uint32

	SetContentType(string)
	ContentType() string

	SetLockToken([16]byte)

	ClientSentTime() time.Time
	SetClientSentTime()

	Schedule(context.Context, time.Time) error
	Send(context.Context) error
	SetSender(azureSenderShim)

	DeadLetter(context.Context) error

	RenewLock(context.Context) error
	LockedUntil() time.Time
}

// azureMessage message
type azureMessage struct {
	azureMessageShim
	receivedMessage   *azservicebus.ReceivedMessage
	sendingMessage    *azservicebus.Message
	clientSentTime    time.Time
	sender            azureSenderShim
	receiver          *azureReceiver
	deadLetterEnabled bool // true if DeadLetter is enabled for the subscription this was consumed from
}

// azureClient namespace
type azureClient struct {
	azureClientShim  //nolint:unused
	client           *azservicebus.Client
	adminClient      *azadmin.Client
	connectionString string
	host             string
	username         string
	password         string
	tier             string
}

// azureSender sender
type azureSender struct {
	azureSenderShim //nolint:unused
	sender          *azservicebus.Sender
}

// azureReceiver receiver
type azureReceiver struct {
	receiver      *azservicebus.Receiver
	inFlightCount int
}

// NewAzureClient create a new namespace object
func NewAzureClient(host, username, password string) azureClientShim {
	connStr := fmt.Sprintf("Endpoint=sb://%s/;SharedAccessKeyName=%s;SharedAccessKey=%s",
		host, username, password)
	return &azureClient{host: host, username: username, password: password, connectionString: connStr}
}

// Connect create a connection to the azure namespace
func (ac *azureClient) Connect() error {
	client, err := azservicebus.NewClientFromConnectionString(ac.connectionString, nil)
	if err != nil {
		return err
	}
	ac.client = client
	adminClient, err := azadmin.NewClientFromConnectionString(ac.connectionString, nil)
	if err != nil {
		return err
	}

	ac.adminClient = adminClient

	ctx := context.Background()
	nsResp, _ := adminClient.GetNamespaceProperties(ctx, nil)
	ac.tier = nsResp.SKU
	return nil
}

func (ac *azureClient) GenerateForwardToName(topicName string) string {
	forward := fmt.Sprintf("sb://%s/%s", ac.host, topicName)
	return forward
}

// NewTopic create a new topic
func (ac *azureClient) CreateTopic(ctx context.Context, topicName string) error {

	var err error
	tSize := int32(10240) // 10 GB
	if ac.tier == STANDARD {
		// on standard tier, use a 1GB max size to create a max topic size of 16GB.
		// 1GB per partition, 16 partitions created for topics on standard tier
		tSize = int32(1024)
	}
	// will not partition queues on Premium tier if partitioning was not enabled
	// when creating the namespace
	partioningEnabled := true

	topicResponse, err := ac.adminClient.GetTopic(ctx, topicName, nil)
	if err == nil {
		if topicResponse == nil {
			// topic does not exist, create it
			opts := azadmin.CreateTopicOptions{
				Properties: &azadmin.TopicProperties{
					MaxSizeInMegabytes: &tSize,
					EnablePartitioning: &partioningEnabled,
				},
			}
			_, err = ac.adminClient.CreateTopic(ctx, topicName, &opts)
			if err != nil {
				return err
			}
		}
	} else {
		return err
	}

	return nil
}

// ScheduleAt schedule a message
func (as *azureSender) ScheduleMessage(ctx context.Context, message *azservicebus.Message, when time.Time) ([]int64, error) {
	sbMessages := make([]*azservicebus.Message, 0)
	sbMessages = append(sbMessages, message)
	seq, err := as.sender.ScheduleMessages(ctx, sbMessages, when, nil)
	if err != nil {
		return nil, err
	}
	return seq, nil
}

// Close close the sender connection
func (as *azureSender) Close() error {
	return as.sender.Close(context.Background())
}

// Create create a new subscription if it does not exist
func (ac *azureClient) CreateSubscription(ctx context.Context, topicName, subscriptionName string, opts *azadmin.CreateSubscriptionOptions) error {
	resp, err := ac.adminClient.GetSubscription(ctx, topicName, subscriptionName, nil)
	if err != nil || (resp == nil && err == nil) {
		// create a subscription if error on get or resp and err are nil
		_, err := ac.adminClient.CreateSubscription(ctx, topicName, subscriptionName, opts)
		if err != nil {
			// don't return an error if we get a 409 (entity already exists)
			if strings.Contains(err.Error(), "ERROR CODE: 409") {
				return nil
			}
			util.Logger.Debugf("error creating subscription: %s", err)
			return err
		}
	}
	return nil
}

// ListRules list filter rules on a subscription
func (ac *azureClient) ListRules(topicName, subscriptionName string) ([]azadmin.RuleProperties, error) {
	pager := ac.adminClient.NewListRulesPager(topicName, subscriptionName, nil)
	rules := make([]azadmin.RuleProperties, 0)
	for pager.More() {
		page, err := pager.NextPage(context.TODO())

		if err != nil {
			return rules, err
		}

		rules = append(rules, page.Rules...)
	}
	return rules, nil
}

// NewSender creates a new sender
func (ac *azureClient) NewSender(name string) (azureSenderShim, error) {
	sender, err := ac.client.NewSender(name, nil)
	if err != nil {
		return nil, err
	}
	snd := &azureSender{sender: sender}
	return snd, nil
}

// DeleteRule delete a rule on a subscription
func (ac *azureClient) DeleteRule(_ context.Context, topicName, subscriptionName, ruleName string) error {
	_, err := ac.adminClient.DeleteRule(context.Background(), topicName, subscriptionName, ruleName, nil)
	return err
}

// CreateRule create a rule on a subscription
func (ac *azureClient) CreateRule(ctx context.Context, topicName, subscriptionName, ruleName string, ruleText string) error {
	filter := &azadmin.SQLFilter{Expression: ruleText}
	opts := &azadmin.CreateRuleOptions{Name: &ruleName, Filter: filter}
	_, err := ac.adminClient.CreateRule(ctx, topicName, subscriptionName, opts)
	return err
}

// UpdateRule update a rule on a subscription
func (ac *azureClient) UpdateRule(ctx context.Context, topicName, subscriptionName, ruleName string, ruleText string) error {
	filter := &azadmin.SQLFilter{Expression: ruleText}
	opts := azadmin.RuleProperties{Name: ruleName, Filter: filter}
	_, err := ac.adminClient.UpdateRule(ctx, topicName, subscriptionName, opts)
	return err
}

// Send send a message to a topic
func (as *azureSender) SendMessage(ctx context.Context, message *azservicebus.Message) error {
	return as.sender.SendMessage(ctx, message, nil)
}

// ReceiveMessages receive messages from a subscription
func (ac *azureClient) ReceiveMessages(ctx context.Context, topicName, subscriptionName string, prefetch int, messages chan azureMessageShim, deadLetterEnabled bool) error {
	opts := &azservicebus.ReceiverOptions{}
	receiver, err := ac.client.NewReceiverForSubscription(topicName, subscriptionName, opts)
	if err != nil {
		return err
	}
	defer receiver.Close(ctx)

	rcvr := &azureReceiver{receiver: receiver, inFlightCount: 0}
	for {
		msgReqCnt := prefetch - rcvr.inFlightCount
		if ctx.Err() != nil {
			return nil // context closed
		}
		if msgReqCnt < 1 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		msgs, err := receiver.ReceiveMessages(ctx, msgReqCnt, nil)
		if err != nil {
			if ctx.Err() != nil { // context closed
				return nil
			}
			return err
		}

		for _, msg := range msgs {
			message := &azureMessage{}
			message.receiver = rcvr
			message.receivedMessage = msg
			message.deadLetterEnabled = deadLetterEnabled
			message.receiver.inFlightCount++
			messages <- message
		}
	}
}

// Ack acknowledges a message and removes it from the broker
func (am *azureMessage) Ack(ctx context.Context) error {
	am.receiver.inFlightCount--
	return am.receiver.receiver.CompleteMessage(ctx, am.receivedMessage, nil)
}

// RenewLock renews the lock on a message for
func (am *azureMessage) RenewLock(ctx context.Context) error {
	return am.receiver.receiver.RenewMessageLock(ctx, am.receivedMessage, nil)
}

func (am *azureMessage) LockedUntil() time.Time {
	return *am.receivedMessage.LockedUntil
}

// Nack mark the message as failed and allow it to be consumed again
func (am *azureMessage) Nack(ctx context.Context, requeue bool) error {
	// if we are to requeue, AbandonMessage will requeue
	// if we are not to requeue and dead lettering is enabled, dead letter
	// if we are not to requeue and dead lettering is not enabled, ack the message
	if requeue {
		am.receiver.inFlightCount--
		return am.receiver.receiver.AbandonMessage(ctx, am.receivedMessage, nil)
	} else if am.deadLetterEnabled {
		return am.DeadLetter(ctx)
	}
	return am.Ack(ctx)
}

// DeadLetter send the message to the dead letter subscription
func (am *azureMessage) DeadLetter(ctx context.Context) error {
	reason := "client request"
	opts := &azservicebus.DeadLetterOptions{Reason: &reason}
	am.receiver.inFlightCount--
	return am.receiver.receiver.DeadLetterMessage(ctx, am.receivedMessage, opts)
}

// SetProperties set message properties
func (am *azureMessage) SetProperties(props map[string]interface{}) {
	if am.receivedMessage != nil {
		am.receivedMessage.ApplicationProperties = props
	} else if am.sendingMessage != nil {
		am.sendingMessage.ApplicationProperties = props
	}
}

// SetProperty set a message property
func (am *azureMessage) SetProperty(key string, value interface{}) {
	if am.receivedMessage != nil {
		am.receivedMessage.ApplicationProperties[key] = value
	} else if am.sendingMessage != nil {
		am.sendingMessage.ApplicationProperties[key] = value
	}
}

// Property get a message property
func (am *azureMessage) Property(key string) (interface{}, bool) {
	var val interface{}
	var ok bool
	if am.receivedMessage != nil {
		val, ok = am.receivedMessage.ApplicationProperties[key]
	} else if am.sendingMessage != nil {
		val, ok = am.sendingMessage.ApplicationProperties[key]
	}
	return val, ok
}

// Properties get message properties
func (am *azureMessage) Properties() map[string]interface{} {
	var props map[string]interface{}
	if am.receivedMessage != nil {
		props = am.receivedMessage.ApplicationProperties
	} else if am.sendingMessage != nil {
		props = am.sendingMessage.ApplicationProperties
	}
	return props
}

// ID get Azure message id
func (am *azureMessage) ID() string {
	var messageID string
	if am.receivedMessage != nil {
		messageID = am.receivedMessage.MessageID
	} else if am.sendingMessage != nil && am.sendingMessage.MessageID != nil {
		messageID = *am.sendingMessage.MessageID
	}
	return messageID
}

// SetData set message data
func (am *azureMessage) SetData(data []byte) {
	if am.sendingMessage != nil {
		am.sendingMessage.Body = data
	}
}

// Data returns message data
func (am *azureMessage) Data() []byte {
	if am.receivedMessage != nil {
		return am.receivedMessage.Body
	} else if am.sendingMessage != nil {
		return am.sendingMessage.Body
	}
	return nil
}

// DeliveryCount returns delivery count of message
func (am *azureMessage) DeliveryCount() uint32 {
	if am.receivedMessage != nil {
		return am.receivedMessage.DeliveryCount
	}
	return 0
}

// ContentType returns the content type of the message
func (am *azureMessage) ContentType() string {
	if am.receivedMessage != nil && am.receivedMessage.ContentType != nil {
		return string(*am.receivedMessage.ContentType)
	} else if am.sendingMessage != nil && am.sendingMessage.ContentType != nil {
		return string(*am.sendingMessage.ContentType)
	}
	return ""
}

// SetContentType sets the content type of the message
func (am *azureMessage) SetContentType(contentType string) {
	if am.receivedMessage != nil {
		am.receivedMessage.ContentType = &contentType
	} else if am.sendingMessage != nil {
		am.sendingMessage.ContentType = &contentType
	}
}

// ClientSentTime returns the time when the message was delivered to the client via arke
func (am *azureMessage) ClientSentTime() time.Time {
	return am.clientSentTime
}

// SetClientSentTime sets the time that message was delivered to client. Used for tracking how long the client took to process the message
func (am *azureMessage) SetClientSentTime() {
	am.clientSentTime = time.Now()
}

// Schedule used for delaying when a message will be delivered to a subscriber when it is published
func (am *azureMessage) Schedule(ctx context.Context, when time.Time) error {
	if am.sender == nil {
		return errors.New("message does not have a sender to schedule")
	}
	_, err := am.sender.ScheduleMessage(ctx, am.sendingMessage, when)
	return err
}

// SetSender used to set the message sender when retrying/scheduling a message because of failure
func (am *azureMessage) SetSender(sender azureSenderShim) {
	am.sender = sender
}

// Send publishes a message to the broker
func (am *azureMessage) Send(ctx context.Context) error {
	return am.sender.SendMessage(ctx, am.sendingMessage)
}

// NewAzureMessageWithSender creates a new message and assigns the sender to the message
func NewAzureMessageWithSender(sender azureSenderShim) azureMessageShim {
	msg := &azureMessage{}
	msg.sender = sender
	msg.sendingMessage = &azservicebus.Message{}
	return msg
}

func propertiesToHeaders(props map[string]interface{}) map[string]string {
	headers := make(map[string]string)

	for k, v := range props {
		headers[k] = fmt.Sprintf("%v", v)
	}
	return headers
}
