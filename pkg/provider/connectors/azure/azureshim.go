package azure

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-amqp-common-go/v3/uuid"
	servicebus "github.com/Azure/azure-service-bus-go"
	"sassoftware.io/convoy/arke/pkg/util"
)

// azureNamespaceShim interface for namespace
type azureNamespaceShim interface {
	Connect() error
	NewSubscriptionManager(string) (azureSubscriptionManagerShim, error)
	NewTopic(string) (azureTopicShim, error)
}

// azureTopicShim interface for topic
type azureTopicShim interface {
	Close() error
	GetEntity() *servicebus.TopicEntity
	GetName() string
	NewSubscription(string, ...servicebus.SubscriptionOption) (azureSubscriptionShim, error)
	ScheduleAt(time.Time, ...azureMessageShim) ([]int64, error)
	Send(context.Context, azureMessageShim, ...servicebus.SendOption) error
}

// azureSubscriptionManagerShim interface for subscription manager
type azureSubscriptionManagerShim interface {
	Create(string, ...servicebus.SubscriptionManagementOption) error
	DeleteRule(string, string) error
	ListRules(string) ([]*servicebus.RuleEntity, error)
	PutRule(string, string, string) (*servicebus.RuleEntity, error)
}

// azureSubscriptionShim interface for subscription
type azureSubscriptionShim interface {
	Close() error
	Receive(context.Context, chan azureMessageShim) error
	Name() string
}

// azureMessageShim interface for messages
type azureMessageShim interface {
	Abandon() error
	Complete() error

	GetContentType() string
	GetData() []byte
	GetDeliveryCount() uint32
	GetID() string
	GetUserProperties() map[string]interface{}
	GetUserProperty(string) (interface{}, bool)

	SetContentType(string)
	SetData([]byte)
	SetLockToken(*uuid.UUID)
	SetUserProperties(map[string]interface{})
	SetUserProperty(string, interface{})

	ClientSentTime() time.Time
	SetClientSentTime()
}

// azureMessage message
type azureMessage struct {
	azureMessageShim
	sbMsg          *servicebus.Message
	clientSentTime time.Time
}

// azureNamespace namespace
type azureNamespace struct {
	azureNamespaceShim
	namespace        *servicebus.Namespace
	topicManager     *servicebus.TopicManager
	connectionString string
}

// azureTopic topic
type azureTopic struct {
	azureTopicShim
	topic       *servicebus.Topic
	topicEntity *servicebus.TopicEntity
}

// azureSubscription subscription
type azureSubscription struct {
	subscription *servicebus.Subscription
	name         string
}

// azureSubscriptionManager subscription manager
type azureSubscriptionManager struct {
	azureSubscriptionManagerShim
	subscriptionManager *servicebus.SubscriptionManager
}

// NewAzureNamespace create a new namespace object
func NewAzureNamespace(connStr string) azureNamespaceShim {
	return &azureNamespace{connectionString: connStr}
}

// Connect create a connection to the azure namespace
func (an *azureNamespace) Connect() error {
	ns, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(an.connectionString))
	if err != nil {
		return err
	}
	an.namespace = ns
	an.topicManager = an.namespace.NewTopicManager()
	return nil
}

// NewTopic create a new topic
func (an *azureNamespace) NewTopic(topicName string) (azureTopicShim, error) {
	ctx := context.Background()

	var topicEntity *servicebus.TopicEntity
	var err error

	topicEntity, err = an.topicManager.Get(ctx, topicName)
	if err != nil {
		topicEntity, err = an.topicManager.Put(ctx, topicName)
		if err != nil {
			return nil, err
		}
	}

	topic, err := an.namespace.NewTopic(topicEntity.Name)
	if err != nil {
		return nil, err
	}
	ast := &azureTopic{topic: topic, topicEntity: topicEntity}
	return ast, nil
}

// NewSubscription create a new subscription
func (at *azureTopic) NewSubscription(name string, opts ...servicebus.SubscriptionOption) (azureSubscriptionShim, error) {
	sub, err := at.topic.NewSubscription(name, opts...)
	if err != nil {
		return nil, err
	}
	as := &azureSubscription{subscription: sub, name: name}
	return as, nil
}

// NewSubscriptionManager create a new subscription manager
func (an *azureNamespace) NewSubscriptionManager(topicName string) (azureSubscriptionManagerShim, error) {
	sm, err := an.namespace.NewSubscriptionManager(topicName)
	if err != nil {
		return nil, err
	}
	asm := &azureSubscriptionManager{subscriptionManager: sm}
	return asm, nil
}

// ScheduleAt schedule a message
func (at *azureTopic) ScheduleAt(delay time.Time, messages ...azureMessageShim) ([]int64, error) {
	sbMessages := make([]*servicebus.Message, 0)
	for _, message := range messages {
		sbMessages = append(sbMessages, message.(*azureMessage).sbMsg)
	}
	seq, err := at.topic.ScheduleAt(context.Background(), delay, sbMessages...)
	if err != nil {
		return nil, err
	}
	return seq, nil
}

// Close close the topic connection
func (at *azureTopic) Close() error {
	return at.topic.Close(context.Background())
}

// GetEntity get the topic entity
func (at *azureTopic) GetEntity() *servicebus.TopicEntity {
	return at.topicEntity
}

// GetName get the topic name
func (at *azureTopic) GetName() string {
	return at.topic.Name
}

// Send send a message to a topic
func (at *azureTopic) Send(ctx context.Context, message azureMessageShim, opts ...servicebus.SendOption) error {
	msg := message.(*azureMessage)
	return at.topic.Send(ctx, msg.sbMsg, opts...)
}

// Create create a new subscription if it does not exist
func (asm *azureSubscriptionManager) Create(name string, opts ...servicebus.SubscriptionManagementOption) error {
	ctx := context.Background()
	_, err := asm.subscriptionManager.Get(ctx, name)
	if err != nil {
		_, err = asm.subscriptionManager.Put(ctx, name, opts...)

		if err != nil {
			// don't return an error if we get a 409 (entity already exists)
			if strings.Contains(err.Error(), "error code: 409") {
				return nil
			}
			return err
		}
	}
	return nil
}

// ListRules list filter rules on a subscription
func (asm *azureSubscriptionManager) ListRules(name string) ([]*servicebus.RuleEntity, error) {
	re, err := asm.subscriptionManager.ListRules(context.Background(), name)
	if err != nil {
		return nil, err
	}
	return re, err
}

// DeleteRule delete a rule on a subscription
func (asm *azureSubscriptionManager) DeleteRule(subscriptionName, ruleName string) error {
	return asm.subscriptionManager.DeleteRule(context.Background(), subscriptionName, ruleName)
}

// PutRule create a rule on a subscription
func (asm *azureSubscriptionManager) PutRule(subscriptionName, ruleName string, ruleText string) (*servicebus.RuleEntity, error) {
	filter := &servicebus.SQLFilter{Expression: ruleText}
	return asm.subscriptionManager.PutRule(context.Background(), subscriptionName, ruleName, filter)
}

// Close close the subscription connection
func (as *azureSubscription) Close() error {
	return as.subscription.Close(context.Background())
}

// Receive receive messages on a subscription
func (as *azureSubscription) Receive(ctx context.Context, messages chan azureMessageShim) error {
	// recover sending on closed channel issues
	defer func() error {
		if err := recover(); err != nil {
			util.Logger.Debugf("recovered: %v", err)
			return fmt.Errorf("%s", err)
		}
		return nil
	}() //nolint errcheck

	err := as.subscription.Receive(ctx, servicebus.HandlerFunc(func(ctx context.Context, msg *servicebus.Message) error {
		amsg := &azureMessage{sbMsg: msg}
		messages <- amsg
		return nil
	}))
	return err
}

func (as *azureSubscription) Name() string {
	return as.name
}

func (m *azureMessage) SetData(data []byte) {
	m.sbMsg.Data = data
}

func (m *azureMessage) SetLockToken(token *uuid.UUID) {
	m.sbMsg.LockToken = token
}

func (m *azureMessage) UserProperties(properties map[string]interface{}) {
	m.sbMsg.UserProperties = properties
}

func (m *azureMessage) SetUserProperties(props map[string]interface{}) {
	m.sbMsg.UserProperties = props
}

func (m *azureMessage) SetUserProperty(key string, value interface{}) {
	m.sbMsg.UserProperties[key] = value
}

func (m *azureMessage) GetUserProperty(key string) (interface{}, bool) {
	val, ok := m.sbMsg.UserProperties[key]
	return val, ok
}

func (m *azureMessage) GetUserProperties() map[string]interface{} {
	return m.sbMsg.UserProperties
}

func (m *azureMessage) GetID() string {
	return m.sbMsg.ID
}

func (m *azureMessage) GetData() []byte {
	return m.sbMsg.Data
}

func (m *azureMessage) GetDeliveryCount() uint32 {
	return m.sbMsg.DeliveryCount
}

func (m *azureMessage) GetContentType() string {
	return m.sbMsg.ContentType
}

func (m *azureMessage) SetContentType(contentType string) {
	m.sbMsg.ContentType = contentType
}

func (m *azureMessage) Abandon() error {
	return m.sbMsg.Abandon(context.Background())
}

func (m *azureMessage) Complete() error {
	return m.sbMsg.Complete(context.Background())
}

func (m *azureMessage) ClientSentTime() time.Time {
	return m.clientSentTime
}

func (m *azureMessage) SetClientSentTime() {
	m.clientSentTime = time.Now()
}

func NewAzureMessage() azureMessageShim {
	msg := &azureMessage{}
	msg.sbMsg = &servicebus.Message{}
	return msg
}
