package amqp091

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	pb "sassoftware.io/viya/arke/api"
	"sassoftware.io/viya/arke/i18n"
	"sassoftware.io/viya/arke/pkg/provider"
	"sassoftware.io/viya/arke/pkg/util"
	"sassoftware.io/viya/arke/pkg/util/tracing"
)

const providerName string = "amqp091"
const trustedCerts = "SAS_TRUSTED_CA_CERTIFICATES_PEM_FILE"

var supportedSourceOptionsList = []string{"MessageTTL", "DeadLetterAddress", "DeadLetterSubject", "Expires"}

var supportedSourceOptions map[string]bool

// NewAmqpConn091 allow overriding the connection for mocking in tests
var NewAmqpConn091 = NewAmqp091Connection

// GetClientIdentifier Set function as a variable so we can replace the GetClientIdentifier method in unit tests
var GetClientIdentifier = util.GetClientIdentifier

type amqp091provider struct {
	provider.Provider
	tlsConfig   *tls.Config
	connections *util.ConcurrentMap
}

// BrokerDetails struct houses connection specific information for the broker
type BrokerDetails struct {
	sync.Mutex
	Connection       amqp091ConnectionShim
	ErrorChannel     chan amqp091Error
	RetryChannel     *amqp091ChannelShim
	ClientIdentifier string
	knownExchanges   *util.ConcurrentMap
	knownQueues      *util.ConcurrentMap
	knownBindings    *util.ConcurrentMap
	activeMessages   *util.ConcurrentMap
	state            uint16
	connectionConfig *pb.ConnectionConfiguration
	tlsSkipVerify    bool
	ActiveStreams    int
	consumed         int
	produced         int
	clientDisconnect bool
	lastPubSubEvent  time.Time
	tlsConfig        *tls.Config
	tlsEnabled       bool
	shutdownChan     chan bool
}

func init() {
	// Register this provider with the Provider factory.
	provider.Register(providerName, NewAMQP091Provider)

	supportedSourceOptions = make(map[string]bool)
	for _, option := range supportedSourceOptionsList {
		supportedSourceOptions[option] = true
	}
	if !strings.HasSuffix(os.Args[0], ".test") {
		go connectionCleaner()
	}
}

/*
 * AMQP 0-9-1 provider code
 */

// NewAMQP091Provider returns a new amqp091 provider
func NewAMQP091Provider() provider.Provider {
	connections := util.NewConcurrentMap()
	prov := &amqp091provider{connections: connections}

	caBundlePath := os.Getenv(trustedCerts)
	prov.tlsConfig = &tls.Config{}

	if caBundlePath != "" {
		caBundle, err := os.ReadFile(filepath.FromSlash(filepath.Clean("/" + strings.Trim(caBundlePath, "/"))))
		if err == nil {
			prov.tlsConfig.RootCAs = x509.NewCertPool()
			prov.tlsConfig.RootCAs.AppendCertsFromPEM(caBundle)
		}
	}

	return prov
}

func (prov *amqp091provider) getBrokerDetails(ctx context.Context) (*BrokerDetails, error) {
	clientIdentifier, err := GetClientIdentifier(ctx)
	if err != nil {
		util.Logger.WarnI(i18n.NoClientUUIDError, err.Error())
		return &BrokerDetails{}, err
	}

	if bd := prov.getBrokerDetailsByIdentifier(clientIdentifier); bd != nil {
		bd.tlsConfig = prov.tlsConfig
		return bd, nil
	}

	return &BrokerDetails{}, fmt.Errorf("could not retrieve broker details for this connection: %s", clientIdentifier)
}

func (prov *amqp091provider) getBrokerDetailsByIdentifier(clientIdentifier string) *BrokerDetails {
	if bd, ok := prov.connections.Get(clientIdentifier); ok {
		return bd.(*BrokerDetails)
	}
	return nil
}

func (prov *amqp091provider) ClientExists(clientIdentifier string) bool {
	if bd := prov.getBrokerDetailsByIdentifier(clientIdentifier); bd != nil {
		return true
	}
	return false
}

// Ack ack a message
func (prov *amqp091provider) Ack(ctx context.Context, msgid string) *pb.Error {
	defer func() *pb.Error {
		if err := recover(); err != nil {
			util.Logger.Debugf("recovered: %v", err)
			return &pb.Error{Message: fmt.Sprintf("%v", err), IsFatal: true}
		}
		return nil
	}()

	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	var span trace.Span
	defer func() {
		if span != nil {
			span.End()
		}
	}()

	if rmu, ok := bd.activeMessages.Get(msgid); ok {
		rm := rmu.(amqp091Message)
		util.Logger.Debugf("Acking message %s with tag %d", msgid, rm.DeliveryTag)
		_, span = tracing.SpanFromHeaders(ctx, fromTableToMap(rm.Headers), msgid+" ack", trace.SpanKindConsumer)
		span.SetAttributes(attribute.String("messaging.message.id", msgid),
			attribute.String("messaging.client_id", bd.ClientIdentifier))
		span.AddEvent("provider acking message")

		err = rm.Ack()
	} else {
		util.Logger.DebugI(i18n.AckNoMessage, bd.ClientIdentifier, msgid)
		return &pb.Error{Message: fmt.Sprintf("No message with uuid %s", msgid)}
	}

	if err != nil {
		util.Logger.WarnI(i18n.AckError, err.Error())

		bd.activeMessages.Delete(msgid)
		errMsg := &pb.Error{
			Message: err.Error(),
		}
		span.RecordError(err)
		return errMsg
	}
	util.Logger.DebugI(i18n.AckMessage, bd.ClientIdentifier, msgid)
	span.AddEvent("provider acked message successfully")
	bd.activeMessages.Delete(msgid)
	return nil
}

// Nack nack a message
func (prov *amqp091provider) Nack(ctx context.Context, msgid string) *pb.Error {
	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	var span trace.Span
	defer func() {
		if span != nil {
			span.End()
		}
	}()

	if rmu, ok := bd.activeMessages.Get(msgid); ok {

		rm := rmu.(amqp091Message)
		_, span = tracing.SpanFromHeaders(ctx, fromTableToMap(rm.Headers), msgid+" nack", trace.SpanKindConsumer)
		span.SetAttributes(attribute.String("messaging.message.id", msgid),
			attribute.String("messaging.client_id", bd.ClientIdentifier))
		span.AddEvent("provider nacking message")

		err = rm.Nack(false)
	} else {
		util.Logger.DebugI(i18n.NackNoMessage, bd.ClientIdentifier, msgid)
		return &pb.Error{Message: fmt.Sprintf("No message with uuid %s", msgid)}
	}

	if err != nil {
		util.Logger.WarnI(i18n.NackError, err.Error())

		bd.activeMessages.Delete(msgid)
		errMsg := &pb.Error{
			Message: err.Error(),
		}
		span.RecordError(err)
		return errMsg
	}
	util.Logger.DebugI(i18n.NackMessage, bd.ClientIdentifier, msgid)
	span.AddEvent("provider nacked message successfully")
	bd.activeMessages.Delete(msgid)
	return nil
}

func (prov *amqp091provider) Retry(ctx context.Context, origSource *pb.Source, msgid string, delay int32) *pb.Error {
	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	var retrySpan trace.Span
	_, retrySpan = tracing.SpanFromHeaders(ctx, nil, msgid+" retry", trace.SpanKindConsumer)
	defer func() {
		if retrySpan != nil {
			retrySpan.End()
		}
	}()

	origSource.Name = sourceName(origSource)

	retrySpan.SetAttributes(attribute.String("source.name", origSource.GetName()),
		attribute.String("messaging.client_id", bd.ClientIdentifier))
	if rmu, ok := bd.activeMessages.Get(msgid); ok {
		retrySpan.AddEvent("setting up retry")

		rm := rmu.(amqp091Message)

		// setup exchange/queue/binding
		subjects := make([]string, 0)
		subjects = append(subjects, "#")
		options := map[string]string{"MessageTTL": strconv.Itoa(int(delay) * 1000), "DeadLetterAddress": ""}
		sourceName := fmt.Sprintf("%s.retry.%ds", origSource.GetAddress().GetName(), delay)

		retrySource := &pb.Source{
			Name:    sourceName,
			Options: options,
			Address: &pb.Address{
				Subjects: subjects,
				Type:     pb.Address_TOPIC,
				Name:     sourceName,
			},
		}

		if bd.RetryChannel == nil {
			bd.Lock()
			retryChannel, err := bd.Connection.NewChannel()
			if err != nil {
				bd.Unlock()
				return &pb.Error{Message: err.Error()}
			}
			bd.RetryChannel = &retryChannel
			bd.Unlock()
		}
		amqpChannel := *bd.RetryChannel

		defer func(bd *BrokerDetails) *pb.Error {
			if err := recover(); err != nil {
				bd.Lock()
				bd.RetryChannel = nil
				bd.Unlock()
				util.Logger.Debugf("recovered: %v", err)
				return &pb.Error{Message: fmt.Sprintf("%v", err), IsFatal: true}
			}
			return nil
		}(bd)

		declareErr := prov.declareExchange(retrySource.GetAddress(), bd, amqpChannel, false)
		if declareErr != nil {
			util.Logger.Debugf("Failed to declare retry exchange [%s]", retrySource.GetAddress().GetName())
		}

		retrySpan.AddEvent("retry address created")

		declareErr = prov.declareQueue(retrySource, bd, amqpChannel, false)
		if declareErr != nil {
			util.Logger.Debugf("Failed to declare retry queue [%s]", retrySource.GetName())
		}

		retrySpan.AddEvent("retry queue created")

		declareErr = prov.declareBinding(retrySource, bd, amqpChannel, false)
		if declareErr != nil {
			util.Logger.Debugf("Failed to bind retry queue [%s] to exchange [%s]", retrySource.GetName(), retrySource.GetAddress().GetName())
		}

		retrySpan.AddEvent("retry binding created")

		retryErr := amqpChannel.Publish(retrySource.Address.GetName(), origSource.GetName(), rm)

		if retryErr != nil {
			util.Logger.Debugf("Failed to publish retry message [%s], requeueing instead.", msgid)
			_ = rm.Nack(true)
		} else {
			_ = rm.Ack() // We ack the message to prevent it from requeueing or dead lettering
		}
		retrySpan.AddEvent("retry ack/nack complete")
		util.Logger.DebugI(i18n.RetryMessage, bd.ClientIdentifier, msgid, delay)
		bd.activeMessages.Delete(msgid)
	} else {
		util.Logger.DebugI(i18n.RetryNoMessage, bd.ClientIdentifier, msgid)
		return &pb.Error{Message: fmt.Sprintf("No message with uuid %s", msgid)}
	}

	return nil
}

// DeadLetter routes the message to a dead letter Address because all retries have failed
func (prov *amqp091provider) DeadLetter(ctx context.Context, _ *pb.Source, msgid string) *pb.Error {
	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	if rmu, ok := bd.activeMessages.Get(msgid); ok {
		rm := rmu.(amqp091Message)
		util.Logger.Debugf("DeadLetter message with id [%s].", msgid)
		_ = rm.Nack(false) // Requeue set to false will cause the message to DeadLetter
		bd.activeMessages.Delete(msgid)
	} else {
		util.Logger.Debugf("DeadLetter message with id [%s] failed, message not found in active messages.", msgid)
		return &pb.Error{Message: fmt.Sprintf("DeadLetter message with id [%s] failed, message not found in active messages.", msgid)}
	}

	return nil
}

// Connect connect to rabbitmq
func (prov *amqp091provider) Connect(ctx context.Context, cf *pb.ConnectionConfiguration, tlsSkipVerify bool) *pb.Error {
	clientIdentifier, err := GetClientIdentifier(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	activeMessages := util.NewConcurrentMap()

	bd := BrokerDetails{
		connectionConfig: cf,
		ClientIdentifier: clientIdentifier,
		ErrorChannel:     make(chan amqp091Error),
		activeMessages:   activeMessages,
		tlsSkipVerify:    tlsSkipVerify,
		tlsConfig:        prov.tlsConfig,
		produced:         0,
		consumed:         0,
		ActiveStreams:    0,
		clientDisconnect: false,
		lastPubSubEvent:  time.Now(),
		shutdownChan:     make(chan bool, 1),
	}

	_, bdErr := bd.connect()
	if bdErr != nil {
		util.Logger.WarnI(i18n.BrokerConnectError, bdErr.Error())
		return &pb.Error{Message: bdErr.Error()}
	}
	prov.connections.Add(bd.ClientIdentifier, &bd)
	go bd.connectionWatcher()

	return nil

}

func (prov *amqp091provider) setupDeadLetter(ctx context.Context, origSource *pb.Source) *pb.Error {
	opts := origSource.GetOptions()
	if _, ok := opts["DeadLetterAddress"]; !ok {
		return nil
	}

	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	amqpChannel, err := bd.Connection.NewChannel()
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}
	defer amqpChannel.Close()

	// setup exchange/queue/binding
	subjects := make([]string, 0)
	sourceName := fmt.Sprintf("%s.dlq", strings.Replace(origSource.GetName(), ".quorum", "", 1))
	subject := origSource.GetName()
	if dls, ok := opts["DeadLetterSubject"]; ok {
		subject = dls
	}

	subjects = append(subjects, subject)

	source := &pb.Source{
		Name: sourceName,
		Address: &pb.Address{
			Subjects: subjects,
			Type:     pb.Address_TOPIC,
			Name:     opts["DeadLetterAddress"],
		},
	}

	_ = prov.declareExchange(source.GetAddress(), bd, amqpChannel, true)

	_ = prov.declareQueue(source, bd, amqpChannel, true)

	err = prov.declareBinding(source, bd, amqpChannel, true)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	return nil
}

func addressTypeToAmqpType(aType pb.Address_TargetType) (string, error) {

	var exchangeType string
	switch aType {
	case pb.Address_TOPIC:
		exchangeType = "topic"
	case pb.Address_FILTER:
		exchangeType = "headers"
	case pb.Address_QUEUE:
		exchangeType = "direct"
	default:
		util.Logger.WarnI(i18n.AddressTypeError, aType.String())
		return "", fmt.Errorf("%s is not a valid address type", aType)
	}
	return exchangeType, nil
}

func (bd *BrokerDetails) exchangeKnown(name string) bool {

	_, ok := bd.knownExchanges.Get(name)
	return ok
}

func (bd *BrokerDetails) queueKnown(name string) bool {

	_, ok := bd.knownQueues.Get(name)
	return ok
}

func (bd *BrokerDetails) bindingKnown(name string) bool {

	_, ok := bd.knownBindings.Get(name)
	return ok
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

func (prov *amqp091provider) declareExchange(address *pb.Address, bd *BrokerDetails, amqpChannel amqp091ChannelShim, force bool) error {

	// don't try to declare an exchange with amq. in the name
	if strings.Contains(address.GetName(), "amq.") {
		return nil
	}

	known := bd.exchangeKnown(address.GetName())

	if !known || force {

		exchangeType, err := addressTypeToAmqpType(address.GetType())

		if err != nil {
			return err
		}
		util.Logger.InfoI(i18n.ExchangeDeclare, address.GetName())

		err = amqpChannel.ExchangeDeclare(address.GetName(), exchangeType, address.GetDurable(), address.GetAutoDelete())
		if err != nil {
			util.Logger.WarnI(i18n.ExchangeDeclareError, err.Error())
			return err
		}

		bd.knownExchanges.Add(address.GetName(), true)
	}

	if parent := address.GetParentAddress(); parent != nil {

		known = bd.exchangeKnown(parent.GetName())
		if !known || force {
			err := prov.declareExchange(parent, bd, amqpChannel, force)
			if err != nil {
				util.Logger.WarnI(i18n.ExchangeDeclareError, err.Error())
			}

			// Bind each subject from the Address exchange to the ParentAddress exchange
			for _, subject := range address.GetSubjects() {
				util.Logger.InfoI(i18n.ExchangeBind, address.GetName(), parent.GetName(), subject)
				err = amqpChannel.ExchangeBind(address.GetName(), subject, parent.GetName())
				if err != nil {
					return err
				}
			}
			bd.knownExchanges.Add(parent.GetName(), true)
		}
	}
	return nil
}

func sourceName(source *pb.Source) string {
	if isQuorum(source) && !strings.HasSuffix(source.GetName(), ".quorum") {
		return source.GetName() + ".quorum"
	}
	return source.GetName()
}

func isQuorum(source *pb.Source) bool {
	if source.AutoDelete || !source.Durable {
		return false
	}
	return true
}

func (prov *amqp091provider) declareQueue(source *pb.Source, bd *BrokerDetails, amqpChannel amqp091ChannelShim, force bool) error {
	known := bd.queueKnown(source.GetName())
	if known && !force {
		return nil
	}

	args := make(amqp091Table)

	if isQuorum(source) {
		args["x-queue-type"] = "quorum"
	}

	for option, value := range source.GetOptions() {
		switch option {
		case "MessageTTL":
			val, err := strconv.Atoi(value)
			if err != nil {
				return errors.New("value for MessageTTL option must be a quoted integer")
			}
			args["x-message-ttl"] = val
		case "Expires":
			val, err := strconv.Atoi(value)
			if err != nil {
				return errors.New("value for Expires option must be a quoted integer")
			}
			args["x-expires"] = val
		case "DeadLetterAddress":
			args["x-dead-letter-exchange"] = value
		case "DeadLetterSubject":
			args["x-dead-letter-routing-key"] = value
		default:
			return fmt.Errorf("%s is an unsupported source option", option)
		}
	}

	qErr := amqpChannel.QueueDeclare(source.GetName(), source.GetDurable(), source.GetAutoDelete(), source.GetExclusive(), args)
	if qErr != nil {
		util.Logger.WarnI(i18n.QueueDeclareError, qErr.Error())
	}
	bd.knownQueues.Add(source.GetName(), true)

	return nil
}

func (bd *BrokerDetails) getManagementClient() *http.Client {
	client := &http.Client{}

	if bd.tlsEnabled {
		tr := &http.Transport{TLSClientConfig: bd.tlsConfig}
		client = &http.Client{Transport: tr}
	}
	return client
}

func (bd *BrokerDetails) doManagementRequest(method, urn string) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	client := bd.getManagementClient()
	proto := "http"
	if bd.tlsEnabled {
		proto = "https"
	}

	adminPort := bd.connectionConfig.GetAdminPort()
	if adminPort == 0 {
		adminPort = bd.connectionConfig.Port + 10000
	}
	host := bd.connectionConfig.Host

	rurl := fmt.Sprintf("%s://%s:%d%s", proto, host, adminPort, urn)
	req, _ := http.NewRequest(method, rurl, nil)
	req.SetBasicAuth(bd.connectionConfig.GetCredentials().GetUsername(), bd.connectionConfig.GetCredentials().GetPassword())
	req.Header.Add("Accept", "application/json")
	resp, respErr := client.Do(req)

	if respErr != nil { //nolint gocritic
		err := fmt.Errorf("Error retrieving bindings: %s", respErr.Error())
		return results, err
	} else if resp == nil {
		err := fmt.Errorf("Error %s binding %s: no response", method, rurl)
		return results, err
	} else if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		err := fmt.Errorf("Error %s binding %s: request returned a %d", method, rurl, resp.StatusCode)
		return results, err
	}

	defer resp.Body.Close()
	body, bodyErr := io.ReadAll(resp.Body)
	if bodyErr != nil {
		return results, bodyErr
	}

	if resp.StatusCode == 204 {
		return results, nil
	}

	if marshErr := json.Unmarshal(body, &results); marshErr != nil {
		return results, marshErr
	}

	return results, nil
}

func (bd *BrokerDetails) getBindingKeysForSource(source *pb.Source) []map[string]interface{} {
	var results []map[string]interface{}

	exchange := url.QueryEscape(source.GetAddress().GetName())
	queue := url.QueryEscape(source.GetName())
	vhost := bd.connectionConfig.GetTenant()
	if vhost == "" {
		vhost = "/"
	}
	vhost = url.QueryEscape(vhost)

	urn := fmt.Sprintf("/api/bindings/%s/e/%s/q/%s/", vhost, exchange, queue)
	results, err := bd.doManagementRequest("GET", urn)

	if err != nil {
		util.Logger.Debugf("Error listing bindings for %s: %s", queue, err.Error())
	}

	return results
}

func (bd *BrokerDetails) deleteBindingByKeyFromSource(source *pb.Source, propKey string) error {
	exchange := url.QueryEscape(source.GetAddress().GetName())
	queue := url.QueryEscape(source.GetName())
	vhost := bd.connectionConfig.GetTenant()
	if vhost == "" {
		vhost = "/"
	}
	vhost = url.QueryEscape(vhost)

	urn := fmt.Sprintf("/api/bindings/%s/e/%s/q/%s/%s/", vhost, exchange, queue, propKey)
	_, err := bd.doManagementRequest("DELETE", urn)

	if err != nil {
		util.Logger.Debugf("Error deleting binding %s from %s: %s", propKey, queue, err.Error())
		return err
	}

	return nil
}

func (bd *BrokerDetails) cleanupBindings(source *pb.Source, subjects []string) []string {
	bindings := bd.getBindingKeysForSource(source)
	removed := make([]string, 0)
	for _, binding := range bindings {
		bindingExpected := false
		routingKey := binding["routing_key"].(string)
		for _, subject := range subjects {
			if routingKey == subject {
				bindingExpected = true
				break
			}
		}
		if !bindingExpected {
			util.Logger.Debugf("Deleting binding %s for routing key %s from %s\n", binding["properties_key"], binding["routing_key"], source.GetName())
			err := bd.deleteBindingByKeyFromSource(source, binding["properties_key"].(string))
			if err != nil {
				util.Logger.Debugf("Error deleting binding: %s", err.Error())
			} else {
				removed = append(removed, binding["properties_key"].(string))
			}
		}
	}
	return removed
}

func (prov *amqp091provider) declareBinding(source *pb.Source, bd *BrokerDetails, amqpChannel amqp091ChannelShim, force bool) error {
	knownBindingKey := fmt.Sprintf("%s:%s", source.GetName(), strings.Join(source.Address.GetSubjects(), ":"))
	known := bd.bindingKnown(knownBindingKey)
	if known && !force {
		return nil
	}

	// If the address has subjects, bind to each subject.
	// But if the address has no subjects, bind without a subject. Don't do both.
	util.Logger.InfoI(i18n.Binding, source.GetName(), strings.Join(source.GetAddress().GetSubjects(), ","), source.GetAddress().GetName())

	matchHeadersList := make([]amqp091Table, 0)

	if source.GetAddress().GetType() == pb.Address_FILTER {
		for _, filter := range source.GetFilters() {
			matchHeaders := make(amqp091Table)
			matches := filter.GetMatches()
			for _, match := range matches {
				util.Logger.Debugf("match: %v", match)
				matchHeaders[match.GetName()] = match.GetValue()
			}

			if len(matchHeaders) > 0 {
				matchHeaders["x-match"] = "all"
				if filter.GetType() == pb.Filter_ANY {
					matchHeaders["x-match"] = "any"
				}
			}

			if len(matchHeaders) > 0 {
				util.Logger.Debugf("Arguments (matches): %s", matchHeaders)
			}

			matchHeadersList = append(matchHeadersList, matchHeaders)
		}
	}

	subjects := source.GetAddress().GetSubjects()
	if len(subjects) == 0 && len(matchHeadersList) > 0 {
		// If subjects aren't included in the address, fake an empty one so
		// we ensure we bind unless we have no Filters
		subjects = append(subjects, "")
	}

	for _, subject := range subjects {
		if len(matchHeadersList) > 0 {
			for _, matchHeaders := range matchHeadersList {
				bErr := amqpChannel.QueueBind(source.GetName(), subject, source.GetAddress().GetName(), matchHeaders)
				if bErr != nil {
					util.Logger.WarnI(i18n.QueueBindError, bErr.Error())
				}
			}
		} else {
			bErr := amqpChannel.QueueBind(source.GetName(), subject, source.GetAddress().GetName(), nil)
			if bErr != nil {
				util.Logger.WarnI(i18n.QueueBindError, bErr.Error())
			}
		}
	}

	removed := bd.cleanupBindings(source, subjects)
	util.Logger.Debugf("removed %d bindings from %s", len(removed), source.GetName())

	bd.knownBindings.Add(knownBindingKey, true)
	return nil
}

// Subscribe subscribe to a stream of messages from the broker
func (prov *amqp091provider) Subscribe(ctx context.Context, source *pb.Source, messageChannel chan<- *pb.Message) *pb.Error {

	if source.GetAddress().GetName() == "" {
		return &pb.Error{Message: "address name not defined"}
	}

	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	bd.updateLastPubSubEvent()

	if bd.Connection.IsClosed() {
		return &pb.Error{Message: "connection to broker is closed"}
	}

	source.Name = sourceName(source)

	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var subSpan trace.Span
	_, subSpan = tracing.SpanFromHeaders(newCtx, nil, source.GetAddress().GetName()+" subscribe setup", trace.SpanKindConsumer)
	subSpan.SetAttributes(attribute.String("source.name", source.GetName()),
		attribute.String("messaging.client_id", bd.ClientIdentifier))

	amqpChannel, err := bd.Connection.NewChannel()
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}
	defer amqpChannel.Close()

	err = prov.declareExchange(source.GetAddress(), bd, amqpChannel, true)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	subSpan.AddEvent("address created")

	err = prov.declareQueue(source, bd, amqpChannel, true)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	subSpan.AddEvent("queue created")

	err = prov.declareBinding(source, bd, amqpChannel, true)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	subSpan.AddEvent("binding created")

	prov.setupDeadLetter(ctx, source)

	subSpan.AddEvent("dead letter address created")

	if source.GetPrefetchCount() > 0 {
		err := amqpChannel.SetPrefetch(int(source.GetPrefetchCount()))
		// if SetPrefetch fails, we need to get out because this could
		// setup a firehose for a client who isn't expecting it
		if err != nil {
			return &pb.Error{Message: err.Error()}
		}
	}

	subSpan.AddEvent("starting consume")
	messages, err := amqpChannel.Consume(
		source.GetName(),      // queue name
		false,                 // auto-ack
		source.GetExclusive(), // exclusive
	)

	if err != nil {
		util.Logger.WarnI(i18n.ClientSubscribeError, bd.ClientIdentifier, source.GetName(), err.Error())
		return &pb.Error{Message: err.Error()}
	}

	util.Logger.InfoI(i18n.ClientSubscribe, bd.ClientIdentifier, source.GetName())

	connErrChan := make(chan amqp091Error)
	connErrChan = bd.Connection.NotifyClose(connErrChan)
	cancelChan := make(chan amqp091Error)
	cancelChan = amqpChannel.NotifyClose(cancelChan)

	defer func() {
		// try to send on the channel and if we can't it's
		// probably not receiving on the other end for some
		// reason
		select {
		case connErrChan <- newAmqp091Error("Subscribe done", 2001):
			return
		default:
			return

		}
	}()

	bd.incrementStreamCount()
	defer bd.decrementStreamCount()

	defer func() *pb.Error {
		if err := recover(); err != nil {
			util.Logger.Debugf("recovered: %v", err)
			return &pb.Error{Message: fmt.Sprintf("%v", err), IsFatal: true}
		}
		return nil
	}()

	subSpan.AddEvent("ending subscribe setup")
	subSpan.End()

	for {
		select {
		case <-ctx.Done():
			return nil
		case cancelErr, ok := <-cancelChan:
			if !ok {
				util.Logger.Debugf("Channel to broker closed during subscribe %v", bd.ClientIdentifier)
				return &pb.Error{Message: "Channel to broker closed", IsFatal: true}
			}

			if cancelErr != (amqp091Error{}) {
				util.Logger.Debugf("Received channel notify for client during subscribe %v : %v", bd.ClientIdentifier, cancelErr)
				return &pb.Error{Message: cancelErr.Error()}
			} else if bd.state != provider.CONNECTED {
				// The connection was closed without an error on the channel, so this was expected.
				// TODO: Should we check for DISCONNECTED/CONNECTING as well?
				util.Logger.Debugf("Received channel state not connected during subscribe %v : %v", bd.ClientIdentifier, bd.state)
				return nil
			}
		case chanErr, ok := <-connErrChan:
			if !ok {
				util.Logger.Debugf("Connection to broke closed during subscribe %v", bd.ClientIdentifier)
				return &pb.Error{Message: "Connection to broker closed"}
			}

			if chanErr != (amqp091Error{}) {
				util.Logger.Debugf("Received connection notify for client during subscribe %v : %v", bd.ClientIdentifier, chanErr)
				return &pb.Error{Message: chanErr.Error()}
			} else if bd.state != provider.CONNECTED {
				// The connection was closed without an error on the channel, so this was expected.
				// TODO: Should we check for DISCONNECTED/CONNECTING as well?
				util.Logger.Debugf("Received connection state not connected during subscribe %v : %v", bd.ClientIdentifier, bd.state)
				return nil
			}
		case msg, ok := <-messages:
			if !ok {
				// Message channel closed
				return nil
			}
			// Sometimes we get a message with a DeliveryTag == 0, which is bad and I'm not sure
			// how this actually happens
			if msg.DeliveryTag == 0 {
				continue
			}

			messageUUID := util.GenUUID()
			headers := make(map[string]string)
			for header, value := range msg.Headers {
				// make everything a string
				headers[header] = fmt.Sprintf("%v", value)
			}
			if msg.ContentType != "" {
				headers["Content-Type"] = msg.ContentType
			}
			if msg.ContentEncoding != "" {
				headers["Content-Encoding"] = msg.ContentEncoding
			}
			message := &pb.Message{Uuid: messageUUID, Body: msg.Body, Headers: headers, Address: source.GetAddress()}

			_, span := tracing.SpanFromHeaders(ctx, message.GetHeaders(), source.GetAddress().GetName()+" subscribe", trace.SpanKindConsumer)

			if tracing.Enabled() {
				span.SetAttributes(attribute.String("source.name", source.GetName()),
					attribute.String("messaging.client_id", bd.ClientIdentifier))

				message.Headers[tracing.HeaderTraceState] = span.SpanContext().TraceState().String()
				message.Headers[tracing.HeaderTraceParent] = fmt.Sprintf("00-%s-%s-%s",
					span.SpanContext().TraceID().String(),
					span.SpanContext().SpanID().String(),
					span.SpanContext().TraceFlags(),
				)
			}

			span.AddEvent("sending message from provider to server for consume")

			bd.activeMessages.Add(messageUUID, msg)
			messageChannel <- message
			bd.consumed++
			span.End()
		}
	}
}

// Disconnect disconnect from the broker
func (prov *amqp091provider) Disconnect(ctx context.Context) {
	clientIdentifier, err := GetClientIdentifier(ctx)
	if err != nil {
		return
	}

	prov.disconnectClientByIdentifier(clientIdentifier)
}

func (prov *amqp091provider) disconnectClientByIdentifier(clientIdentifier string) {

	var bd *BrokerDetails
	if bdu, ok := prov.connections.Get(clientIdentifier); ok {
		bd = bdu.(*BrokerDetails)
	} else {
		return
	}

	defer func() {
		if err := recover(); err != nil {
			return
		}
	}()

	bd.clientDisconnect = true
	util.Logger.InfoI(i18n.ClientDisconnect, bd.ClientIdentifier)
	bd.shutdownChan <- true // shut down the connectionWatcher
	// close the client if it is still connected
	if bd.Connection != nil && !bd.Connection.IsClosed() {
		bd.Connection.Close()
	}
	prov.connections.Delete(clientIdentifier)

	bd = nil
}

// Publish publish a message to the broker
func (prov *amqp091provider) Publish(ctx context.Context, messageChannel <-chan *pb.Message, errChan chan<- *pb.Error) *pb.Error {

	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}

	bd.updateLastPubSubEvent()
	bd.incrementStreamCount()
	defer bd.decrementStreamCount()

	if bd.Connection.IsClosed() {
		return &pb.Error{Message: "connection to broker is closed"}
	}

	amqpChannel, err := bd.Connection.NewChannel()
	if err != nil {
		return &pb.Error{Message: err.Error()}
	}
	defer amqpChannel.Close()

	connErrChan := make(chan amqp091Error)
	connErrChan = bd.Connection.NotifyClose(connErrChan)
	cancelChan := make(chan amqp091Error)
	cancelChan = bd.Connection.NotifyClose(cancelChan)

	defer func() {
		// try to send on the channel and if we can't it's
		// probably not receiving on the other end for some
		// reason
		select {
		case connErrChan <- newAmqp091Error("Publish done", 2002):
			return
		default:
			return

		}
	}()

	for {
		select {
		case cancelErr, ok := <-cancelChan:
			if !ok {
				util.Logger.Debugf("Channel to broker closed during publish %v", bd.ClientIdentifier)
				return &pb.Error{Message: "Channel to broker closed"}
			}

			if cancelErr != (amqp091Error{}) {
				util.Logger.Debugf("Received channel notify for client during publish %v : %v", bd.ClientIdentifier, cancelErr)
				return &pb.Error{Message: cancelErr.Error()}
			} else if bd.state != provider.CONNECTED {
				// The connection was closed without an error on the channel, so this was expected.
				// TODO: Should we check for DISCONNECTED/CONNECTING as well?
				util.Logger.Debugf("Received channel state not connected during publish %v : %v", bd.ClientIdentifier, bd.state)
				return nil
			}
		case chanErr, ok := <-connErrChan:
			if !ok {
				util.Logger.Debugf("Connection to broker closed during publish %v", bd.ClientIdentifier)
				return &pb.Error{Message: "Connection to broker closed"}
			}

			if chanErr != (amqp091Error{}) {
				util.Logger.Debugf("Received connection notify for client during publish %v : %v", bd.ClientIdentifier, chanErr)
				retError := &pb.Error{Message: chanErr.Error()}
				return retError
			} else if bd.state != provider.CONNECTED {
				// The connection was closed without an error on the channel, so this was expected.
				// TODO: Should we check for DISCONNECTED/CONNECTING as well?
				util.Logger.Debugf("Received connection state not connected during publish %v : %v", bd.ClientIdentifier, bd.state)
				return nil
			}
		case message := <-messageChannel:
			if message == nil {
				// nil message means shut it down
				return nil
			}
			mCtx := context.Background()

			_, span := tracing.SpanFromHeaders(mCtx, message.GetHeaders(), message.GetAddress().GetName()+" publish", trace.SpanKindProducer)

			span.SetAttributes(attribute.String("subject.name", message.GetAddress().GetSubjects()[0]),
				attribute.String("messaging.client_id", bd.ClientIdentifier))

			address := message.GetAddress()
			deliveryMode := 1
			if message.GetPersistent() {
				deliveryMode = 2
			}

			err = prov.declareExchange(message.GetAddress(), bd, amqpChannel, false)
			if err != nil {
				errChan <- &pb.Error{
					Message: err.Error(),
					IsFatal: true,
				}
				span.End()
				continue
			}
			span.AddEvent("address created")

			amqpMessage := amqp091Message{}
			amqpMessage.Body = message.GetBody()
			amqpMessage.DeliveryMode = deliveryMode

			headers := amqp091Table{}

			for headerName, headerValue := range message.GetHeaders() {
				headers[headerName] = headerValue
				switch headerName {
				case "Content-Type":
					amqpMessage.ContentType = headerValue
				case "Content-Encoding":
					amqpMessage.ContentEncoding = headerValue
				}
			}

			amqpMessage.Headers = headers

			err = amqpChannel.Publish(
				address.GetName(),        // exchange
				address.GetSubjects()[0], // routing key
				amqpMessage)

			span.AddEvent("message published to broker")

			if err != nil {
				util.Logger.WarnI(i18n.PublishError, err.Error())

				errMsg := &pb.Error{
					Message: err.Error(),
					IsFatal: true,
				}
				errChan <- errMsg
			} else {
				util.Logger.DebugI(i18n.ClientPublished, bd.ClientIdentifier)
				bd.produced++
			}
			errChan <- nil
			span.End()
		}
	}

}

// SupportSourceOptions returns a map[string]bool of support options for Source.Options
func (prov *amqp091provider) SupportedSourceOptions() map[string]bool {
	return supportedSourceOptions
}

// WaitForConnect returns true if connected, false if connection fails
func (prov *amqp091provider) WaitForConnect(ctx context.Context) bool {
	bd, err := prov.getBrokerDetails(ctx)
	if err != nil {
		return false
	}

	// to prevent unwanted disconnects for a client with a single stream
	// we need to increment the stream count if we are waiting for provider connect
	bd.incrementStreamCount()
	defer bd.decrementStreamCount()

	for start := time.Now(); time.Since(start) < provider.CONNECTTIMEOUT*time.Second; {
		if bd.state == provider.CONNECTED {
			util.Logger.InfoI(i18n.ClientConnected, bd.ClientIdentifier)
			return true
		}
		bd, err = prov.getBrokerDetails(ctx)
		if err != nil {
			util.Logger.InfoI(i18n.ClientDetailsGone, bd.ClientIdentifier)
			return false
		}

		sleepRandomReconnect()

	}
	return false
}

func sleepRandomReconnect() {
	util.SleepRandom(500, provider.ReconnectDelay)
}

// connectionWatcher Called at the end of BrokerDetails.connect(), we monitor the bd.ErrorChannel and try to reconnect
// if we get an error on the channel. Receiving nil on the channel means we've closed because of the client
func (bd *BrokerDetails) connectionWatcher() {

	for !bd.clientDisconnect {
		select {
		case <-bd.shutdownChan:
			return
		case err, ok := <-bd.ErrorChannel:
			bd.Lock()
			if !ok || (err != (amqp091Error{}) && err.Code() != 0) {
				bd.state = provider.DISCONNECTED
				sleepRandomReconnect()
				bd.Unlock()
				// Ignore this error because we will reconnect in 30 seconds
				bd.connect() //nolint errcheck
				continue
			}
			bd.Unlock()
		case <-time.After(30 * time.Second):
			// if we never get an error on the bd.ErrorChannel, try again after 30 seconds
			// this is to help deal with race condition where we're not listening on the bd.ErrorChannel
			// when there is an error on the connection
			if bd.Connection.IsClosed() {
				bd.Lock()
				bd.state = provider.DISCONNECTED
				bd.Unlock()
				// Ignore this error because we will reconnect in 30 seconds
				bd.connect() //nolint errcheck
			}
			continue
		}
	}
}

func getCaBundlePath() string {
	caBundlePath := os.Getenv("CA_BUNDLE")
	return caBundlePath
}

func (bd *BrokerDetails) connect() (bool, error) {

	if bd.clientDisconnect {
		return false, nil
	}

	if bd.state == provider.CONNECTING {
		for start := time.Now(); time.Since(start) < 30*time.Second; {
			breakLoop := false
			switch bd.state {
			case provider.CONNECTED:
				return true, nil
			case provider.CONNECTING:
				time.Sleep(100 * time.Millisecond)
				continue
			case provider.CLOSED:
				return false, nil
			case provider.DISCONNECTED:
				breakLoop = true
			}

			if breakLoop {
				break
			}
		}
	}

	bd.Lock()
	defer bd.Unlock()
	if bd.state == provider.CONNECTED {
		return true, nil
	}

	bd.state = provider.CONNECTING
	var conn amqp091ConnectionShim
	var err error

	// Reinitialize these maps early, we especially want to
	// ensure activeMessages gets cleared out before an Ack/Nacks
	// are sent from the client.
	bd.knownExchanges = util.NewConcurrentMap()
	bd.knownQueues = util.NewConcurrentMap()
	bd.knownBindings = util.NewConcurrentMap()
	bd.activeMessages = util.NewConcurrentMap()

	cf := bd.connectionConfig

	var tenant = cf.GetTenant()
	if tenant == "" {
		tenant = "/"
	}

	util.Logger.InfoI(i18n.ClientConnect, bd.ClientIdentifier, cf.GetHost())

	scheme := "amqp"

	// Use TLS in these scenarios:
	// * ConnectionConfiguration.TLS = true
	// * ConnectionConfiguration.CaCertificate is not empty
	if cf.GetTls() || len(cf.GetCaCertificate()) > 0 {
		bd.tlsEnabled = true
		scheme = "amqps"
	}

	var connStr string
	var tlsConfig = bd.tlsConfig

	// force TLS and also skip verification if true
	if bd.tlsEnabled && bd.tlsSkipVerify { //nolint gocritic
		util.Logger.Debugf("%s connecting with TLS enabled but verification off: %s:%d", bd.ClientIdentifier, cf.GetHost(), cf.GetPort())
		tlsConfig.InsecureSkipVerify = true
	} else if bd.tlsEnabled { // Regular TLS with cert verification against system certs
		if tlsConfig.RootCAs != nil {
			util.Logger.Debugf("%s connecting with TLS using system certs: %s:%d", bd.ClientIdentifier, cf.GetHost(), cf.GetPort())
		} else if caBundlePath := getCaBundlePath(); caBundlePath != "" {
			util.Logger.Debugf("%s connecting with TLS using certs from %s: %s:%d", bd.ClientIdentifier, caBundlePath, cf.GetHost(), cf.GetPort())
			caBundle, err := os.ReadFile(filepath.FromSlash(filepath.Clean("/" + strings.Trim(caBundlePath, "/"))))
			if err != nil {
				return false, fmt.Errorf("could not read CA_BUNDLE %s: %s", caBundlePath, err.Error())
			}
			tlsConfig.RootCAs = x509.NewCertPool()
			tlsConfig.RootCAs.AppendCertsFromPEM(caBundle)
		}
	} else { // no tls
		util.Logger.Debugf("%s connecting without TLS: %s:%d", bd.ClientIdentifier, cf.GetHost(), cf.GetPort())
	}

	connStr = fmt.Sprintf("%s://%s:%s@%s:%d/%s", scheme, cf.GetCredentials().GetUsername(),
		cf.GetCredentials().GetPassword(), cf.GetHost(), cf.GetPort(), tenant)

	conn = NewAmqpConn091(connStr, bd.ClientIdentifier, tlsConfig)
	err = conn.Connect()

	if err != nil {
		util.Logger.WarnI(i18n.BrokerConnectError, err.Error())
		bd.state = provider.CLOSED
		return false, err
	}

	bd.Connection = conn
	bd.ErrorChannel = make(chan amqp091Error)
	bd.ErrorChannel = bd.Connection.NotifyClose(bd.ErrorChannel) // this looks unneeded but it aids in unit testing
	bd.state = provider.CONNECTED

	util.Logger.InfoI(i18n.ClientConnected, bd.ClientIdentifier)

	return true, nil

}

func (prov *amqp091provider) Stats() *provider.Stats {

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
