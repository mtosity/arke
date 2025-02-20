package amqp091

import (
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

const maxProducers = 10
const maxConsumers = 10
const port = 5552

// NewStreamConnection Create a new streamConnection object with a connection string and tls config
func NewStreamConnection(connStr string, clientIdentifier string, streamName string, publisherName string, tlsCfg *tls.Config) streamConnectionShim {
	return &streamConnection{maxProducers: maxProducers, maxConsumers: maxConsumers,
		connStr: connStr, tlsCfg: tlsCfg, clientIdentifier: clientIdentifier,
		streamName: streamName, publisherName: publisherName}
}

func getStreamConnectionString(bd *BrokerDetails) string {
	cf := bd.connectionConfig

	var tenant = cf.GetTenant()
	if tenant == "" {
		tenant = "/"
	}

	scheme := "rabbitmq-stream"

	// Use TLS if enabled
	if bd.tlsEnabled {
		scheme = fmt.Sprintf("%s+tls", scheme)
	}

	return fmt.Sprintf("%s://%s:%s@%s:%d/%s", scheme, cf.GetCredentials().GetUsername(),
		cf.GetCredentials().GetPassword(), cf.GetHost(), port, tenant)
}

func toStreamMessage(origMsg streamMessage) message.StreamMessage {
	msg := amqp.NewMessage(origMsg.Body)
	msg.ApplicationProperties = toStreamHeaders(origMsg.Headers)
	msg.Properties = &amqp.MessageProperties{ContentEncoding: origMsg.ContentEncoding,
		ContentType: origMsg.ContentType}
	if origMsg.PublishID > 0 {
		msg.SetPublishingId(origMsg.PublishID)
	}
	_, _ = msg.MarshalBinary()
	return msg
}

func toStreamHeaders(orig map[string]string) map[string]interface{} {
	intMap := make(map[string]interface{})
	for key, value := range orig {
		intMap[key] = value
	}
	return intMap
}

func fromStreamHeaders(orig map[string]interface{}) map[string]string {
	sMap := make(map[string]string)
	for key, value := range orig {
		sMap[key] = value.(string)
	}
	return sMap
}

func toStreamOffset(offset string, lastOffset int64) (stream.OffsetSpecification, error) {
	switch strings.ToLower(offset) {
	case "first":
		// start consuming from 0
		return stream.OffsetSpecification{}.First(), nil
	case "continue":
		// start where we left off, if no offset stored
		// then start from next()
		return stream.OffsetSpecification{}.Offset(lastOffset), nil
	case "next", "":
		return stream.OffsetSpecification{}.Next(), nil
	}
	pOffset, pErr := strconv.Atoi(offset)
	if pErr == nil {
		return stream.OffsetSpecification{}.Offset(int64(pOffset)), nil
	}
	return stream.OffsetSpecification{}, fmt.Errorf("Invalid offset: %s", offset)
}
