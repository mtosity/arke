package util

import (
	"os"

	"sassoftware.io/viya/zlog"
)

const messages = `
error.generic=Encounted an error: {0}
error.cpuprofile=Error creating CPU profile: {0}
error.memprofile=Error creating memory profile: {0}
error.netlisten=Error listening on port: {0}
error.failedserve=Error serving gRPC: {0}
error.ack=Error acking message: {0}
error.nack=Error nacking message: {0}
error.brokerconnect=Error connecting to the broker: {0}
error.exchangedeclar=Error creating exchange: {0}
error.queuedeclar=Error from queue create: {0}
error.queuebind=Error from bind: {0}
error.noclientuuid=Could not retrieve client UUID: {0}
error.clientsubscribe=Error subscribing client {0} queue {1}: {2}
error.publish=Failed to publish a message: {0}
error.subscribe=Error in subscribe: {0}
error.streamsend=Error sending on stream: {0}
error.clientnoprovider=Could not find connection information for {0}
info.starting=Serving...
info.clientconnected={0} is connected
info.exchangedeclare=Declaring exchange {0}
info.binding=Binding to Queue/Key/Exchange: {0}/{1}/{2}
info.clientsubscribe=Client {0} subscribed to {0}
info.clientdisconnect=Client {0} disconnected
info.clientdetailsgone=Broker details for {0} no longer exist. Client initiated disconnect.
info.clientconnect=Client {0} connecting to broker {1}:{2}
info.clientconnected=Client {0} connected to broker
info.unsupportedsourceoption=Unsupported source option: {0}
debug.clientpublished=Client {0} published a message
debug.acknomessage=Client {0} attempted to ack unknown message {1}
debug.ackmessage=Client {0} acked message {1}
debug.nacknomessage=Client {0} attempted to ack unknown message {1}
debug.nackmessage=Client {0} acked message {1}
`

// Logger default logger
var Logger *zlog.Logger

func init() {
	err := zlog.DefaultBundle.Add([]byte(messages))
	if err != nil {
		panic(err.Error())
	}

	// Set up our default logger

	logFormat := os.Getenv("LOG_FORMAT")
	if logFormat == "" {
		logFormat = zlog.DefaultFormat
	}

	Logger = zlog.New(os.Stderr, logFormat)

	logLevel, err := zlog.ParseLevel(os.Getenv("LOG_LEVEL"))

	if err == nil {
		Logger.MessageBundleLevel = logLevel
	} else {
		logLevel = zlog.DefaultLevel
	}

	Logger.Level = logLevel
}
