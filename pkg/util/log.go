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
error.failedserve=Error starting server: {0}
error.ack=Error acking message: {0}
error.nack=Error nacking message: {0}
error.brokerconnect=Error connecting to the broker: {0}
error.exchangedeclare=Error creating exchange: {0}
error.queuedeclare=Error from queue create: {0}
error.queuebind=Error from bind: {0}
error.noclientuuid=Could not retrieve client UUID: {0}
error.clientsubscribe=Error subscribing client {0} queue {1}: {2}
error.publish=Failed to publish a message: {0}
error.subscribe=Error in subscribe: {0}
error.streamsend=Error sending on stream for {1}: {0}
error.clientnoprovider=Could not find connection information for {0}
error.metricsserve=Could not serve metrics handler: {0}
error.port=Error parsing PORT environment variable: {0}
error.addresstype={0} is not a valid address type
info.starting=Serving on port {0}
info.clientconnected={0} is connected
info.exchangedeclare=Declaring exchange {0}
info.binding=Binding to Queue/Key/Exchange: {0}/{1}/{2}
info.clientsubscribe=Client {0} subscribed to {1}
info.clientdisconnect=Client {0} disconnected
info.clientdetailsgone=Broker details for {0} no longer exist. Client initiated disconnect.
info.clientconnect=Client {0} connecting to broker {1}
info.clientconnected=Client {0} connected to broker
info.unsupportedsourceoption=Unsupported source option: {0}
debug.clientpublished=Client {0} published a message
debug.acknomessage=Client {0} attempted to ack unknown message {1}
debug.ackmessage=Client {0} acked message {1}
debug.nacknomessage=Client {0} attempted to ack unknown message {1}
debug.nackmessage=Client {0} acked message {1}
debug.retrynomessage=Client {0} attempted to retry unknown message {1}
debug.retrymessage=Client {0} retried message {1} with delay {2}
info.subscribefailbutclientexists=Client {0} failed subscribe with: {1}
info.exchangebind=Binding exchange {0} to {1} with key {2}
error.streamsubscribemax=Client {0} has reached {1} max subscribes on a single stream. Stopping Consume RPC.
error.consumerecvchan=Client {0} Consume stream received an error: {1}
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
