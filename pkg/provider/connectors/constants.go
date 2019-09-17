package connectors

const (
	// DISCONNECTED Closed by the broker, retry connecting
	DISCONNECTED = iota
	// CONNECTED Connected to the broker
	CONNECTED = iota
	// CONNECTING Currently connecting to the broker
	CONNECTING = iota
	// CLOSED Closed by the client
	CLOSED = iota
)

const (
	// CONNECT_TIMEOUT Default timeout for waiting for connection in WaitForConnect()
	CONNECT_TIMEOUT = 15
)
