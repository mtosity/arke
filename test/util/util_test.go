package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnectToArke(t *testing.T) {
	conn, err := ConnectToArke(true)
	assert.Nil(t, err, "error when connecting to arke: %v", err)
	assert.NotNil(t, conn, "got a nil connection")
	if conn != nil {
		conn.Close()
	}
}
