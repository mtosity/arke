package provider_test

import (
	"bytes"
	"log"
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	. "sassoftware.io/convoy/arke/pkg/provider"
	_ "sassoftware.io/convoy/arke/pkg/provider/connectors"
)

func TestNewProvider(t *testing.T) {
	prov, err := NewProvider("amqp091")
	assert.NotNil(t, prov)
	assert.Nil(t, err)
}

func TestNewProviderFail(t *testing.T) {
	prov, err := NewProvider("fail")
	assert.Nil(t, prov)
	assert.NotNil(t, err)
}

func TestTestProvider(t *testing.T) {
	prov, err := NewProvider("test")
	assert.NotNil(t, prov)
	assert.Nil(t, err)
}

func TestRegisterFail(t *testing.T) {
	regOutput := captureOutput(func() {
		Register("fail", nil)
	})
	assert.Regexp(t, regexp.MustCompile("can not be nil"), regOutput)
}

func TestRegisterTwice(t *testing.T) {
	regOutput := captureOutput(func() {
		Register("test", NewTestProvider)
	})
	assert.Regexp(t, regexp.MustCompile("already registered"), regOutput)
}

func captureOutput(f func()) string {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	f()
	log.SetOutput(os.Stderr)
	return buf.String()
}

/*
 * Test Provider
 */

const providerName string = "test"

type testprovider struct {
	Provider
}

func init() {
	// Register this provider with the Provider factory.
	Register(providerName, NewTestProvider)
}

func NewTestProvider() Provider {
	return &testprovider{}
}
