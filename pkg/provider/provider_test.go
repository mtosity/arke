package provider_test

import (
	"bytes"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	. "sassoftware.io/convoy/arke/pkg/provider"
	_ "sassoftware.io/convoy/arke/pkg/provider/connectors"
	"sassoftware.io/convoy/arke/pkg/util"
	"sassoftware.io/viya/zlog"
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

func TestGetProvider(t *testing.T) {
	// Make sure GetProvider returns a Provider
	prov, err := GetProvider("amqp091")
	assert.NotNil(t, prov)
	assert.Nil(t, err)

	// If we call GetProvider twice, we want to make sure
	// We get the same *Provider.
	prov2, err2 := GetProvider("amqp091")
	// fmt.Printf("Provider pointer address : %p\n", &prov)
	// fmt.Printf("Provider2 pointer address : %p\n", &prov2)
	assert.NotNil(t, prov2)
	assert.Nil(t, err2)
	assert.Equal(t, &prov, &prov2)
}

func TestGetProvider_Fail(t *testing.T) {
	prov, err := GetProvider("unknown")
	assert.Nil(t, prov)
	assert.Regexp(t, regexp.MustCompile("Invalid provider name"), err.Error())
}

func captureOutput(f func()) string {
	var buf bytes.Buffer
	oldLogger := util.Logger
	defer func() { util.Logger = oldLogger }()
	util.Logger = zlog.New(&buf, "term")
	util.Logger.Level = zlog.Debug

	f()
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
