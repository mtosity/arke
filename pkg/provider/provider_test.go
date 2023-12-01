package provider_test

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	p "sassoftware.io/convoy/arke/pkg/provider"
	_ "sassoftware.io/convoy/arke/pkg/provider/connectors"
	"sassoftware.io/convoy/arke/pkg/util"
	"sassoftware.io/viya/zlog"
)

func TestNewProvider(t *testing.T) {
	prov, err := p.NewProvider("amqp091")
	assert.NotNil(t, prov)
	assert.Nil(t, err)
}

func TestNewProviderFail(t *testing.T) {
	prov, err := p.NewProvider("fail")
	assert.Nil(t, prov)
	assert.NotNil(t, err)
}

func TestTestProvider(t *testing.T) {
	prov, err := p.NewProvider("test")
	assert.NotNil(t, prov)
	assert.Nil(t, err)
}

func TestRegisterFail(t *testing.T) {
	regOutput := captureOutput(func() {
		p.Register("fail", nil)
	})
	assert.Regexp(t, regexp.MustCompile("can not be nil"), regOutput)
}

func TestRegisterTwice(t *testing.T) {
	regOutput := captureOutput(func() {
		p.Register("test", NewTestProvider)
	})
	assert.Regexp(t, regexp.MustCompile("already registered"), regOutput)
}

func TestGetProvider(t *testing.T) {
	// Make sure GetProvider returns a Provider
	prov, err := p.GetProvider("amqp091")
	assert.NotNil(t, prov)
	assert.Nil(t, err)

	// If we call GetProvider twice, we want to make sure
	// We get the same *Provider.
	prov2, err2 := p.GetProvider("amqp091")
	// fmt.Printf("Provider pointer address : %p\n", &prov)
	// fmt.Printf("Provider2 pointer address : %p\n", &prov2)
	assert.NotNil(t, prov2)
	assert.Nil(t, err2)
	assert.Equal(t, &prov, &prov2)
}

func TestGetProvider_Fail(t *testing.T) {
	prov, err := p.GetProvider("unknown")
	assert.Nil(t, prov)
	assert.Regexp(t, regexp.MustCompile("Invalid provider name"), err.Error())
}

func TestConcurrentNewProvider(t *testing.T) {
	// Register a whole bunch of providers, then GetProvider on all of them.
	// This would panic every time because of concurrent writes before the
	// change to util.ConcurrentMap for providerVault
	letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	for _, letter := range strings.Split(letters, "") {
		p.Register(letter, NewTestProvider)
	}
	for _, letter := range strings.Split(letters, "") {
		go p.GetProvider(letter) //nolint errorcheck
	}

	providerNames := []string{"amqp091", "azure", "test"}
	for _, name := range providerNames {
		go p.GetProvider(name) //nolint errcheck
	}
	time.Sleep(100 * time.Millisecond)
	providers := p.RegisteredProviders()
	assert.Equal(t, 55, providers.Length())
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
	p.Provider
}

func init() {
	// Register this provider with the Provider factory.
	p.Register(providerName, NewTestProvider)
}

func NewTestProvider() p.Provider {
	return &testprovider{}
}
