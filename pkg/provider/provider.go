package provider

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"

	pb "sassoftware.io/convoy/arke/api"
	"sassoftware.io/convoy/arke/pkg/util"
)

// Provider provider interface
type Provider interface {
	PublishOne(*context.Context, *pb.Message) (bool, *pb.Error)
	Publish(*context.Context, <-chan *pb.Message, chan<- *pb.Error) (bool, *pb.Error)
	Subscribe(*context.Context, *pb.Source, chan<- *pb.Message) *pb.Error
	Ack(*context.Context, *pb.Message) *pb.Error
	Nack(*context.Context, *pb.Message) *pb.Error
	Connect(*context.Context, *pb.ConnectionConfiguration) *pb.Error
	Disconnect(*context.Context)
	SupportedSourceOptions() map[string]bool
}

// Factory method for creating a specific provider
type Factory func() Provider

// Map of our registered Provider types
var registeredProviderTypes = util.NewConcurrentMap()

// Map of registered Providers
var registeredProviders = util.NewConcurrentMap()

type providerOnce struct {
	m    sync.Mutex
	done uint32
}

var providerVault = make(map[string]*providerOnce)

func (po *providerOnce) Do(f func() Provider) Provider {
	if atomic.LoadUint32(&po.done) == 1 {
		return nil
	}
	// Slow-path.
	po.m.Lock()
	defer po.m.Unlock()
	if po.done == 0 {
		defer atomic.StoreUint32(&po.done, 1)
		return f()
	}
	return nil
}

func NewProvider(providerType string) (Provider, error) {
	pf, ok := registeredProviderTypes.Get(providerType)
	if !ok {
		providerList := registeredProviderTypes.GetList()
		return nil, errors.New(fmt.Sprintf("Invalid provider name. Must be one of: %s", strings.Join(providerList, ",")))
	}
	var provOnce providerOnce
	providerVault[providerType] = &provOnce
	providerFactory := pf.(Factory)
	// This ensures we only generate one provider of providerType
	provider := provOnce.Do(func() Provider { return providerFactory() })
	return provider, nil
}

func GetProvider(providerType string) (Provider, error) {
	prov, registered := registeredProviders.Get(providerType)
	log.Printf("Looking up provider %s.\n", providerType)
	if !registered {
		log.Printf("Provider %s not found in cache, creating new provider\n", providerType)
		prov, newProvErr := NewProvider(providerType)
		if prov != nil {
			registeredProviders.Add(providerType, prov)
		}
		if newProvErr != nil {
			return nil, newProvErr
		}
	}
	prov, _ = registeredProviders.Get(providerType)

	return prov.(Provider), nil
}

func Register(name string, factory Factory) {
	if factory == nil {
		log.Printf("Provider factory %s can not be nil.", name)
	} else {
		_, registered := registeredProviderTypes.Get(name)
		if registered {
			log.Printf("Provider factory %s already registered. Ignoring.", name)
		} else {
			registeredProviderTypes.Add(name, factory)
			log.Printf("Registering Provider %s.", name)
		}
	}
}
