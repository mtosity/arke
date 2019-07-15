package provider

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	pb "sassoftware.io/convoy/arke/api"
	"sassoftware.io/convoy/arke/pkg/util"
)

// Provider provider interface
type Provider interface {
	Publish(*context.Context, *pb.Message) (bool, *pb.Error)
	Subscribe(*context.Context, *pb.Source, chan<- *pb.Message) *pb.Error
	Ack(*context.Context, *pb.Message) *pb.Error
	Nack(*context.Context, *pb.Message) *pb.Error
	Connect(*context.Context, *pb.ConnectionConfiguration) *pb.Error
	Disconnect(*context.Context)
}

// Factory method for creating a specific provider
type Factory func() Provider

// Map of our registered Providers
var registeredProviderTypes = util.NewConcurrentMap()

func NewProvider(providerType string) (Provider, error) {
	pf, ok := registeredProviderTypes.Get(providerType)
	if !ok {
		providerList := registeredProviderTypes.GetList()
		return nil, errors.New(fmt.Sprintf("Invalid provider name. Must be one of: %s", strings.Join(providerList, ",")))
	}

	providerFactory := pf.(Factory)
	return providerFactory(), nil
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
