package provider

import (
	"context"

	pb "sassoftware.io/convoy/arke/api"
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

// type provider struct {
// 	Provider
// }
