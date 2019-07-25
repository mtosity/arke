package util

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"google.golang.org/grpc/peer"
)

// GetClientUUID gets the client-id from the context metadata
func GetClientUUID(ctx context.Context) (string, error) {
	if client, ok := peer.FromContext(ctx); ok {
		return client.Addr.String(), nil
	}
	return "", errors.New("Could not retrieve peer info")
	// md, ok := metadata.FromIncomingContext(ctx)
	// if !ok {
	// 	return "", errors.New("Could not read incoming metadata")
	// }
	// if clientUUID, ok := md["client-uuid"]; ok {
	// 	return clientUUID[0], nil
	// } else {
	// 	return "", errors.New("Could not retrieve client-id from context")
	// }
}

// GenUUID Generate a UUID and return the string representation
func GenUUID() string {
	uuidRaw := uuid.New()
	return uuidRaw.String()
}
