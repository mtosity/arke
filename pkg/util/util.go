package util

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/grpc/peer"
)

var clientMap = NewConcurrentMap()

func SetClientIdentifier(ctx context.Context, name string) (string, error) {
	clientAddr, err := GetClientAddr(ctx)
	if err != nil {
		return "", err
	}
	h := fmt.Sprintf("%x", sha1.Sum([]byte(clientAddr)))[:8]
	clientIdentifier := fmt.Sprintf("%s-%s", name, h)
	clientMap.Add(clientAddr, clientIdentifier)
	return clientIdentifier, err
}

func RemoveClientIdentifier(ctx context.Context) {
	clientAddr, _ := GetClientAddr(ctx)
	clientMap.Delete(clientAddr)
}

// GetClientIdentifier retrieves or generates the client identifier
func GetClientIdentifier(ctx context.Context) (string, error) {
	clientAddr, err := GetClientAddr(ctx)

	if err != nil {
		return "", errors.New("Could not retrieve client-id from context")
	}

	if identifier, found := clientMap.Get(clientAddr); found {
		return identifier.(string), nil
	}

	return "", errors.New("Could not find client identifier")
}

// GetClientAddr gets the client-id from the context metadata
func GetClientAddr(ctx context.Context) (string, error) {
	if client, ok := peer.FromContext(ctx); ok {
		return client.Addr.String(), nil
	}
	return "", errors.New("Could not retrieve peer info")
}

// GenUUID Generate a UUID and return the string representation
func GenUUID() string {
	uuidRaw := uuid.New()
	return uuidRaw.String()
}
