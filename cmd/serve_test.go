package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "sassoftware.io/viya/arke/api"
)

func testHealth(port int) error {
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials())) //nolint staticcheck
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewHealthzClient(conn)
	ctx := context.Background()
	_, err = c.Check(ctx)
	return err
}

func TestMonitorProcessStats(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(500 * time.Millisecond)
		err := testHealth(50058)
		assert.Nil(t, err)
		cancel()
	}()
	os.Setenv("PORT", "50058")
	defer os.Unsetenv("PORT")
	err := run(ctx)
	assert.Nil(t, err)
}
