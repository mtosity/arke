package main

import (
	"testing"

	"sassoftware.io/convoy/arke"
)

func TestRun(_ *testing.T) {
	arke.DefaultArkeServer().Serve() //nolint errcheck
}
