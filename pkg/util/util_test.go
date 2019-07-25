package util

import (
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/stretchr/testify/assert"
)

func TestGenUUID(t *testing.T) {
	uuidStr := GenUUID()
	fmt.Println(uuidStr)
	assert.NotNil(t, uuidStr)
	id, err := uuid.Parse(uuidStr)
	assert.IsType(t, uuid.UUID{}, id)
	assert.Nil(t, err)
}
