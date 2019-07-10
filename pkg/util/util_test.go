package util

import (
	"fmt"
	"testing"

	uuid "github.com/nu7hatch/gouuid"

	"github.com/stretchr/testify/assert"
)

func TestGenUUID(t *testing.T) {
	uuidStr := GenUUID()
	fmt.Println(uuidStr)
	assert.NotNil(t, uuidStr)
	id, err := uuid.ParseHex(uuidStr)
	assert.IsType(t, &uuid.UUID{}, id)
	assert.Nil(t, err)
}
