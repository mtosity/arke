package tests

import (
	"fmt"
	"testing"

	"sassoftware.io/convoy/arke/pkg/util"

	uuid "github.com/nu7hatch/gouuid"

	"github.com/stretchr/testify/assert"
	// mp "sassoftware.io/convoy/arke/pkg/provider/mock"
)

func TestGenUUID(t *testing.T) {
	uuidStr := util.GenUUID()
	fmt.Println(uuidStr)
	assert.NotNil(t, uuidStr)
	id, err := uuid.ParseHex(uuidStr)
	assert.IsType(t, &uuid.UUID{}, id)
	assert.Nil(t, err)
}
