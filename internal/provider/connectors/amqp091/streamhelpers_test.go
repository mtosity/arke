package amqp091

import (
	"testing"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/stretchr/testify/assert"
)

func Test_toStreamOffset(t *testing.T) {
	off, err := toStreamOffset("first", 0)
	assert.Equal(t, stream.OffsetSpecification{}.First(), off)
	assert.Nil(t, err)

	off, err = toStreamOffset("FIRST", 0)
	assert.Equal(t, stream.OffsetSpecification{}.First(), off)
	assert.Nil(t, err)

	off, err = toStreamOffset("continue", 100)
	assert.Equal(t, stream.OffsetSpecification{}.Offset(100), off)
	assert.Nil(t, err)

	off, err = toStreamOffset("0", 100)
	assert.Equal(t, stream.OffsetSpecification{}.Offset(0), off)
	assert.Nil(t, err)

	off, err = toStreamOffset("next", 100)
	assert.Equal(t, stream.OffsetSpecification{}.Next(), off)
	assert.Nil(t, err)

	off, err = toStreamOffset("", 100)
	assert.Equal(t, stream.OffsetSpecification{}.Next(), off)
	assert.Nil(t, err)

	_, err = toStreamOffset("invalid", 100)
	assert.NotNil(t, err)
}
