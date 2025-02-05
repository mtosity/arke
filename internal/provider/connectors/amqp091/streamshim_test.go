package amqp091

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_getMaxProducers(t *testing.T) {
	sc := streamConnection{}
	assert.Equal(t, 1, sc.getMaxProducers())

	sc.maxProducers = 0
	assert.Equal(t, 1, sc.getMaxProducers())

	sc.maxProducers = -1
	assert.Equal(t, 1, sc.getMaxProducers())

	sc.maxProducers = 2
	assert.Equal(t, 2, sc.getMaxProducers())
}

func Test_getMaxConsumers(t *testing.T) {
	sc := streamConnection{}
	assert.Equal(t, 1, sc.getMaxConsumers())

	sc.maxConsumers = 0
	assert.Equal(t, 1, sc.getMaxConsumers())

	sc.maxConsumers = -1
	assert.Equal(t, 1, sc.getMaxConsumers())

	sc.maxConsumers = 2
	assert.Equal(t, 2, sc.getMaxConsumers())
}
