package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestItem struct {
	name string
}

func TestNewConcurrentMap(t *testing.T) {
	cMap := NewConcurrentMap()
	assert.NotNil(t, cMap)
}

func TestConcurrentMapAdd(t *testing.T) {
	cMap := NewConcurrentMap()
	testItem := TestItem{"test item"}
	testItem2 := TestItem{"test item 2"}
	assert.NotNil(t, cMap)
	cMap.Add("testItem", &testItem)
	cMap.Add("testItem2", testItem2)

	cItem, ok := cMap.Get("testItem")
	assert.True(t, ok)
	cItem2, ok := cMap.Get("testItem2")
	assert.True(t, ok)
	assert.Equal(t, cItem, &testItem)
	assert.Equal(t, cItem2, testItem2)
}

func TestConcurrentMapDelete(t *testing.T) {
	cMap := NewConcurrentMap()
	assert.NotNil(t, cMap)
	testItem := TestItem{"test item"}
	cMap.Add("testItem", testItem)
	cItem, ok := cMap.Get("testItem")
	assert.True(t, ok)
	assert.Equal(t, cItem, testItem)
	cMap.Delete("testItem")
	cItem, ok = cMap.Get("tetstItem")
	assert.Nil(t, cItem)
	assert.False(t, ok)
}
