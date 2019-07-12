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

	cItem := cMap.Get("testItem")
	cItem2 := cMap.Get("testItem2")
	assert.Equal(t, cItem, &testItem)
	assert.Equal(t, cItem2, testItem2)
}

func TestConcurrentMapDelete(t *testing.T) {
	cMap := NewConcurrentMap()
	assert.NotNil(t, cMap)
	testItem := TestItem{"test item"}
	cMap.Add("testItem", testItem)
	cItem := cMap.Get("testItem")
	assert.Equal(t, cItem, testItem)
	cMap.Delete("testItem")
	cItem = cMap.Get("tetstItem")
	assert.Nil(t, cItem)
}
