package cache

import (
	"testing"
	"time"

	"github.com/seaguest/common/logger"
	"github.com/stretchr/testify/assert"
)

type TestStruct struct {
	Name string
}

// this will be called by deepcopy to improves reflect copy performance
func (p TestStruct) DeepCopy() interface{} {
	c := p
	return &c
}

func getStruct(id uint32) (*TestStruct, error) {
	key := GetKey("val", id)
	var v TestStruct
	err := GetObject(key, &v, 60, func() (interface{}, error) {
		// data fetch logic to be done here
		time.Sleep(time.Millisecond * 100)
		return &TestStruct{Name: "test"}, nil
	})
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	return &v, nil
}

func TestCache(t *testing.T) {
	Init("test", "127.0.0.1:6379", "", 200)
	v, e := getStruct(100)
	logger.Error(v, e)
	time.Sleep(1 * time.Second)
	assert.True(t, false)
}
