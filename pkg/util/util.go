package util

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var clientMap = NewConcurrentMap()
var cpuHistory = make([]float64, 24) // 2 minutes worth of cpu usage
var memHistory = make([]int, 24)
var maxMemory = float64(0) //nolint

var (
	maxMemReader = func() (io.Reader, error) {
		return os.Open("/sys/fs/cgroup/memory.max")
	}
	limitMemReader = func() (io.Reader, error) {
		return os.Open("/sys/fs/cgroup/memory/memory.limit_in_bytes")
	}
)

func SetClientIdentifier(ctx context.Context, name string) (string, error) {
	clientAddr, err := GetClientAddr(ctx)
	if err != nil {
		return "", err
	}
	// deepcode ignore InsecureHash: no sensitive data-only hashing for a unique client identifier
	h := fmt.Sprintf("%x", sha1.Sum([]byte(clientAddr)))[:8]
	clientIdentifier := fmt.Sprintf("%s-%s", name, h)
	clientMap.Add(clientAddr, clientIdentifier)
	return clientIdentifier, err
}

func RemoveClientIdentifier(ctx context.Context) {
	clientAddr, _ := GetClientAddr(ctx)
	clientMap.Delete(clientAddr)
}

// GetClientIdentifier retrieves or generates the client identifier
func GetClientIdentifier(ctx context.Context) (string, error) {
	clientAddr, err := GetClientAddr(ctx)

	if err != nil {
		return "", errors.New("Could not retrieve client-id from context")
	}

	if identifier, found := clientMap.Get(clientAddr); found {
		return identifier.(string), nil
	}

	return "", errors.New("Could not find client identifier")
}

// GetClientAddr gets the client-id from the context metadata
func GetClientAddr(ctx context.Context) (string, error) {
	if client, ok := peer.FromContext(ctx); ok {
		if client.Addr.String() == "" {
			return "", errors.New("Could not retrieve address info from peer")
		}
		return client.Addr.String(), nil
	}
	return "", errors.New("Could not retrieve peer info")
}

// GenUUID Generate a UUID and return the string representation
func GenUUID() string {
	uuidRaw := uuid.New()
	return uuidRaw.String()
}

type ProcessStats struct {
	MaxMemory       int
	MemoryAverage   float64
	CurrentMemory   int
	CurrentCPUUsage float64
	CPUUsageAverage float64
}

func GetProcessStats() *ProcessStats {
	ps := &ProcessStats{}

	var cpuUsageTotal float64
	for _, usage := range cpuHistory {
		cpuUsageTotal += usage
	}
	cpuUsageAvg := cpuUsageTotal / float64(len(cpuHistory))
	ps.CPUUsageAverage = cpuUsageAvg

	var memTotal int
	for _, mem := range memHistory {
		memTotal += mem
	}
	ps.MemoryAverage = float64(memTotal) / float64(len(memHistory))
	ps.CurrentMemory = memHistory[len(memHistory)-1]
	ps.CurrentCPUUsage = cpuHistory[len(cpuHistory)-1]

	return ps
}

type cpuProcStat struct {
	hz         int64
	utime      int64
	stime      int64
	gatherTime time.Time
}

func NewTimestampPB() *timestamppb.Timestamp {
	return timestamppb.New(time.Now())
}

func SleepRandom(min int, max int) {
	rn := rand.New(rand.NewSource(time.Now().UnixNano()))

	splay := time.Duration(rn.Intn(max-min)+min) * time.Millisecond
	time.Sleep(splay)
}

func GetConfig(key string, def interface{}) interface{} {
	val := os.Getenv(key)
	// Can't find the env var, return default value
	if val == "" {
		return def
	}
	// We assume the environment variable is the same type
	// as the default value passed in.
	switch def.(type) {
	case bool:
		boolVal, err := strconv.ParseBool(val)
		if err == nil {
			return boolVal
		}
	case int:
		intVal, err := strconv.Atoi(val)
		if err == nil {
			return intVal
		}
	case string:
		return val
	}
	// If we don't have a case for the type or if we have
	// an error parsing, then return the default value
	return def
}

func GetMemoryLimit() int64 {
	var memFile []byte
	var memLimit int64

	memReader, err := maxMemReader()
	if err == nil {
		memFile, err = io.ReadAll(memReader)
	}
	if err != nil {
		var limitReader io.Reader
		limitReader, err = limitMemReader()
		if err == nil {
			memFile, err = io.ReadAll(limitReader)
		}
	}
	if err == nil {
		memBytes, pErr := strconv.ParseFloat(string(memFile), 64)
		if pErr == nil {
			memLimit = int64(memBytes * 0.9)
		}
	}
	return memLimit
}
