package util

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var clientMap = NewConcurrentMap()
var cpuHistory = make([]float64, 24) // 2 minutes worth of cpu usage
var memHistory = make([]int, 24)
var maxMemory = float64(0) //nolint

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

// ServceNameFromClientAddr returns the service name from the client address.
// The client address typically looks like <service-name>-<random-string>.
// The random string almost always contain numbers, so we can use that to
// to determine the service it came from.
func ServceNameFromClientAddr(clientAddr string) string {
	var serviceName []string
	for _, token := range strings.Split(clientAddr, "-") {
		if strings.ContainsAny(token, "0123456789") {
			break
		}
		serviceName = append(serviceName, token)
	}
	return strings.Join(serviceName, "-")
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

func RecoverPanic() {
	if err := recover(); err != nil {
		Logger.Warn(fmt.Sprintf("%v", err))
		return
	}

}

// GenUUID Generate a UUID and return the string representation
func GenUUID() string {
	uuidRaw := uuid.New()
	return uuidRaw.String()
}

type ProcessStats struct {
	MaxMemory          int
	MemoryAverage      float64
	CurrentMemory      int
	CurrentCPUUsage    float64
	CPUUsageAverage    float64
	CPUAvailability    float32 // CPU availability as a ratio (0.0 to 1.0)
	MemoryAvailability float32 // Memory availability as a ratio (0.0 to 1.0)
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
	ps.MaxMemory = int(maxMemory)

	numCores := runtime.GOMAXPROCS(0)
	maxCPUPercent := float64(numCores) * 100 // 4 cores = 400% max
	actualUsagePercent := (cpuUsageAvg / maxCPUPercent)
	// Calculate CPU availability (1.0 means fully available, 0.0 means fully utilized)
	// Assume 100% per CPU core as maximum, so availability = (100 - usage_percentage) / 100
	ps.CPUAvailability = float32((100.0 - actualUsagePercent) / 100.0)

	if ps.CPUAvailability < 0 {
		ps.CPUAvailability = float32(0)
	}

	// Calculate Memory availability (1.0 means fully available, 0.0 means fully utilized)
	if ps.MaxMemory > 0 {
		memoryUsagePercent := (ps.MemoryAverage / float64(ps.MaxMemory) * 100)
		ps.MemoryAvailability = float32((100.0 - memoryUsagePercent) / 100.0)
		if ps.MemoryAvailability < 0 {
			ps.MemoryAvailability = float32(0)
		}
	} else {
		// If we don't know max memory, assume 100% available
		ps.MemoryAvailability = float32(1.0)
	}

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

func SleepRandom(sleepMin int, sleepMax int) {
	rn := rand.New(rand.NewSource(time.Now().UnixNano()))

	splay := time.Duration(rn.Intn(sleepMax-sleepMin)+sleepMin) * time.Millisecond
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
