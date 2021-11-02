package util

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var clientMap = NewConcurrentMap()
var cpuHistory = make([]float64, 24) // 2 minutes worth of cpu usage
var memHistory = make([]int, 24)
var maxMemory = float64(0)

func SetClientIdentifier(ctx context.Context, name string) (string, error) {
	clientAddr, err := GetClientAddr(ctx)
	if err != nil {
		return "", err
	}
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
	CurrentCpuUsage float64
	CpuUsageAverage float64
}

func GetProcessStats() *ProcessStats {
	ps := &ProcessStats{}

	var cpuUsageTotal float64
	for _, usage := range cpuHistory {
		cpuUsageTotal += usage
	}
	cpuUsageAvg := cpuUsageTotal / float64(len(cpuHistory))
	ps.CpuUsageAverage = cpuUsageAvg

	var memTotal int
	for _, mem := range memHistory {
		memTotal += mem
	}
	ps.MemoryAverage = float64(memTotal) / float64(len(memHistory))
	ps.CurrentMemory = memHistory[len(memHistory)-1]
	ps.CurrentCpuUsage = cpuHistory[len(cpuHistory)-1]

	return ps
}

type cpuProcStat struct {
	hz         int64
	utime      int64
	stime      int64
	gatherTime time.Time
}

func MonitorProcessStats() {
	var hz int64
	hzOut, err := exec.Command("getconfig", "CLK_TCK").Output()
	if err != nil {
		hz = 100
	} else {
		hz, err = strconv.ParseInt(string(hzOut), 10, 64)
		if err != nil {
			hz = 100
		}
	}

	if maxMemory == 0 { // only read memory limit once
		// Determine memory limit
		// if we can't determine memory limit then we must not be in k8s
		memoryLimitFile := "/sys/fs/cgroup/memory/memory.limit_in_bytes"
		contents, err := os.ReadFile(memoryLimitFile)
		if err == nil {
			maxmem, err := strconv.ParseFloat(string(contents), 64)
			if err == nil {
				maxMemory = maxmem
			}
		}
	}

	ticker := time.NewTicker(5 * time.Second)
	lastProcStat := &cpuProcStat{}
	for {
		ps := &cpuProcStat{}
		ps.hz = hz
		ps.gatherTime = time.Now()

		rusage := &syscall.Rusage{}
		syscall.Getrusage(syscall.RUSAGE_SELF, rusage)
		ps.utime = rusage.Utime.Nano() / int64(time.Millisecond) / 10
		ps.stime = rusage.Stime.Nano() / int64(time.Millisecond) / 10

		// Determine memory usage
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)

		memHistory = memHistory[1:]
		memHistory = append(memHistory, int(mem.HeapAlloc))

		currTotalTime := ps.utime + ps.stime
		oldTotalTime := lastProcStat.utime + lastProcStat.stime
		totalTime := currTotalTime - oldTotalTime
		seconds := ps.gatherTime.Sub(lastProcStat.gatherTime).Seconds()
		cpuUsage := 100 * ((float64(totalTime) / float64(ps.hz)) / seconds)
		cpuHistory = cpuHistory[1:]
		cpuHistory = append(cpuHistory, cpuUsage)
		Logger.Debugf("CPU Usage: %0.2f%%, Memory Heap (bytes): %d", cpuUsage, mem.HeapAlloc)
		lastProcStat = ps

		<-ticker.C
	}
}

func NewTimestampPB() *timestamppb.Timestamp {
	return timestamppb.New(time.Now())
}
