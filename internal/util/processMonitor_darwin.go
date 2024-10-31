//go:build darwin
// +build darwin

package util

import (
	"os/exec"
	"runtime"
	"strconv"
	"syscall"
	"time"
)

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

	ticker := time.NewTicker(5 * time.Second)
	lastProcStat := &cpuProcStat{}
	for {
		ps := &cpuProcStat{}
		ps.hz = hz
		ps.gatherTime = time.Now()

		rusage := &syscall.Rusage{}
		rusageErr := syscall.Getrusage(syscall.RUSAGE_SELF, rusage)
		if rusageErr == nil {
			ps.utime = rusage.Utime.Nano() / int64(time.Millisecond) / 10
			ps.stime = rusage.Stime.Nano() / int64(time.Millisecond) / 10
		}

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
		Logger.Tracef("CPU Usage: %0.2f%%, Memory Heap (bytes): %d", cpuUsage, mem.HeapAlloc)
		lastProcStat = ps

		<-ticker.C
	}
}
