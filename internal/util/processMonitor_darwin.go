//go:build darwin

package util

import (
	"runtime"
	"syscall"
	"time"
)

func MonitorProcessStats() {
	cpuMillicoreLimit = runtime.NumCPU() * 1000

	ticker := time.NewTicker(5 * time.Second)
	lastProcStat := &cpuProcStat{}
	for range ticker.C {
		ps := &cpuProcStat{}
		ps.gatherTime = time.Now()

		// Determine memory usage
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		addMemoryHistory(mem.Sys)

		rusage := &syscall.Rusage{}
		rusageErr := syscall.Getrusage(syscall.RUSAGE_SELF, rusage)
		if rusageErr == nil {
			ps.utime = rusage.Utime.Nano() / int64(time.Millisecond) / 10
			ps.stime = rusage.Stime.Nano() / int64(time.Millisecond) / 10
		}

		currTotalTime := ps.utime + ps.stime
		oldTotalTime := lastProcStat.utime + lastProcStat.stime
		totalTime := currTotalTime - oldTotalTime
		seconds := ps.gatherTime.Sub(lastProcStat.gatherTime).Seconds()
		cpuUsageAsADecimal := (float64(totalTime) / 100.0) / seconds // CPU usage as a decimal (e.g., 0.25 for 25%)
		millicoreUsage := int(cpuUsageAsADecimal * 1000)
		Logger.Tracef("CPU Usage: %0.2f%% (%d mCPU), Memory Sys (bytes): %d", cpuUsageAsADecimal*100, millicoreUsage, mem.Sys)
		addCPUHistory(millicoreUsage)
		lastProcStat = ps
	}
}
