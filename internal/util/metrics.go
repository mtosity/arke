// Copyright © 2026, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"time"
)

const (
	// constants for process stats monitoring
	historySeconds         = 60
	historyIntervalSeconds = 5

	// Define thresholds for unhealthy usage
	cpuAvailabilityThreshold    = 0.1 // 10% CPU availability
	memoryAvailabilityThreshold = 0.1 // 10% Memory availability
)

var cpuMillicoreHistory = NewSafeBuffer(historySeconds / historyIntervalSeconds)
var memHistory = NewSafeBuffer(historySeconds / historyIntervalSeconds)
var maxMemory float64
var cpuMillicoreLimit int

type ProcessStats struct {
	CPUAvailability    float32 // CPU availability as a ratio (0.0 to 1.0)
	MemoryAvailability float32 // Memory availability as a ratio (0.0 to 1.0)
}

func setMaxMemory(mem float64) {
	maxMemory = mem
}

func setCPUMillicoreLimit(millicores int) {
	cpuMillicoreLimit = millicores
}

func addMemoryHistory(mem uint64) { //nolint:unused
	memHistory.Add(mem)
}

func addCPUHistory(cpu int) { //nolint:unused
	cpuMillicoreHistory.Add(cpu)
}

func memoryAvailability() float32 {
	memData := memHistory.GetData()
	if len(memData) == 0 {
		return float32(-1.0) // Unknown availability if we have no data
	}

	var memTotal uint64
	for _, mem := range memData {
		memTotal += mem.(uint64)
	}
	memAvg := float64(memTotal) / float64(len(memData))
	if maxMemory > 0 {
		memoryUsageRate := memAvg / float64(maxMemory)
		memoryAvailability := 1 - memoryUsageRate
		if memoryAvailability < 0 {
			memoryAvailability = 0 // Ensure availability doesn't go negative
		}
		return float32(memoryAvailability)
	}
	// If we don't know max memory, we can't calculate availability, so return -1 to indicate unknown
	return float32(-1.0)
}

func cpuAvailability() float32 {
	cpuData := cpuMillicoreHistory.GetData()
	if len(cpuData) == 0 {
		return float32(-1.0) // Unknown availability if we have no data
	}

	if cpuMillicoreLimit <= 0 {
		return float32(-1.0) // Unknown availability if we don't know the limit
	}

	var cpuMillicoreTotal int
	for _, usage := range cpuData {
		cpuMillicoreTotal += usage.(int)
	}
	cpuMillicoreAvg := float64(cpuMillicoreTotal) / float64(len(cpuData))
	millicoreUsage := cpuMillicoreAvg / float64(cpuMillicoreLimit)
	Logger.Tracef("cpuMillicoreHistory: %v", cpuMillicoreHistory)
	Logger.Tracef("cpuMillicoreAvg: %v, cpuMillicoreLimit: %v, percentageOfAllocatedMillicores: %v", cpuMillicoreAvg, cpuMillicoreLimit, millicoreUsage*100)

	// Calculate CPU availability (1.0 means fully available, 0.0 means fully utilized)
	// Assume 100% per CPU core as maximum, so availability = (100 - usage_percentage) / 100
	cpuAvailability := 1.0 - millicoreUsage
	if cpuAvailability < 0 {
		cpuAvailability = 0 // Ensure availability doesn't go negative
	}
	return float32(cpuAvailability)
}

func GetProcessStats() *ProcessStats {
	ps := &ProcessStats{}

	ps.CPUAvailability = cpuAvailability()
	ps.MemoryAvailability = memoryAvailability()

	return ps
}

func (ps *ProcessStats) IsUnhealthyUsage() bool {
	if ps.CPUAvailability == -1.0 {
		Logger.Trace("Unable to determine CPU availability")
	}

	if ps.MemoryAvailability == -1.0 {
		Logger.Trace("Unable to determine Memory availability")
	}

	if ps.CPUAvailability >= 0 && ps.CPUAvailability < cpuAvailabilityThreshold {
		return true
	}

	if ps.MemoryAvailability >= 0 && ps.MemoryAvailability < memoryAvailabilityThreshold {
		return true
	}

	return false
}

type cpuProcStat struct {
	utime      int64
	stime      int64
	gatherTime time.Time
}
