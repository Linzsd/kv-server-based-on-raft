package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func cloneLogs(origin []LogEntry) []LogEntry {
	x := make([]LogEntry, len(origin))
	copy(x, origin)
	return x
}
