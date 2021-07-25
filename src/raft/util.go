package raft

import (
	"fmt"
	"sync"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		fmt.Printf(time.Now().Format("15:04:05.00")+" <---> "+format+"\n", a...)
	}
	return
}

func TDPrintf(mu *sync.Mutex, format string, a ...interface{}) {
	mu.Lock()
	if Debug {
		fmt.Printf(time.Now().Format("15:04:05.00")+" <---> "+format+"\n", a...)
	}
	mu.Unlock()
	return
}
