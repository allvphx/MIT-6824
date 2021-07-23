package raft

import (
	"fmt"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		fmt.Printf(time.Now().Format("15:04:05.00")+" <---> "+format+"\n", a...)
	}
	return
}
