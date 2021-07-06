package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type MsgType uint8

const (
	Map     MsgType = 0
	Reduce  MsgType = 1
	Done    MsgType = 2
	Succeed MsgType = 3
	Fail    MsgType = 4
)

//
//	the communicator between worker and coordinator.
//
type WorkerArgs struct {
	Type MsgType
	X    int
	Y    int
}

type WorkerReply struct {
	Type     MsgType
	X        int
	Y        int
	Filename string
	NReduce  int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
