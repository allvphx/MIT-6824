package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type WorkerArgs struct {
	Cmd int
	X   int
	Y   int
}

//
//	the communicator between worker and coordinator.
//	cmd = 0 for map task or success finished.
//	cmd = 1 for reduce task or failure finished.
//	cmd = 2 for empty round
//	cmd = 3 for termination
//
type WorkerReply struct {
	Cmd      int
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
