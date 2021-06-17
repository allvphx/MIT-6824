package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nReduce int
	nMap    int
	files   []string

	mu           sync.Mutex
	cReduce      int
	cMap         int
	mapperState  []int       // the state for the map tasks 0->1->2
	reducerState []int       // the state for the reduce tasks 0->1->2
	beginTime    []time.Time // restart one task if time-out
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *WorkerReply) {
	c.mu.Lock()
	// if the mapper task is not finished
	if c.cMap < c.nMap {
		for i, state := range c.mapperState {
			if state == 0 {
				// the unprocessed mappers
				state = 1
				args.cmd = 0
				args.X = i
				args.nReduce = c.cReduce
				args.filename = c.files[i]
				c.beginTime[i] = time.Now()
				break

			} else if state == 1 {
				// the time out mapper
				timeSpan := time.Now().Sub(c.beginTime[i])
				if timeSpan > 10*time.Second {
					args.cmd = 0
					args.X = i
					args.nReduce = c.cReduce
					args.filename = c.files[i]
					c.beginTime[i] = time.Now()
					break
				}
			}
		}
	} else if c.cReduce < c.nReduce { // if the reducer task is not finished
		for i, state := range c.reducerState {
			if state == 0 {
				// the unprocessed reducers
				state = 1
				args.cmd = 1
				args.Y = i
				c.beginTime[i+c.nMap] = time.Now()
				break

			} else if state == 1 {
				// the timed-out reducers
				timeSpan := time.Now().Sub(c.beginTime[i+c.nMap])
				if timeSpan > 10*time.Second {
					args.cmd = 1
					args.Y = i
					c.beginTime[i+c.nMap] = time.Now()
					break
				}
			}
		}
	} else { // all works are done
		args.cmd = 2
	}

	c.mu.Unlock()
}

func (c *Coordinator) GetArgs(args *WorkerArgs, reply *WorkerReply) error {
	// assign as a mapper or a reducer here.
	// the blocking is done here
	c.GetTask(reply)
	return nil
}

func (c *Coordinator) Finshied(args *WorkerArgs, reply *WorkerReply) error {
	// release the task
	c.mu.Lock()
	if reply.cmd == 0 && c.mapperState[reply.X] != 2 {
		c.mapperState[reply.X] = 2
		c.cMap++
	}
	if reply.cmd == 1 && c.reducerState[reply.Y] != 2 {
		c.reducerState[reply.Y] = 2
		c.cReduce++
	}
	c.mu.Unlock()
	reply.cmd = 0
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true
	// if all the reducers are finished, then the task is done.
	c.mu.Lock()
	ret = c.cReduce == c.nReduce
	c.mu.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:        files,
		nMap:         len(files),
		nReduce:      nReduce,
		mapperState:  make([]int, len(files)),
		reducerState: make([]int, nReduce),
		beginTime:    make([]time.Time, len(files)+nReduce), // mapper, then reducer
	}
	c.server()
	return &c
}
