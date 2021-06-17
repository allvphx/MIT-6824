package mr

import (
	"fmt"
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

func (c *Coordinator) GetArgs(Args *WorkerArgs, Reply *WorkerReply) error {
	// assign as a mapper or a reducer here.
	// the blocking is done here
	c.mu.Lock()
	defer c.mu.Unlock()
	// if the mapper task is not finished
	/*for _, x := range c.mapperState {
		fmt.Print(x)
	}
	fmt.Print("\n")
	for _, x := range c.reducerState {
		fmt.Print(x)
	}
	fmt.Print("\n")*/

	Reply.Cmd = 2 // default no task

	if c.cMap < c.nMap {
		for i, state := range c.mapperState {
			if state == 0 {
				// the unprocessed mappers
				c.mapperState[i] = 1
				Reply.Cmd = 0
				Reply.X = i
				Reply.NReduce = c.nReduce
				Reply.Filename = c.files[i]
				c.beginTime[i] = time.Now()
				break

			} else if state == 1 {
				// the time out mapper
				timeSpan := time.Now().Sub(c.beginTime[i])
				if timeSpan > 10*time.Second {
					fmt.Println("Time out for ", i)
					Reply.Cmd = 0
					Reply.X = i
					Reply.NReduce = c.nReduce
					Reply.Filename = c.files[i]
					c.beginTime[i] = time.Now()
					break
				}
			}
		}
	} else if c.cReduce < c.nReduce { // if the reducer task is not finished
		for i, state := range c.reducerState {
			if state == 0 {
				// the unprocessed reducers
				c.reducerState[i] = 1
				Reply.Cmd = 1
				Reply.Y = i
				c.beginTime[i+c.nMap] = time.Now()
				break

			} else if state == 1 {
				// the timed-out reducers
				timeSpan := time.Now().Sub(c.beginTime[i+c.nMap])
				if timeSpan > 10*time.Second {
					Reply.Cmd = 1
					Reply.Y = i
					c.beginTime[i+c.nMap] = time.Now()
					break
				}
			}
		}
	}

	return nil
}

func (c *Coordinator) Finsh(Args *WorkerArgs, Reply *WorkerReply) error {
	// release the task
	c.mu.Lock()
	defer c.mu.Unlock()
	if Args.Cmd == 0 && c.mapperState[Args.X] != 2 {
		c.mapperState[Args.X] = 2
		c.cMap++
	}
	if Args.Cmd == 1 && c.reducerState[Args.Y] != 2 {
		c.reducerState[Args.Y] = 2
		c.cReduce++
	}
	Reply.Cmd = 0
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
	if ret {
		//		println("Done called with ", c.nReduce, c.cReduce)
	}
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
		mapperState:  make([]int, len(files), len(files)),
		reducerState: make([]int, nReduce, nReduce),
		beginTime:    make([]time.Time, len(files)+nReduce), // mapper, then reducer
	}
	c.server()
	return &c
}
