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

type WorkerState int8

const (
	Waiting  WorkerState = 0
	Pending  WorkerState = 1
	Finished WorkerState = 2
)

type Coordinator struct {
	// Your definitions here.
	nReduce int
	nMap    int
	files   []string

	mu           sync.Mutex
	cond         *sync.Cond
	cReduce      int
	cMap         int
	mapperState  []WorkerState // the state for the map tasks 0->1->2
	reducerState []WorkerState // the state for the reduce tasks 0->1->2
	beginTime    []time.Time   // restart one task if time-out
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) setMapper(Reply *WorkerReply, i int) {
	c.mapperState[i] = Pending
	Reply.Type = Map
	Reply.X = i
	Reply.NReduce = c.nReduce
	Reply.Filename = c.files[i]
	c.beginTime[i] = time.Now()
}

func (c *Coordinator) setReducer(Reply *WorkerReply, i int) {
	c.reducerState[i] = Pending
	Reply.Type = Reduce
	Reply.Y = i
	c.beginTime[i+c.nMap] = time.Now()
}

func (c *Coordinator) testMapperState() {
	for _, state := range c.mapperState {
		fmt.Print(state)
	}
	fmt.Print("\n")
}

func (c *Coordinator) HandleGetArgs(Args *WorkerArgs, Reply *WorkerReply) error {
	// assign as a mapper or a reducer here.
	// the blocking is done here
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if c.cReduce == c.nReduce {
			Reply.Type = Done
			return nil
		}

		if c.cMap < c.nMap {
			for i, state := range c.mapperState {
				if state == Waiting {
					c.setMapper(Reply, i)
					return nil
				} else if state == Pending {
					timeSpan := time.Now().Sub(c.beginTime[i])
					if timeSpan > 10*time.Second {
						c.setMapper(Reply, i)
						return nil
					}
				}
			}
		} else if c.cReduce < c.nReduce {
			for i, state := range c.reducerState {
				if state == Waiting {
					c.setReducer(Reply, i)
					return nil

				} else if state == Pending {
					timeSpan := time.Now().Sub(c.beginTime[i+c.nMap])
					if timeSpan > 10*time.Second {
						c.setReducer(Reply, i)
						return nil
					}
				}
			}
		}

		c.cond.Wait()
	}
}

func (c *Coordinator) HandleFinish(Args *WorkerArgs, Reply *WorkerReply) error {
	Reply.Type = Fail
	c.mu.Lock()
	defer c.mu.Unlock()
	if Args.Type == Map && c.mapperState[Args.X] != Finished {
		c.mapperState[Args.X] = Finished
		c.cMap++
	}
	if Args.Type == Reduce && c.reducerState[Args.Y] != Finished {
		c.reducerState[Args.Y] = Finished
		c.cReduce++
	}
	Reply.Type = Succeed
	c.cond.Broadcast()
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
		mapperState:  make([]WorkerState, len(files), len(files)),
		reducerState: make([]WorkerState, nReduce, nReduce),
		beginTime:    make([]time.Time, len(files)+nReduce), // mapper, then reducer
	}
	c.cond = sync.NewCond(&c.mu)

	go func() {
		for !c.Done() {
			c.mu.Lock()
			c.cond.Broadcast()
			c.mu.Unlock()
			time.Sleep(2 * time.Second)
		}
	}()

	c.server()
	return &c
}
