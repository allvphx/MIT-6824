package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// X is the identifier for the map worker.
//
func MapWorker(mapf func(string, string) []KeyValue, filename string, X int, nReduce int) {
	// read each input file
	// calculate the intermediate keys.
	fmt.Println("In mapping -- ", X)

	intermediate := []KeyValue{}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open mapper %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read mapper %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// partition the keys.
	sort.Sort(ByKey(intermediate))

	tempfiles := []*os.File{}
	encs := []*json.Encoder{}

	for i := 0; i < nReduce; i++ {
		tempfile, _ := ioutil.TempFile("./", "mr-temp-*")
		tempfiles = append(tempfiles, tempfile)
		encs = append(encs, json.NewEncoder(tempfile))
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		Y := ihash(intermediate[i].Key) % nReduce

		// use template file to avoid partial write.
		tempfile := tempfiles[Y]
		for k := i; k < j; k++ {
			err := encs[Y].Encode(&intermediate[k])
			if err != nil {
				log.Fatalf("encoding error for %v", tempfile.Name())
			}
		}
		i = j
	}

	// atomic rename of the template file here.
	for i := 0; i < nReduce; i++ {
		opath := tempfiles[i].Name()
		err := os.Rename(opath, "./"+"mr-"+strconv.Itoa(X)+"-"+strconv.Itoa(i))
		tempfiles[i].Close()
		if err != nil {
			log.Fatalf("cannot rename %v", opath)
		}
	}
	fmt.Println("Done mapping -- ", X)
}

//
// Y is the identifer for the reduce worker.
//
func ReduceWorker(reducef func(string, []string) string, Y int) {

	fmt.Println("In reducing -- ", Y)

	// get all the files that writes to Y
	files, err := filepath.Glob("*-" + strconv.Itoa(Y))
	if err != nil {
		log.Fatalf("cannot open the files for %v", Y)
	}

	intermediate := []KeyValue{}

	for _, fname := range files {
		file, err := os.Open(fname)
		if err != nil {
			log.Fatalf("cannot open reducer %v", fname)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// partition on the intermediate keys.
	sort.Sort(ByKey(intermediate))

	ofile, _ := ioutil.TempFile("./", "mr-temp-*")

	// merge and reduce
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	// output the reduced values
	opath := ofile.Name()
	err = os.Rename(opath, "./"+"mr-out-"+strconv.Itoa(Y))
	ofile.Close()
	if err != nil {
		log.Fatalf("cannot rename %v", opath)
	}

	fmt.Println("Done reducing -- ", Y)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := GetWorkerArgs()

		if args.Cmd == 2 {
			continue
		} else if args.Cmd == 3 {
			return
		} else if args.Cmd == 0 {
			MapWorker(mapf, args.Filename, args.X, args.NReduce)
		} else {
			ReduceWorker(reducef, args.Y)
		}

		if !CallFinished(args) {
			log.Fatal("the worker failed to call finsihed")
		}
		time.Sleep(1)
	}
}

func GetWorkerArgs() *WorkerReply {
	args := WorkerArgs{}
	reply := WorkerReply{}
	call("Coordinator.GetArgs", &args, &reply)
	//	println(reply.Cmd, reply.X, reply.Y, reply.Filename)
	return &reply
}

func CallFinished(ctx *WorkerReply) bool {
	args := WorkerArgs{}
	args.Cmd = ctx.Cmd
	args.X = ctx.X
	args.Y = ctx.Y
	reply := WorkerReply{}
	if !call("Coordinator.Finsh", &args, &reply) {
		return true
	}
	return reply.Cmd == 0
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
