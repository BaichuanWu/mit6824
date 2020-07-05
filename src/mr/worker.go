package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"time"
	"os"
	"io/ioutil"
	"sort"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key)) //nolint: errcheck
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := ApplyTaskArgs{}
		reply := ApplyTaskReply{}
		isWork := call("Master.ApplyTask", &args, &reply)
		if !isWork{
			break
		}
		if len(reply.TaskFiles) == 0 {
			time.Sleep(2 * time.Second)
		}
		if reply.TaskType == TaskTypeMap {
			HandleMapTask(mapf, &reply)
		} else if reply.TaskType == TaskTypeReduce {
			HandleReduceTask(reducef, &reply)
		}
	}	
	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()


}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func HandleMapTask (mapf func(string, string) []KeyValue, applyReply *ApplyTaskReply) {
	fileMap := make(map[string] []KeyValue)
	for _, f := range applyReply.TaskFiles{
		filename := f.Filename
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		for _, kv := range kva {
			outFilename := fmt.Sprintf("mr-%d-%d", f.WorkerID, ihash(kv.Key) % applyReply.ReduceCnt)
			fileMap[outFilename] = append(fileMap[outFilename], kv)
		}
	}
	outFiles := make([]TaskFile, 0)
	for filename, kva:= range fileMap {
		ofile, _ := os.Create(filename)
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			_ = enc.Encode(kv)
		}
		ofile.Close()
		outFiles = append(outFiles, TaskFile{Filename:filename,
			TaskType:TaskTypeReduce,
			WorkerID:0,
			Status:TaskFileStatusInit,
		})
	}
	args := ReportTaskArgs{WorkerID:applyReply.TaskFiles[0].WorkerID, TaskType:applyReply.TaskType, OutFiles:outFiles}
	reply := ReportTaskReply{}
	call("Master.ReportTask", &args, &reply)
}

func HandleReduceTask (reducef func(string, []string) string, applyReply *ApplyTaskReply) {
	intermediate := make([]KeyValue, 0)
	for _, f := range applyReply.TaskFiles{
		filename := f.Filename
		ifile, _ := os.Open(filename)
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}	
	}
	oname := fmt.Sprintf("mr-out-%d", applyReply.TaskFiles[0].WorkerID)
	sort.Sort(ByKey(intermediate))

	ofile, _ := os.Create(oname)
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
	ofile.Close()
	// 未做master 按照接收到的上报的文件为准（避免master接收失败，结果读取了失败的文件）
	args := ReportTaskArgs{WorkerID:applyReply.TaskFiles[0].WorkerID, TaskType:applyReply.TaskType, OutFiles:[]TaskFile{}}
	reply := ReportTaskReply{}
	call("Master.ReportTask", &args, &reply)
}


//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
