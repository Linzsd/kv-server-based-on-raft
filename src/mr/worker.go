package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type SortedKey []KeyValue

func (k SortedKey) Len() int           { return len(k) }
func (k SortedKey) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k SortedKey) Less(i, j int) bool { return k[i].Key < k[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	unfinishFlag := true
	for unfinishFlag {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(mapf, &task)
				callDone(&task)
			}
		case ReduceTask:
			{
				DoReduceTask(reducef, &task)
				callDone(&task)
			}
		case WaittingTask:
			{
				fmt.Println("[INFO] All tasks are in progress, please wait...")
				time.Sleep(time.Second)
			}
		case ExitTask:
			{
				fmt.Println("[INFO] Exit")
				unfinishFlag = false
			}
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func callDone(finishedTask *Task) Task {
	reply := Task{}
	ok := call("Coordinator.MarkFinished", &finishedTask, &reply)
	if ok {
		fmt.Println("[INFO] close task: ", finishedTask)
	} else {
		fmt.Println("[ERROR] call failed!")
	}
	return reply
}

func GetTask() Task {
	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.PollTask", &args, &reply)
	if !ok {
		fmt.Printf("[ERROR] call failed!\n")
	}
	return reply
}

func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	filename := response.FileSlice[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("[ERROR] cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("[ERROR] cannot read %v", filename)
	}
	file.Close()
	// 返回kv结构体数组
	intermediate := mapf(filename, string(content))

	rn := response.ReducerNum
	HashedKV := make([][]KeyValue, rn)
	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}
	for i := 0; i < rn; i++ {
		outputname := "mr-tmp-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		outputfile, err := os.Create(outputname)
		if err != nil {
			log.Fatalf("[ERROR] cannot create %v", outputname)
		}
		enc := json.NewEncoder(outputfile)
		for _, kv := range HashedKV[i] {
			err := enc.Encode(kv)
			if err != nil {
				log.Fatalf("[ERROR] cannot encode")
				return
			}
		}
		outputfile.Close()
	}
}

func DoReduceTask(reducef func(string, []string) string, response *Task) {
	reduceFileNum := response.TaskId
	intermediate := shuffle(response.FileSlice)
	root_path, _ := os.Getwd()
	outputFile, err := os.CreateTemp(root_path, "mr-tmp-*")
	if err != nil {
		log.Fatal("[ERROR] failed to create output file", err)
	}
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
		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	outputFile.Close()
	fn := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(outputFile.Name(), fn)
}

func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(SortedKey(kva))
	return kva
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
