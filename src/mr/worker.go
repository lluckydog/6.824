package mr

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type MapArgs struct {
	Filename  string
	ReudceNum int
	Number    int
}

type ReudceArgs struct {
	//KVs    []KeyValue
	MapCnt int
	Number int
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string, NReduce int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()&0x7fffffff) % NReduce
}

func DoMapTask(taskInfo RealReply, mapf func(string, string) []KeyValue) {
	filename := taskInfo.MapTask.Filename
	number := taskInfo.MapTask.Number
	n := taskInfo.MapTask.ReudceNum
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
	sort.Sort(ByKey(kva))
	onames := make([]string, n)
	ofiles := make([]*os.File, n)

	for i := range onames {
		onames[i] = "mr-" + strconv.Itoa(number) + "-" + strconv.Itoa(i)
		ofiles[i], _ = os.Create(onames[i])
	}

	for i := 0; i < len(kva); i++ {

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofiles[ihash(kva[i].Key, n)], "%v %v\n", kva[i].Key, kva[i].Value)
	}
	for i := range ofiles {
		ofiles[i].Close()
	}
}

func DoReduceTask(taskInfo RealReply, reducef func(string, []string) string) {
	var kva []KeyValue
	num := taskInfo.ReduceTask.Number
	//root := "/Users/danddy/6.824/src/main"
	var files []string
	for i := 0; i < taskInfo.ReduceTask.MapCnt; i++ {
		files = append(files, "mr-"+strconv.Itoa(i)+"-"+strconv.Itoa(num))
	}

	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		fileScanner := bufio.NewScanner(file)
		fileScanner.Split(bufio.ScanLines)
		for fileScanner.Scan() {
			string_slice := strings.Split(fileScanner.Text(), " ")
			kva = append(kva, KeyValue{Key: string_slice[0], Value: string_slice[1]})
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	i := 0
	oname := "mr-out-" + strconv.Itoa(num)
	outfile, _ := os.Create(oname)
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outfile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	outfile.Close()
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	lastTask := -1
	lastType := MAP
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	for true {
		//fmt.Println("ask task")
		taskInfo, err := CallTask(lastTask, lastType)
		if err != nil {
			fmt.Println(err)
			return
		}
		lastTask = taskInfo.TaskIndex
		lastType = taskInfo.Type

		switch taskInfo.Type {
		case MAP:
			DoMapTask(taskInfo, mapf)
		case REDUCE:
			DoReduceTask(taskInfo, reducef)
		case FINISH:
			return
		case IDLE:
			time.Sleep(time.Second)
		default:
			panic(fmt.Sprintf("work jobType %v", taskInfo.Type))
		}
	}

}

func CallTask(lastTask int, lastType Task) (RealReply, error) {
	args := RealArgs{
		LastTask: lastTask,
		Type:     lastType,
	}
	reply := RealReply{}
	ok := call("Coordinator.SendTask", &args, &reply)
	if !ok {
		fmt.Println("call failed!")
		return RealReply{}, errors.New("call error")
	}
	return reply, nil
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
func Map(filename string, contents string) []KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	cnt := 0
	for _, s := range values {
		c, _ := strconv.Atoi(s)
		cnt = cnt + c
	}
	return strconv.Itoa(cnt)
}
