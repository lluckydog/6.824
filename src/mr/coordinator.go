package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	MapTasks          chan string
	ReduceTasks       chan int
	mu                sync.RWMutex
	MapStatusTable    []MapTask
	ReduceStatueTable []ReduceTask
	table             map[string]int
	NReduce           int
	NMAP              int
	Status            Task
}

type TaskSTATUS int

const (
	pending TaskSTATUS = iota
	doing
	finish
)

type MapTask struct {
	statue    TaskSTATUS
	filename  string
	StartTime time.Time
}

type ReduceTask struct {
	statue    TaskSTATUS
	Num       int
	StartTime time.Time
}

type Snapshot struct {
	MapCnt    int
	ReduceCnt int
	Doing     int
	Status    Task
}

const timeout = 10 * time.Second

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Status == FINISH {
		return true
	}
	return false
}

func (c *Coordinator) FinishMap() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.Status != MAP {
		return true
	}

	for _, s := range c.MapStatusTable {
		if s.statue != finish {
			return false
		}
	}
	return true
}

func (c *Coordinator) AllDoingMap() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, s := range c.MapStatusTable {
		if s.statue == pending {
			return false
		}
	}
	return true
}

func (c *Coordinator) FinishReduce() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, s := range c.ReduceStatueTable {
		if s.statue != finish {
			return false
		}
	}
	return true
}

func (c *Coordinator) AllDoingReduce() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, s := range c.ReduceStatueTable {
		if s.statue == pending {
			return false
		}
	}
	return true
}

func (c *Coordinator) InMap() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Status == MAP
}

func (c *Coordinator) Call(lastTask *RealArgs, reply *RealReply) {
	if lastTask.LastTask >= 0 {
		c.mu.Lock()
		if lastTask.Type == MAP && c.MapStatusTable[lastTask.LastTask].statue == doing {
			c.MapStatusTable[lastTask.LastTask].statue = finish
		} else if lastTask.Type == REDUCE && c.ReduceStatueTable[lastTask.LastTask].statue == doing {
			c.ReduceStatueTable[lastTask.LastTask].statue = finish
		}
		c.mu.Unlock()
	}

	if !c.FinishMap() {
		if c.AllDoingMap() {
			reply.Type = IDLE
			return
		}
		task, ok := <-c.MapTasks
		if !ok {
			reply.Type = IDLE
		} else {
			c.mu.Lock()
			index := c.table[task]
			c.MapStatusTable[index].statue = doing
			c.MapStatusTable[index].StartTime = time.Now()
			arg := MapArgs{
				Filename:  task,
				ReudceNum: c.NReduce,
				Number:    c.table[task],
			}
			reply.Type = MAP
			reply.MapTask = arg
			reply.TaskIndex = c.table[task]
			c.mu.Unlock()
		}
		return
	} else if c.InMap() {
		c.mu.Lock()
		c.Status = REDUCE
		close(c.MapTasks)
		c.mu.Unlock()
	}

	if !c.FinishReduce() {
		if c.AllDoingReduce() {
			reply.Type = IDLE
			return
		} else {
			r, ok := <-c.ReduceTasks
			if !ok {
				reply.Type = IDLE
				return
			} else {
				c.mu.Lock()
				c.ReduceStatueTable[r].statue = doing
				c.ReduceStatueTable[r].StartTime = time.Now()
				arg := ReudceArgs{
					Number: r,
					MapCnt: c.NMAP,
				}
				reply.ReduceTask = arg
				reply.Type = REDUCE
				reply.TaskIndex = r
				c.mu.Unlock()
			}
		}
	} else {
		c.mu.Lock()
		c.Status = FINISH
		fmt.Println("finish all")
		close(c.ReduceTasks)
		c.mu.Unlock()
		reply.Type = FINISH
	}
}

func (c *Coordinator) SendTask(args *RealArgs, reply *RealReply) error {
	// do map
	c.Call(args, reply)
	return nil
}

func (c *Coordinator) ExamineTimeOut() {
	for {
		c.mu.Lock()
		if c.Status == MAP {
			for i, s := range c.MapStatusTable {
				if s.statue == doing && time.Now().Sub(s.StartTime) > timeout {
					c.MapTasks <- s.filename
					c.MapStatusTable[i].statue = pending
					fmt.Println("restart map")
				}
			}
		} else if c.Status == REDUCE {
			for i, s := range c.ReduceStatueTable {
				if s.statue == doing && time.Now().Sub(s.StartTime) > timeout {
					c.ReduceTasks <- s.Num
					c.ReduceStatueTable[i].statue = pending
					fmt.Println("restart reduce")
				}
			}
		}
		c.mu.Unlock()
		time.Sleep(5 * time.Second)
	}

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := make(chan string, len(files))
	table := make(map[string]int)
	statusTable := make([]MapTask, len(files))
	reduceTable := make([]ReduceTask, nReduce)
	for i, file := range files {
		mapTasks <- file
		table[file] = i
		statusTable[i] = MapTask{
			statue:   pending,
			filename: file,
		}
	}
	reudceTasks := make(chan int, nReduce)
	for i := 0; i < nReduce; i++ {
		reudceTasks <- i
		reduceTable[i] = ReduceTask{
			statue: pending,
			Num:    i,
		}
	}
	c := Coordinator{
		MapTasks:          mapTasks,
		ReduceTasks:       reudceTasks,
		NMAP:              len(table),
		NReduce:           nReduce,
		Status:            MAP,
		table:             table,
		MapStatusTable:    statusTable,
		ReduceStatueTable: reduceTable,
	}

	// Your code here.
	c.server()
	go c.ExamineTimeOut()
	return &c
}
