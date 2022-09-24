package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const taskTimeout = 10 * time.Second

type Phase string

const (
	MapPhase    Phase = "mapPhase"
	ReducePhase Phase = "reducePhase"
	FinishPhase Phase = "finishPhase"
)

type Coordinator struct {
	nextTaskID                  int
	workIDMut                   sync.Mutex
	nextWorkerID                int
	numOfReduceTasks            int
	mapTasks                    map[int]*MapTask
	completedMapTaskCount       int
	completedMapTaskCountMut    sync.Mutex
	reduceTasks                 map[int]*ReduceTask
	completedReduceTaskCount    int
	completedReduceTaskCountMut sync.Mutex
	phase                       Phase
	phaseMux                    sync.RWMutex
}

func (c *Coordinator) RegisterWorker(args Empty, reply *RegisterWorkerReply) error {
	c.workIDMut.Lock()
	defer c.workIDMut.Unlock()
	reply.WorkerID = c.nextWorkerID
	c.nextWorkerID++
	log.Printf("Registered worker: workerID=%v", reply.WorkerID)
	return nil
}

func (c *Coordinator) RequestTask(args RequestTaskArgs, reply *TaskReply) error {
	log.Printf("Enter RequestTask: args=%v\n", args)

	c.phaseMux.RLock()
	phase := c.phase
	c.phaseMux.RUnlock()

	switch phase {
	case MapPhase:
		reply.Type = MapTaskType
		mapTask, err := c.findIdleOrTimeoutMapTask()
		if err != nil {
			log.Println(err)
			return err
		}

		log.Printf("Allocated map task: %v", mapTask.String())

		mapTask.mut.Lock()
		if !mapTask.canAllocate(taskTimeout) {
			mapTask.mut.Unlock()
			log.Printf("inconsistent state for map task: %v\n", mapTask.taskID)
			return fmt.Errorf("map task is updated: %v\n", mapTask.taskID)
		}

		mapTask.workerID = args.WorkerID
		mapTask.state = InProgress
		mapTask.startedAt = time.Now()
		mapTask.mut.Unlock()
		log.Printf("Starting map task: %v", mapTask.String())

		reply.MapTask = MapTaskReply{
			TaskID:           mapTask.taskID,
			InputFilePath:    mapTask.inputFilePath,
			NumOfReduceTasks: mapTask.numOfReduceTasks,
		}
	case ReducePhase:
		reply.Type = ReduceTaskType
		reduceTask, err := c.findIdleOrTimeoutReduceTask()
		if err != nil {
			log.Println(err)
			return err
		}

		log.Printf("Allocated reduce task: %v", reduceTask.String())

		reduceTask.mut.Lock()
		if !reduceTask.canAllocate(taskTimeout) {
			reduceTask.mut.Unlock()
			log.Printf("inconsistent state for reduce task: %v\n", reduceTask.taskID)
			return fmt.Errorf("reduce task is updated: %v\n", reduceTask.taskID)
		}

		reduceTask.workerID = args.WorkerID
		reduceTask.state = InProgress
		reduceTask.startedAt = time.Now()
		reduceTask.mut.Unlock()
		log.Printf("Starting reduce task: %v", reduceTask.String())

		reply.ReduceTask = ReduceTaskReply{
			TaskID:         reduceTask.taskID,
			PartitionIndex: reduceTask.partitionIndex,
			Inputs:         reduceTask.inputs,
		}
	case FinishPhase:
		reply.Type = ShutdownTaskType
		log.Println("Started shutdown task")
	}

	return nil
}

func (c *Coordinator) CompleteMapTask(args CompleteTaskArgs, reply *Empty) error {
	mapTask := c.mapTasks[args.TaskID]
	if !mapTask.tryComplete(args.WorkerID) {
		return nil
	}

	log.Printf("Completed mapTask: taskID=%v", mapTask.taskID)
	if c.addAndCompareCompletedMapTaskCount(1, len(c.mapTasks)) {
		var reduceInputs []ReduceInput
		for taskID, currMapTask := range c.mapTasks {
			reduceInput := ReduceInput{
				MapTaskID: taskID,
				WorkerID:  currMapTask.workerID,
			}
			reduceInputs = append(reduceInputs, reduceInput)
		}

		for partitionIndex := 0; partitionIndex < c.numOfReduceTasks; partitionIndex++ {
			c.reduceTasks[c.nextTaskID] = &ReduceTask{
				taskID:         c.nextTaskID,
				partitionIndex: partitionIndex,
				inputs:         reduceInputs,
				state:          IdleTaskState,
			}
			log.Printf("Created reduce task: %v", c.reduceTasks[c.nextTaskID])
			c.nextTaskID++
		}

		c.phaseMux.Lock()
		c.phase = ReducePhase
		c.phaseMux.Unlock()
		log.Println("Entered reduce phase")
	}

	return nil
}

func (c *Coordinator) CompleteReduceTask(args CompleteTaskArgs, reply *Empty) error {
	reduceTask := c.reduceTasks[args.TaskID]
	if !reduceTask.tryComplete(args.WorkerID) {
		return nil
	}

	log.Printf("Completed reduceTask: taskID=%v", reduceTask.taskID)
	if c.addAndCompareCompletedReduceTaskCount(1, len(c.reduceTasks)) {
		c.phaseMux.Lock()
		c.phase = FinishPhase
		c.phaseMux.Unlock()
		log.Println("Entered finish phase")
	}

	return nil
}

func (c *Coordinator) addAndCompareCompletedMapTaskCount(delta int, otherCount int) bool {
	c.completedMapTaskCountMut.Lock()
	defer c.completedMapTaskCountMut.Unlock()
	c.completedMapTaskCount += delta
	return c.completedMapTaskCount == otherCount
}

func (c *Coordinator) addAndCompareCompletedReduceTaskCount(delta int, otherCount int) bool {
	c.completedReduceTaskCountMut.Lock()
	defer c.completedReduceTaskCountMut.Unlock()
	c.completedReduceTaskCount += delta
	return c.completedReduceTaskCount == otherCount
}

func (c *Coordinator) findIdleOrTimeoutMapTask() (*MapTask, error) {
	for _, mapTask := range c.mapTasks {
		mapTask.mut.RLock()
		if mapTask.state == IdleTaskState {
			mapTask.mut.RUnlock()
			return mapTask, nil
		}

		mapTask.mut.RUnlock()
	}

	for _, mapTask := range c.mapTasks {
		mapTask.mut.RLock()
		if mapTask.canAllocate(taskTimeout) {
			mapTask.mut.RUnlock()
			return mapTask, nil
		}

		mapTask.mut.RUnlock()
	}

	return nil, errors.New("no map task found")
}

func (c *Coordinator) findIdleOrTimeoutReduceTask() (*ReduceTask, error) {
	for _, reduceTask := range c.reduceTasks {
		reduceTask.mut.RLock()
		if reduceTask.state == IdleTaskState {
			reduceTask.mut.RUnlock()
			return reduceTask, nil
		}

		reduceTask.mut.RUnlock()
	}

	for _, reduceTask := range c.reduceTasks {
		reduceTask.mut.RLock()
		if reduceTask.canAllocate(taskTimeout) {
			reduceTask.mut.RUnlock()
			return reduceTask, nil
		}

		reduceTask.mut.RUnlock()
	}

	return nil, errors.New("no reduce task found")
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
	log.Printf("Server started at: %v", sockname)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.phaseMux.RLock()
	defer c.phaseMux.RUnlock()
	return c.phase == FinishPhase
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nextTaskID:       1,
		nextWorkerID:     1,
		numOfReduceTasks: nReduce,
		mapTasks:         map[int]*MapTask{},
		reduceTasks:      map[int]*ReduceTask{},
		phase:            MapPhase,
	}
	for _, file := range files {
		c.mapTasks[c.nextTaskID] = &MapTask{
			taskID:           c.nextTaskID,
			inputFilePath:    file,
			numOfReduceTasks: nReduce,
			state:            IdleTaskState,
		}
		log.Printf("Created map task: %v", c.mapTasks[c.nextTaskID])
		c.nextTaskID++
	}

	c.server()
	return &c
}
