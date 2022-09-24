package mr

import (
	"fmt"
	"sync"
	"time"
)

type TaskType string

const (
	MapTaskType      TaskType = "mapTask"
	ReduceTaskType   TaskType = "reduceTask"
	ShutdownTaskType TaskType = "shutdownTask"
)

type TaskState string

const (
	IdleTaskState TaskState = "idle"
	InProgress    TaskState = "inProgress"
	Completed     TaskState = "completed"
)

type MapTask struct {
	taskID           int
	inputFilePath    string
	numOfReduceTasks int
	state            TaskState
	startedAt        time.Time
	workerID         int
	mut              sync.RWMutex
}

func (m *MapTask) tryComplete(workerID int) bool {
	m.mut.Lock()
	defer m.mut.Unlock()
	if m.state == Completed {
		return false
	}

	m.workerID = workerID
	m.state = Completed
	return true
}

func (m *MapTask) canAllocate(timeout time.Duration) bool {
	if m.state == IdleTaskState {
		return true
	}

	return m.state == InProgress && time.Now().After(m.startedAt.Add(timeout))
}

func (m *MapTask) String() string {
	m.mut.RLock()
	defer m.mut.RUnlock()
	return fmt.Sprintf("[MapTask (TaskID=%v) (InputFilePath=%v) (NumOfReduceTasks=%v) (state=%v) (startedAt=%v) (workerID=%v)]",
		m.taskID,
		m.inputFilePath,
		m.numOfReduceTasks,
		m.state,
		m.startedAt,
		m.workerID)
}

type ReduceInput struct {
	MapTaskID int
	WorkerID  int
}

func (r ReduceInput) String() string {
	return fmt.Sprintf("[ReduceInput (MapTaskID=%v) (WorkerID=%v)]",
		r.MapTaskID,
		r.WorkerID)
}

type ReduceTask struct {
	taskID         int
	partitionIndex int
	inputs         []ReduceInput
	state          TaskState
	startedAt      time.Time
	workerID       int
	mut            sync.RWMutex
}

func (r *ReduceTask) tryComplete(workerID int) bool {
	r.mut.Lock()
	defer r.mut.Unlock()
	if r.state == Completed {
		return false
	}

	r.workerID = workerID
	r.state = Completed
	return true
}

func (r *ReduceTask) canAllocate(timeout time.Duration) bool {
	if r.state == IdleTaskState {
		return true
	}

	return r.state == InProgress && time.Now().After(r.startedAt.Add(timeout))
}

func (r *ReduceTask) String() string {
	r.mut.RLock()
	defer r.mut.RUnlock()
	return fmt.Sprintf("[ReduceTask (TaskID=%v) (PartitionIndex=%v) (Inputs=%v) (state=%v) (startedAt=%v) (workerID=%v)]",
		r.taskID,
		r.partitionIndex,
		r.inputs,
		r.state,
		r.startedAt,
		r.workerID)
}

type MapTaskReply struct {
	TaskID           int
	InputFilePath    string
	NumOfReduceTasks int
}

type ReduceTaskReply struct {
	TaskID         int
	PartitionIndex int
	Inputs         []ReduceInput
}

type TaskReply struct {
	Type       TaskType
	MapTask    MapTaskReply
	ReduceTask ReduceTaskReply
}

func (t *TaskReply) String() string {
	return fmt.Sprintf("[Task (Type=%v) (MapTask=%v) (ReduceTask=%v)]",
		t.Type,
		t.MapTask,
		t.ReduceTask)
}
