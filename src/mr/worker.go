package mr

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"
)

const mapOutputPrefix = "mr-intermediate-"

type KeyValue struct {
	Key   string
	Value string
}

type KeyValues struct {
	Key    string
	Values []string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	empty := Empty{}
	registerWorkerReply := RegisterWorkerReply{}
	succeed, err := call("Coordinator.RegisterWorker", &empty, &registerWorkerReply)
	if err != nil {
		log.Println(err)
		return
	}

	if !succeed {
		return
	}

	workerID := registerWorkerReply.WorkerID
	log.Printf("Received worker ID: workerID=%v", workerID)

	for {
		requestTaskArgs := RequestTaskArgs{
			WorkerID: workerID,
		}
		task := TaskReply{}
		succeed, err = call("Coordinator.RequestTask", &requestTaskArgs, &task)
		if err != nil {
			log.Println(err)
			log.Printf("Shutdown worker: workerID=%v", workerID)
			return
		}

		if !succeed {
			time.Sleep(2 * time.Second)
			continue
		}

		log.Printf("Received task: %v", task)
		switch task.Type {
		case ShutdownTaskType:
			log.Printf("Shutdown worker: workerID=%v", workerID)
			return
		case MapTaskType:
			log.Printf("Start executing mapTask: taskID=%v", task.MapTask.TaskID)
			err = executeMapTask(workerID, task.MapTask, mapf)
			if err != nil {
				log.Println(err)
				continue
			}

			log.Printf("Finished executing mapTask: taskID=%v", task.MapTask.TaskID)

			log.Printf("Report mapTask complete: taskID=%v", task.MapTask.TaskID)
			completeTaskArgs := CompleteTaskArgs{
				TaskID:   task.MapTask.TaskID,
				WorkerID: workerID,
			}
			_, err = call("Coordinator.CompleteMapTask", &completeTaskArgs, &Empty{})
			if err != nil {
				log.Println(err)
				log.Printf("Shutdown worker: workerID=%v", workerID)
				return
			}
		case ReduceTaskType:
			log.Printf("Start executing reduceTask: taskID=%v", task.ReduceTask.TaskID)
			err = executeReduceTask(task.ReduceTask, reducef)
			if err != nil {
				log.Println(err)
				fmt.Printf("Fail to execute reduce task: taskID=%v\n", task.ReduceTask.TaskID)
				continue
			}
			log.Printf("Finished executing reduceTask: taskID=%v", task.ReduceTask.TaskID)

			log.Printf("Report reduceTask complete: taskID=%v", task.ReduceTask.TaskID)
			completeTaskArgs := CompleteTaskArgs{
				TaskID:   task.ReduceTask.TaskID,
				WorkerID: workerID,
			}
			_, err = call("Coordinator.CompleteReduceTask", &completeTaskArgs, &Empty{})
			if err != nil {
				log.Println(err)
				log.Printf("Shutdown worker: workerID=%v", workerID)
				return
			}
		}

	}
}

func executeMapTask(workerID int, mapTask MapTaskReply, mapFunc func(string, string) []KeyValue) error {
	inputBuf, err := os.ReadFile(mapTask.InputFilePath)
	if err != nil {
		log.Println(err)
		return err
	}

	outputFiles := make([]*os.File, 0)
	for partitionIndex := 0; partitionIndex < mapTask.NumOfReduceTasks; partitionIndex++ {
		outputFileName := getMapTaskOutputFileName(workerID, mapTask.TaskID, partitionIndex)
		outputFile, err := os.Create(outputFileName)
		if err != nil {
			log.Println(err)
			return err
		}

		outputFiles = append(outputFiles, outputFile)
	}

	value := string(inputBuf)
	kvPairs := mapFunc(mapTask.InputFilePath, value)
	for _, kvPair := range kvPairs {
		partitionIndex := ihash(kvPair.Key) % mapTask.NumOfReduceTasks
		outputFile := outputFiles[partitionIndex]
		_, err = outputFile.WriteString(fmt.Sprintf("%v %v\n", kvPair.Key, kvPair.Value))
		if err != nil {
			log.Println(err)
			continue
		}
	}

	for _, outputFile := range outputFiles {
		outputFile.Close()
	}

	return nil
}

func executeReduceTask(reduceTask ReduceTaskReply, reduceFunc func(string, []string) string) error {
	outputTmpFileName := fmt.Sprintf("mr-out-tmp-%v", reduceTask.PartitionIndex)
	outputTmpFile, err := os.Create(outputTmpFileName)
	if err != nil {
		log.Println(err)
		return err
	}

	defer outputTmpFile.Close()

	var allKVPairs []KeyValue
	for _, reduceInput := range reduceTask.Inputs {
		inputFileName := getMapTaskOutputFileName(reduceInput.WorkerID, reduceInput.MapTaskID, reduceTask.PartitionIndex)
		kvPairs, err := readKeyValues(inputFileName)
		if err != nil {
			log.Println(err)
			continue
		}

		allKVPairs = append(allKVPairs, kvPairs...)
	}

	keyToValues := toKeyValues(allKVPairs)

	for _, keyValues := range keyToValues {
		output := reduceFunc(keyValues.Key, keyValues.Values)
		_, err = outputTmpFile.WriteString(fmt.Sprintf("%v %v\n", keyValues.Key, output))
		if err != nil {
			log.Println(err)
			continue
		}
	}

	outputFileName := fmt.Sprintf("mr-out-%v", reduceTask.PartitionIndex)
	return os.Rename(outputTmpFileName, outputFileName)
}

func toKeyValues(kvPairs []KeyValue) []KeyValues {
	if len(kvPairs) < 1 {
		return nil
	}

	sort.Slice(kvPairs, func(i, j int) bool {
		return kvPairs[i].Key < kvPairs[j].Key
	})

	var keyValuesList []KeyValues
	currKeyValues := KeyValues{Key: kvPairs[0].Key}

	for _, kvPair := range kvPairs {
		if kvPair.Key != currKeyValues.Key {
			keyValuesList = append(keyValuesList, currKeyValues)
			currKeyValues = KeyValues{Key: kvPair.Key}
		}

		currKeyValues.Values = append(currKeyValues.Values, kvPair.Value)
	}

	return append(keyValuesList, currKeyValues)
}

func readKeyValues(inputFileName string) ([]KeyValue, error) {
	inputFile, err := os.Open(inputFileName)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	defer inputFile.Close()
	reader := bufio.NewReader(inputFile)

	var kvPairs []KeyValue
	for {
		lineBuf, _, err := reader.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return kvPairs, nil
			}

			return nil, err
		}

		line := string(lineBuf)
		columns := strings.Split(line, " ")
		if len(columns) != 2 {
			return nil, fmt.Errorf("invalid file format: %v", inputFileName)
		}

		kvPair := KeyValue{
			Key:   columns[0],
			Value: columns[1],
		}
		kvPairs = append(kvPairs, kvPair)
	}
}

func getMapTaskOutputFileName(workerID int, mapTaskID int, partitionIndex int) string {
	return fmt.Sprintf("%v%v-%v-%v", mapOutputPrefix, workerID, mapTaskID, partitionIndex)
}
