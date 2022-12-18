package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"time"
)

func main() {
	args := os.Args[1:]
	if len(args) < 5 {
		fmt.Println("Usage: go run tester/main.go [testPattern] [testSrcDir] [totalTimes] [logOutputDir] [numParallelWorkers]")
		fmt.Println("Eg. go run tester/main.go TestFigure8Unreliable2C ./raft/... 2000 ./logs/TestFigure8Unreliable2C-2 10")
		return
	}

	testPattern := args[0]
	testSrcDir := args[1]
	totalTimesArg, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		panic(err)
	}

	logOutputDir := args[3]
	numWorkersArg, err := strconv.ParseInt(args[4], 10, 64)
	if err != nil {
		panic(err)
	}

	numWorkers := int(numWorkersArg)
	totalTimes := int(totalTimesArg)

	os.RemoveAll(logOutputDir)
	err = os.MkdirAll(logOutputDir, 0700)
	err = os.MkdirAll(logOutputDir, 0700)
	if err != nil {
		panic(err)
	}

	runTestCh := make(chan int)
	finishRunCh := make(chan bool)

	startAt := time.Now()
	for worker := 0; worker < numWorkers; worker++ {
		go func() {
			for testIndex := range runTestCh {
				func(testIndex int) {
					path := fmt.Sprintf("%v/%v.log", logOutputDir, testIndex)
					outputFile, err := os.Create(path)
					if err != nil {
						fmt.Println(err)
						finishRunCh <- false
						return
					}

					defer outputFile.Close()

					cmd := exec.Command("go", "test", testSrcDir, "--race", "-run", testPattern, "-timeout", "1h", "--count=1", "-v")
					stdoutPipe, err := cmd.StdoutPipe()
					if err != nil {
						panic(err)
					}

					writer := bufio.NewWriter(outputFile)
					go io.Copy(writer, stdoutPipe)

					err = cmd.Start()
					if err != nil {
						fmt.Println(err)
						finishRunCh <- false
						return
					}

					err = cmd.Wait()
					if err != nil {
						fmt.Printf("Error: testIndex=%v, err=%v\n", testIndex, err)
						finishRunCh <- false
						return
					}

					os.Remove(path)
					finishRunCh <- true
				}(testIndex)
			}
		}()
	}

	go func() {
		for runIndex := 0; runIndex < totalTimes; runIndex++ {
			runTestCh <- runIndex
		}

		close(runTestCh)
	}()

	totalFinishedTests := 0
	passedTests := 0
	failedTests := 0
	for testResult := range finishRunCh {
		totalFinishedTests++
		fmt.Printf("Total finished tests:%v, failed tests:%v, total time:%v\n", totalFinishedTests, failedTests, time.Now().Sub(startAt))

		if testResult {
			passedTests++
		} else {
			failedTests++
		}

		if totalFinishedTests == totalTimes {
			close(finishRunCh)
		}
	}

	fmt.Printf("Finished running all tests: total=%v passed=%v failed=%v\n", totalFinishedTests, passedTests, failedTests)
}
