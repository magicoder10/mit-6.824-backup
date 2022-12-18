package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	args := os.Args[1:]
	if len(args) < 1 {
		fmt.Println("Usage: go run analysis/count.go [substring] [logFilePath]")
		return
	}

	subStr := args[0]
	logFilePath := args[1]
	file, err := os.Open(logFilePath)
	if err != nil {
		panic(err)
	}

	defer file.Close()
	count := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, subStr) {
			count++
		}
	}

	fmt.Printf("total count=%v\n", count)
}
