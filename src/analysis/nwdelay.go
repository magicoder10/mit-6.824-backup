package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
)

var prefixes = []string{
	"request",
	"response",
}

func main() {
	args := os.Args[1:]
	if len(args) < 2 {
		fmt.Println("Usage: go run analysis/nwdelay.go [methodPattern] [logFilePath]")
		return
	}

	methodPattern := args[0]
	logFilePath := args[1]
	file, err := os.Open(logFilePath)
	if err != nil {
		panic(err)
	}

	defer file.Close()

	for _, prefix := range prefixes {
		file.Seek(0, 0)
		var delayPattern = regexp.MustCompile(fmt.Sprintf("%v is delayed for ([0-9]+) ms: [*]%v", prefix, methodPattern))

		totalDelay := 0
		delayCount := 0
		maxDelay := 0
		scanner := bufio.NewScanner(file)

		var delays []int

		for scanner.Scan() {
			line := scanner.Text()
			matches := delayPattern.FindStringSubmatch(line)
			if len(matches) < 2 {
				continue
			}

			parsedDelay, err := strconv.ParseInt(matches[1], 10, 64)
			if err != nil {
				panic(err)
			}

			delay := int(parsedDelay)
			delays = append(delays, delay)

			totalDelay += delay
			if delay > maxDelay {
				maxDelay = delay
			}

			delayCount++
		}

		average := 0
		if delayCount > 0 {
			average = totalDelay / delayCount
		}

		sort.Ints(delays)
		fmt.Printf("[%v] totalDelay=%vms count=%v max delay=%v average delay=%vms medium delay=%vms\n", prefix, totalDelay, delayCount, maxDelay, average, medium(delays))
	}
}

func medium(nums []int) int {
	if len(nums) == 0 {
		return 0
	}

	halfIndex := len(nums) / 2
	if len(nums)%2 == 1 {
		return nums[halfIndex]
	}

	return (nums[halfIndex-1] + nums[halfIndex]) / 2
}
