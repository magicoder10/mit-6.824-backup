package main

import (
	"log"
	"math/rand"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	voteYesCount := 0
	totalVoters := 0
	voteCh := make(chan bool)

	for voterIndex := 1; voterIndex <= 10; voterIndex++ {
		go func() {
			voteCh <- requestVote()
		}()
	}

	for vote := range voteCh {
		// Only main goroutine is accessing voteYesCount and totalVoters
		if vote {
			voteYesCount++
		}

		totalVoters++
		if voteYesCount >= 5 || totalVoters == 10 {
			break
		}
	}

	if voteYesCount >= 5 {
		log.Println("Win!")
	} else {
		log.Println("Lose!")
	}
}
