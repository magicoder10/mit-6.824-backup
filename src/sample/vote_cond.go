package main

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	voteYesCount := 0
	totalVoters := 0
	var mut sync.Mutex
	cond := sync.NewCond(&mut)
	for voterIndex := 0; voterIndex < 10; voterIndex++ {
		go func() {
			vote := requestVote()
			mut.Lock()
			defer mut.Unlock()
			if vote {
				voteYesCount++
			}

			totalVoters++
			cond.Signal()
		}()
	}

	mut.Lock()
	for voteYesCount < 5 && totalVoters < 10 {
		// https://pkg.go.dev/sync#Cond.Wait
		// Wait atomically unlocks c.L and suspends execution of the calling goroutine.
		// After later resuming execution, Wait locks c.L before returning.
		// Unlike in other systems, Wait cannot return unless awoken by Broadcast or Signal.

		// Because c.L is not locked when Wait first resumes, the caller typically cannot
		// assume that the condition is true when Wait returns.
		// Instead, the caller should Wait in a loop:
		// https://stackoverflow.com/questions/61330985/cond-for-loop-clarification
		cond.Wait()
	}

	if voteYesCount >= 5 {
		log.Println("Win!")
	} else {
		log.Println("Lose!")
	}
}
