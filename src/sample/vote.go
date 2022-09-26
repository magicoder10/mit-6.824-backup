package main

import (
	"math/rand"
	"time"
)

const voteYes = 1

func requestVote() bool {
	time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
	return rand.Intn(2) == voteYes
}
