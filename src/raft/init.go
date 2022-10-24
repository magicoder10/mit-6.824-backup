package raft

import (
	"log"
	"math/rand"
	"time"
)

func init() {
	log.SetFlags(log.Lmicroseconds)
	rand.Seed(time.Now().UnixNano())
}
