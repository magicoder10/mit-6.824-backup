package raft

import (
	"log"
)

type LogLevel string

const (
	offLogLevel   LogLevel = "Off"
	fatalLogLevel LogLevel = "Fatal"
	errorLogLevel LogLevel = "Error"
	infoLogLevel  LogLevel = "Info"
	debugLogLevel LogLevel = "Debug"
)

var logPriorities = map[LogLevel]int{
	offLogLevel:   0,
	fatalLogLevel: 1,
	errorLogLevel: 2,
	infoLogLevel:  3,
	debugLogLevel: 4,
}

type Flow string

const (
	FollowerFlow       Flow = "Follower"
	CandidateFlow      Flow = "Candidate"
	LeaderFlow         Flow = "Leader"
	LeaderElectionFlow Flow = "Election"
	LogReplicationFlow Flow = "LogReplicate"
	PersistenceFlow    Flow = "Persistence"
	SnapshotFlow       Flow = "Snapshot"
	RPCFlow            Flow = "RPCFlow"
)

type Logger struct {
	replicateID     int
	visibleFlows    map[Flow]bool
	visibleLogLevel LogLevel
}

func (l Logger) log(level LogLevel, flow Flow, message string) {
	if logPriorities[level] > logPriorities[l.visibleLogLevel] {
		return
	}

	visible, ok := l.visibleFlows[flow]
	if !ok || !visible {
		return
	}

	log.Printf("[%v][%v][%v] %v\n", level, l.replicateID, flow, message)
}

func NewLogger(visibleLevel LogLevel, visibleFlows map[Flow]bool, replicateID int) Logger {
	return Logger{
		visibleFlows:    visibleFlows,
		visibleLogLevel: visibleLevel,
		replicateID:     replicateID,
	}
}
