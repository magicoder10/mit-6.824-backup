package raft

import (
	"fmt"
)

type RequestVoteArgs struct {
	RequestID    int
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (r RequestVoteArgs) String() string {
	return fmt.Sprintf("[RequestVoteArgs RequestID=%v Term=%v CandidateId=%v LastLogIndex=%v LastLogTerm=%v]",
		r.RequestID,
		r.Term,
		r.CandidateId,
		r.LastLogIndex,
		r.LastLogTerm)
}

type RequestVoteReply struct {
	PeerTerm    int
	VoteGranted bool
}

func (r RequestVoteReply) String() string {
	return fmt.Sprintf("[RequestVoteReply PeerTerm=%v VoteGranted=%v]",
		r.PeerTerm,
		r.VoteGranted)
}

type AppendEntriesArgs struct {
	RequestID         int
	LeaderTerm        int
	LeaderID          int
	PrevLogIndex      int
	PrevLogTerm       int
	LeaderCommitIndex int
	Entries           []LogEntry
}

func (a AppendEntriesArgs) String() string {
	return fmt.Sprintf("[AppendEntriesArgs RequestID=%v LeaderTerm=%v LeaderID=%v PrevLogIndex=%v PrevLogTerm=%v  LeaderCommitIndex=%v Entries=%v]",
		a.RequestID,
		a.LeaderTerm,
		a.LeaderID,
		a.PrevLogIndex,
		a.PrevLogTerm,
		a.LeaderCommitIndex,
		a.Entries)
}

type AppendEntriesReply struct {
	PeerTerm      int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

type InstallSnapshotArgs struct {
	RequestID                 int
	LeaderTerm                int
	LeaderID                  int
	SnapshotLastIncludedIndex int
	SnapshotLastIncludedTerm  int
	SnapshotData              []byte
}

func (i InstallSnapshotArgs) String() string {
	return fmt.Sprintf("[InstallSnapshotArgs RequestID=%v LeaderTerm=%v LeaderID=%v SnapshotLastIncludedIndex=%v SnapshotLastIncludedTerm=%v]",
		i.RequestID,
		i.LeaderTerm,
		i.LeaderID,
		i.SnapshotLastIncludedIndex,
		i.SnapshotLastIncludedTerm)
}

type InstallSnapshotReply struct {
	PeerTerm int
}

func (i InstallSnapshotReply) String() string {
	return fmt.Sprintf("[InstallSnapshotReply PeerTerm=%v]", i.PeerTerm)
}
