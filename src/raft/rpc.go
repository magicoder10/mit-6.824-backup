package raft

import (
	"fmt"
)

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (r RequestVoteArgs) String() string {
	return fmt.Sprintf("[RequestVoteArgs Term=%v CandidateId=%v LastLogIndex=%v LastLogTerm=%v]",
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
	LeaderTerm        int
	LeaderID          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int
}

func (a AppendEntriesArgs) String() string {
	return fmt.Sprintf("[AppendEntriesArgs LeaderTerm=%v LeaderID=%v PrevLogIndex=%v PrevLogTerm=%v Entries=%v LeaderCommitIndex=%v]",
		a.LeaderTerm,
		a.LeaderID,
		a.PrevLogIndex,
		a.PrevLogTerm,
		a.Entries,
		a.LeaderCommitIndex)
}

type AppendEntriesReply struct {
	PeerTerm     int
	Success      bool
	ConflictTerm int
}

type InstallSnapshotArgs struct {
	LeaderTerm                int
	LeaderID                  int
	SnapshotLastIncludedIndex int
	SnapshotLastIncludedTerm  int
	SnapshotData              []byte
}

func (i InstallSnapshotArgs) String() string {
	return fmt.Sprintf("[InstallSnapshotArgs LeaderTerm=%v LeaderID=%v SnapshotLastIncludedIndex=%v SnapshotLastIncludedTerm=%v]",
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
