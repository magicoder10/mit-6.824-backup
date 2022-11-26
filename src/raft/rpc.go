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
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int
}

func (a AppendEntriesArgs) String() string {
	return fmt.Sprintf("[AppendEntriesArgs LeaderTerm=%v LeaderId=%v PrevLogIndex=%v PrevLogTerm=%v Entries=%v LeaderCommitIndex=%v]",
		a.LeaderTerm,
		a.LeaderId,
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
