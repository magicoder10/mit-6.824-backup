package raft

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex *int
	LastLogTerm  *int
}

type RequestVoteReply struct {
	PeerTerm    int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	LeaderTerm        int
	LeaderId          int
	PrevLogIndex      *int
	PrevLogTerm       *int
	Entries           []LogEntry
	LeaderCommitIndex *int
}

type AppendEntriesReply struct {
	PeerTerm int
	Success  bool
}
