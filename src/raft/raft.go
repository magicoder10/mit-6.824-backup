package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

const heartbeatInterval = 120 * time.Millisecond
const minElectionTimeout = 500 * time.Millisecond
const maxElectionTimeoutOffsetRange = 150

const rpcBackupDeadline = 120 * time.Millisecond
const rpcBackupInterval = 100 * time.Millisecond
const rpcTimeout = 300 * time.Millisecond
const replicateLogRetryBackOff = 100 * time.Millisecond

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (a ApplyMsg) String() string {
	return fmt.Sprintf("[ApplyMsg CommandValid=%v Command=%v Index=%v SnapshotValid=%v SnapshotIndex=%v SnapshotTerm=%v]",
		a.CommandValid,
		a.Command,
		a.CommandIndex,
		a.SnapshotValid,
		a.SnapshotIndex,
		a.SnapshotTerm)
}

type LogEntry struct {
	Command            interface{}
	Index              int
	LeaderReceivedTerm int
}

func (l LogEntry) String() string {
	return fmt.Sprintf("[LogEntry Command=%v Index=%v LeaderReceivedTerm=%v]",
		l.Command,
		l.Index,
		l.LeaderReceivedTerm)
}

type Role string

const (
	FollowerRole  Role = "Follower"
	CandidateRole Role = "Candidate"
	LeaderRole    Role = "Leader"
)

type Raft struct {
	logger    Logger
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// state a Raft server must maintain.
	// Persistent on all servers
	currentTerm               int
	votedForCandidateID       *int
	snapshotLastIncludedIndex int
	snapshotLastIncludedTerm  int

	// Index starts from 1
	logEntries []LogEntry

	// Volatile on all servers
	role                      Role
	commitLogIndex            int
	lastAppliedLogIndex       int
	receivedMessageFromLeader bool
	grantedVoteToPeer         bool

	// Volatile state on leaders
	nextLogToSendIndices     []int
	lastReplicatedLogIndices []int
	replicateLogToPeerChs    []chan int

	commitLogCh        chan bool
	installingSnapshot bool
	nextRequestID      int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == LeaderRole
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("start taking snapshot: oldLastIncludedIndex=%v newLastIncludedTerm=%v committedLogIndex=%v",
		rf.snapshotLastIncludedIndex,
		index,
		rf.commitLogIndex))

	relativeIndex := rf.relativeIndex(index)
	if relativeIndex < 1 {
		rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("snapshot up to date, skipping: currentLastIncludedIndex=%v newLastIncludedTerm=%v",
			rf.snapshotLastIncludedIndex,
			index))
		return
	}

	rf.snapshotLastIncludedTerm = rf.logEntryTerm(index)
	rf.trimLogEntries(rf.relativeIndex(index))
	rf.snapshotLastIncludedIndex = index
	raftState, err := rf.encodeState()
	if err != nil {
		rf.logger.log(errorLogLevel, SnapshotFlow, fmt.Sprintf("encode state: err=%v", err))
		return
	}

	rf.persister.SaveStateAndSnapshot(raftState, snapshot)
	rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("finish taking snapshot: newLastIncludedTerm=%v lastIncludedTerm=%v",
		rf.snapshotLastIncludedIndex,
		index))
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("(%v)[RequestVote] receive request vote: args=%v",
		args.RequestID,
		args))

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.PeerTerm = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("(%v)[RequestVote] request outdated: snapshotLastTerm=%v snapshotLastIndex=%v currentTerm=%v peerTerm=%v",
			args.RequestID,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			rf.currentTerm,
			args.Term))
		return
	}

	if args.Term > rf.currentTerm {
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("(%v)[RequestVote] enter new term: snapshotLastTerm=%v snapshotLastIndex=%v currentTerm=%v peerTerm=%v",
			args.RequestID,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			rf.currentTerm,
			args.Term))
		rf.enterNewTerm(args.Term)
		rf.persist()
		reply.PeerTerm = rf.currentTerm

		if rf.role != FollowerRole {
			rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("(%v)[RequestVote] convert from %v to follower",
				args.RequestID,
				rf.role))
			rf.role = FollowerRole
			defer rf.runAsFollower()
		}
	}

	if rf.votedForCandidateID != nil {
		if *rf.votedForCandidateID == args.CandidateID {
			rf.grantedVoteToPeer = true
			reply.VoteGranted = true
			rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("(%v)[RequestVote] vote already granted to candidate: currentTerm=%v candidateID=%v",
				args.RequestID,
				rf.currentTerm,
				args.CandidateID))
			return
		}

		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("(%v)[RequestVote] already voted for another candidate: term=%v votedFor=%v candidateID=%v",
			args.RequestID,
			rf.currentTerm,
			intPtrToString(rf.votedForCandidateID),
			args.CandidateID))
		return
	}

	logLen := len(rf.logEntries)
	lastLogIndex := rf.absoluteIndex(logLen)
	lastLogTerm := rf.logEntryTerm(lastLogIndex)
	if lastLogTerm > args.LastLogTerm {
		rf.logger.log(errorLogLevel, LeaderElectionFlow, fmt.Sprintf("(%v)[RequestVote] candidate log is on old term: snapshotLastTerm=%v snapshotLastIndex=%v candidateID=%v candidateTerm=%v currentLastLogTerm=%v candidateLastLogTerm=%v",
			args.RequestID,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			args.CandidateID,
			args.Term,
			lastLogTerm,
			args.LastLogTerm))
		return
	}

	if lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex {
		rf.logger.log(errorLogLevel, LeaderElectionFlow, fmt.Sprintf("(%v)[RequestVote] candidate log is shorter: snapshotLastTerm=%v snapshotLastIndex=%v candidateID=%v candidateTerm=%v myLogLength=%v candidateLastLogIndex=%v",
			args.RequestID,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			args.CandidateID,
			args.Term,
			rf.absoluteIndex(logLen),
			args.LastLogIndex))
		return
	}

	rf.grantedVoteToPeer = true
	reply.VoteGranted = true
	rf.votedForCandidateID = &args.CandidateID
	rf.persist()
	rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("(%v)[RequestVote] granted vote to candidate: term=%v candidateID=%v myLastLogTerm=%v myLastLogIndex=%v peerLastLogTerm=%v peerLastLogIndex=%v",
		args.RequestID,
		rf.currentTerm,
		args.CandidateID,
		lastLogTerm,
		lastLogIndex,
		args.LastLogTerm,
		args.LastLogIndex))
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[AppendEntries] receive append entries: args=%v",
		args.RequestID,
		args))

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.PeerTerm = rf.currentTerm
	if args.LeaderTerm < rf.currentTerm {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[AppendEntries] outdated append entries: snapshotLastTerm=%v snapshotLastIndex=%v leaderTerm=%v currentTerm=%v",
			args.RequestID,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			args.LeaderTerm,
			rf.currentTerm))
		return
	}

	if args.LeaderTerm > rf.currentTerm {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[AppendEntries] update term: snapshotLastTerm=%v snapshotLastIndex=%v leaderTerm=%v currentTerm=%v",
			args.RequestID,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			args.LeaderTerm,
			rf.currentTerm))
		rf.enterNewTerm(args.LeaderTerm)
		rf.persist()
		reply.PeerTerm = rf.currentTerm

		if rf.role != FollowerRole {
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[AppendEntries] convert from %v to follower",
				args.RequestID,
				rf.role))
			rf.role = FollowerRole
			defer rf.runAsFollower()
		}
	} else if rf.role == CandidateRole {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[AppendEntries] convert from %v to follower",
			args.RequestID,
			rf.role))
		rf.role = FollowerRole
		defer rf.runAsFollower()
	}

	rf.receivedMessageFromLeader = true

	lastLogIndex := rf.absoluteIndex(len(rf.logEntries))
	if lastLogIndex < args.PrevLogIndex {
		rf.logger.log(errorLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[AppendEntries] prevLogIndex exceeds last log entry: snapshotLastTerm=%v snapshotLastIndex=%v leaderPrevLogIndex=%v",
			args.RequestID,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			args.PrevLogIndex))
		rf.logger.log(debugLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[AppendEntries] logEntries=%v",
			args.RequestID,
			rf.logEntries))

		reply.ConflictIndex = rf.absoluteIndex(len(rf.logEntries))
		return
	}

	if rf.snapshotLastIncludedIndex <= args.PrevLogIndex {
		prevLogTerm := rf.logEntryTerm(args.PrevLogIndex)
		if prevLogTerm != args.PrevLogTerm {
			rf.logger.log(errorLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[AppendEntries] previous log entry term mismatch: snapshotLastTerm=%v snapshotLastIndex=%v prevLogIndex=%v prevLogTerm=%v expectedPrevLogTerm=%v",
				args.RequestID,
				rf.snapshotLastIncludedTerm,
				rf.snapshotLastIncludedIndex,
				args.PrevLogIndex,
				prevLogTerm,
				args.PrevLogTerm))
			rf.logger.log(debugLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[AppendEntries] logEntries=%v",
				args.RequestID,
				rf.logEntries))
			reply.ConflictTerm = prevLogTerm
			reply.ConflictIndex = rf.findIndexOfFirstLogEntry(prevLogTerm)
			return
		}
	}

	rf.logger.log(debugLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[AppendEntries] current log entries: snapshotLastTerm=%v snapshotLastIndex=%v logEntries=%v",
		args.RequestID,
		rf.snapshotLastIncludedTerm,
		rf.snapshotLastIncludedIndex,
		rf.logEntries))
	newEntriesStartIndex := 0
	updated := false

	for ; newEntriesStartIndex < len(args.Entries); newEntriesStartIndex++ {
		newEntry := args.Entries[newEntriesStartIndex]
		if newEntry.Index <= rf.snapshotLastIncludedIndex {
			continue
		}

		relativeLogIndex := rf.relativeIndex(newEntry.Index)
		if len(rf.logEntries) < relativeLogIndex {
			break
		}

		oldEntry := rf.logEntries[relativeLogIndex-1]
		if oldEntry.LeaderReceivedTerm != newEntry.LeaderReceivedTerm {
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[AppendEntries] removing conflicting entries: snapshotLastTerm=%v snapshotLastIndex=%v newEntryStartIndex=%v oldEntry=%v newEntry=%v",
				args.RequestID,
				rf.snapshotLastIncludedTerm,
				rf.snapshotLastIncludedIndex,
				newEntriesStartIndex,
				oldEntry,
				newEntry))
			rf.logEntries = rf.logEntries[:relativeLogIndex-1]
			rf.logger.log(debugLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[AppendEntries] logEntries=%v",
				args.RequestID,
				rf.logEntries))
			rf.persist()
			updated = true
			break
		}
	}

	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[AppendEntries] add newEntries if not exist: snapshotLastTerm=%v snapshotLastIndex=%v newEntryStartIndex=%v",
		args.RequestID,
		rf.snapshotLastIncludedTerm,
		rf.snapshotLastIncludedIndex,
		newEntriesStartIndex))
	if newEntriesStartIndex < len(args.Entries) {
		rf.logEntries = append(rf.logEntries, args.Entries[newEntriesStartIndex:]...)
		rf.persist()
		updated = true
	}

	if updated {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[AppendEntries] updated log entries: snapshotLastTerm=%v snapshotLastIndex=%v",
			args.RequestID,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex))
	} else {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[AppendEntries] no change to log entries: snapshotLastTerm=%v snapshotLastIndex=%v",
			args.RequestID,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex))
	}

	rf.logger.log(debugLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[AppendEntries] logEntries=%v",
		args.RequestID,
		rf.logEntries))

	if args.LeaderCommitIndex > rf.commitLogIndex {
		lastIndex := args.PrevLogIndex
		if len(args.Entries) > 0 {
			lastIndex = args.Entries[len(args.Entries)-1].Index
		}

		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[AppendEntries] before commit entries: snapshotLastTerm=%v snapshotLastIndex=%v commitLogIndex=%v LeaderCommitIndex=%v lastIndex=%v",
			args.RequestID,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			rf.commitLogIndex,
			args.LeaderCommitIndex,
			lastIndex))
		rf.commitLogIndex = min(args.LeaderCommitIndex, lastIndex)
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[AppendEntries] commit entries: snapshotLastTerm=%v snapshotLastIndex=%v commitLogIndex=%v",
			args.RequestID,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			rf.commitLogIndex))
		commitLogCh := rf.commitLogCh
		go func() {
			commitLogCh <- true
		}()
	}

	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("(%v)[InstallSnapshot] receive install snapshot: args=%v",
		args.RequestID,
		args))
	rf.mu.Lock()
	reply.PeerTerm = rf.currentTerm
	if args.LeaderTerm < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.LeaderTerm > rf.currentTerm {
		rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("(%v)[InstallSnapshot] update term: snapshotLastTerm=%v snapshotLastIndex=%v leaderTerm=%v currentTerm=%v",
			args.RequestID,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			args.LeaderTerm,
			rf.currentTerm))
		rf.enterNewTerm(args.LeaderTerm)
		rf.persist()
		reply.PeerTerm = rf.currentTerm

		if rf.role != FollowerRole {
			rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("(%v)[InstallSnapshot] convert from %v to follower",
				args.RequestID,
				rf.role))
			rf.role = FollowerRole
			defer rf.runAsFollower()
		}
	} else if rf.role == CandidateRole {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[InstallSnapshot] convert from %v to follower",
			args.RequestID,
			rf.role))
		rf.role = FollowerRole
		defer rf.runAsFollower()
	}

	rf.receivedMessageFromLeader = true
	if rf.installingSnapshot {
		rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("(%v)[InstallSnapshot] Installing snapshot, skipping install snapshot request: lastIncludedIndex=%v lastIncludedTerm=%v",
			args.RequestID,
			args.SnapshotLastIncludedIndex,
			args.SnapshotLastIncludedTerm))
		rf.mu.Unlock()
		return
	}

	relativeSnapshotLastIncludedIndex := rf.relativeIndex(args.SnapshotLastIncludedIndex)
	if relativeSnapshotLastIncludedIndex < 1 {
		rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("(%v)[InstallSnapshot] snapshot up to date, skipping: currentLastIncludedIndex=%v newLastIncludedTerm=%v",
			args.RequestID,
			rf.snapshotLastIncludedIndex,
			args.SnapshotLastIncludedIndex))
		rf.mu.Unlock()
		return
	}

	rf.installingSnapshot = true
	commitLogCh := rf.commitLogCh
	rf.mu.Unlock()

	rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("(%v)[InstallSnapshot] finish applying the last command if there is one",
		args.RequestID))
	commitLogCh <- true

	rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("(%v)[InstallSnapshot] installing snapshot: currentLastIncludedIndex=%v newLastIncludedTerm=%v",
		args.RequestID,
		args.SnapshotLastIncludedIndex,
		args.SnapshotLastIncludedTerm))

	rf.mu.Lock()
	relativeSnapshotLastIncludedIndex = rf.relativeIndex(args.SnapshotLastIncludedIndex)
	if relativeSnapshotLastIncludedIndex < 1 {
		rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("(%v)[InstallSnapshot] snapshot up to date, skipping: currentLastIncludedIndex=%v newLastIncludedTerm=%v",
			args.RequestID,
			rf.snapshotLastIncludedIndex,
			args.SnapshotLastIncludedIndex))
		rf.installingSnapshot = false
		rf.mu.Unlock()
		return
	}

	if relativeSnapshotLastIncludedIndex >= len(rf.logEntries) ||
		rf.logEntries[relativeSnapshotLastIncludedIndex-1].LeaderReceivedTerm != args.SnapshotLastIncludedTerm {
		rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("(%v)[InstallSnapshot] clearing log entries: currentLastIncludedIndex=%v newLastIncludedTerm=%v",
			args.RequestID,
			args.SnapshotLastIncludedIndex,
			args.SnapshotLastIncludedTerm))
		rf.logger.log(debugLogLevel, SnapshotFlow, fmt.Sprintf("(%v)[InstallSnapshot] clearing log entries: logEntries=%v",
			args.RequestID,
			rf.logEntries))
		rf.logEntries = make([]LogEntry, 0)
	} else {
		rf.trimLogEntries(relativeSnapshotLastIncludedIndex)
	}

	rf.snapshotLastIncludedIndex = args.SnapshotLastIncludedIndex
	rf.snapshotLastIncludedTerm = args.SnapshotLastIncludedTerm
	raftState, err := rf.encodeState()
	if err != nil {
		rf.logger.log(errorLogLevel, SnapshotFlow, fmt.Sprintf("(%v)[InstallSnapshot] encode state: err=%v",
			args.RequestID,
			err))
		rf.installingSnapshot = false
		rf.mu.Unlock()
		return
	}

	rf.persister.SaveStateAndSnapshot(raftState, args.SnapshotData)
	rf.commitLogIndex = max(rf.commitLogIndex, args.SnapshotLastIncludedIndex)
	rf.lastAppliedLogIndex = args.SnapshotLastIncludedIndex
	rf.mu.Unlock()

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.SnapshotData,
		SnapshotIndex: args.SnapshotLastIncludedIndex,
		SnapshotTerm:  args.SnapshotLastIncludedTerm,
	}
	rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("(%v)[InstallSnapshot] applying snapshot: applyMsg=%v",
		args.RequestID,
		applyMsg))
	rf.applyCh <- applyMsg

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.installingSnapshot = false
	rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("(%v)[InstallSnapshot] installed snapshot: lastIncludedIndex=%v lastIncludedTerm=%v",
		args.RequestID,
		args.SnapshotLastIncludedIndex,
		args.SnapshotLastIncludedTerm))
	go func() {
		commitLogCh <- true
	}()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()

	defer rf.mu.Unlock()
	if rf.killed() {
		return 0, rf.currentTerm, rf.role == LeaderRole
	}

	if rf.role != LeaderRole {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[Start] only leader can receive client command: command=%v",
			command))
		return 0, rf.currentTerm, false
	}

	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[Start] received command from client: command=%v",
		command))
	nextLogIndex := rf.absoluteIndex(len(rf.logEntries) + 1)
	logEntry := LogEntry{
		Command:            command,
		Index:              nextLogIndex,
		LeaderReceivedTerm: rf.currentTerm,
	}
	rf.logEntries = append(rf.logEntries, logEntry)
	rf.persist()
	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[Start] append entry to leader log: snapshotLastTerm=%v snapshotLastIndex=%v logEntry=%v",
		rf.snapshotLastIncludedTerm,
		rf.snapshotLastIncludedIndex,
		logEntry))

	rf.replicateLogToPeers()
	return nextLogIndex, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(
	peers []*labrpc.ClientEnd,
	me int,
	persister *Persister,
	applyCh chan ApplyMsg,
) *Raft {
	logger := NewLogger(offLogLevel, map[Flow]bool{
		FollowerFlow:       true,
		CandidateFlow:      true,
		LeaderFlow:         true,
		LeaderElectionFlow: false,
		LogReplicationFlow: true,
		PersistenceFlow:    false,
		SnapshotFlow:       true,
		RPCFlow:            true,
	}, me)
	rf := &Raft{
		logger:                   logger,
		peers:                    peers,
		persister:                persister,
		applyCh:                  applyCh,
		me:                       me,
		role:                     FollowerRole,
		currentTerm:              0,
		logEntries:               make([]LogEntry, 0),
		commitLogIndex:           0,
		lastAppliedLogIndex:      0,
		nextLogToSendIndices:     make([]int, len(peers)),
		lastReplicatedLogIndices: make([]int, len(peers)),
		replicateLogToPeerChs:    make([]chan int, len(peers)),
		commitLogCh:              make(chan bool),
	}

	rf.logger.log(infoLogLevel, PersistenceFlow, "[Make] loading state")
	rf.readPersist(persister.ReadRaftState())
	rf.logger.log(infoLogLevel, FollowerFlow, "[Make] start as follower")
	go func() {
		rf.runAsFollower()
	}()

	lastIncludedIndex := rf.snapshotLastIncludedIndex
	lastIncludedTerm := rf.snapshotLastIncludedTerm
	go func() {
		if lastIncludedIndex > 0 {
			snapshotData := rf.persister.ReadSnapshot()

			rf.mu.Lock()
			rf.commitLogIndex = lastIncludedIndex
			rf.lastAppliedLogIndex = lastIncludedIndex
			rf.mu.Unlock()

			applyMsg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapshotData,
				SnapshotIndex: lastIncludedIndex,
				SnapshotTerm:  lastIncludedTerm,
			}
			rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("[Make] applying snapshot: applyMsg=%v", applyMsg))
			rf.applyCh <- applyMsg
		}
		rf.applyCommittedEntries()
	}()
	rf.replicateLogInBackground()
	return rf
}

func (rf *Raft) runAsLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.log(infoLogLevel, LeaderFlow, fmt.Sprintf("[runAsLeader] running as leader: term=%v", rf.currentTerm))

	numPeers := len(rf.peers)
	for PeerID := 0; PeerID < numPeers; PeerID++ {
		rf.nextLogToSendIndices[PeerID] = rf.absoluteIndex(len(rf.logEntries) + 1)
	}

	rf.lastReplicatedLogIndices = make([]int, numPeers)

	go func() {
		rf.sendPeriodicHeartbeat()
	}()
}

func (rf *Raft) runAsCandidate() {
	rf.logger.log(infoLogLevel, CandidateFlow, "[runAsCandidate] running as candidate in background")
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			if rf.role != CandidateRole {
				rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[runAsCandidate] not candidate any more, exit election: role=%v", rf.role))
				rf.mu.Unlock()
				return
			}

			if !rf.grantedVoteToPeer {
				rf.currentTerm++
				rf.votedForCandidateID = &rf.me
				rf.persist()
				currentTerm := rf.currentTerm
				rf.mu.Unlock()

				rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[runAsCandidate] start election: newTerm=%v", currentTerm))
				halfVotes := len(rf.peers) / 2
				go func(electionTerm int) {
					rf.logger.log(infoLogLevel, LeaderElectionFlow, "[runAsCandidate] request votes from peers")
					grantedVotes := rf.requestVoteFromPeers() + 1
					rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[runAsCandidate] received votes from peers: grantedVotes=%v, requiredVotes=%v",
						grantedVotes,
						halfVotes+1))

					rf.mu.Lock()
					rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[runAsCandidate] electionTerm=%v, currentTerm=%v",
						electionTerm,
						rf.currentTerm))
					if electionTerm < rf.currentTerm {
						// new election started after election timeout
						rf.logger.log(infoLogLevel, LeaderElectionFlow, "[runAsCandidate] votes outdated")
						rf.mu.Unlock()
						return
					}

					if rf.role != CandidateRole {
						// received heartbeat from new leader & fallback to follower
						rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[runAsCandidate] not candidate, abandoned vote result: role=%s", rf.role))
						rf.mu.Unlock()
						return
					}

					if grantedVotes > halfVotes {
						rf.logger.log(infoLogLevel, LeaderElectionFlow, "[runAsCandidate] received enough votes, elected as leader")
						rf.role = LeaderRole
						rf.mu.Unlock()
						rf.runAsLeader()
						return
					}

					if grantedVotes < halfVotes {
						rf.logger.log(infoLogLevel, LeaderElectionFlow, "[runAsCandidate] not enough votes, fallback to follower")
						rf.role = FollowerRole
						rf.mu.Unlock()
						rf.runAsFollower()
						return
					}

					rf.mu.Unlock()
					rf.logger.log(infoLogLevel, LeaderElectionFlow, "[runAsCandidate] split vote, no leader")
				}(currentTerm)
			} else {
				rf.mu.Unlock()
			}

			rf.logger.log(infoLogLevel, LeaderElectionFlow, "[runAsCandidate] wait for election timeout")
			rf.waitForElectionTimeout()

			rf.mu.Lock()
			rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[runAsCandidate] candidate election timeout: role=%v", rf.role))
			rf.mu.Unlock()
		}

		if rf.killed() {
			rf.logger.log(infoLogLevel, CandidateFlow, "[runAsCandidate] candidate is killed")
		}
	}()
}

func (rf *Raft) runAsFollower() {
	rf.logger.log(infoLogLevel, FollowerFlow, "[runAsFollower] running as follower in background")
	go func() {
		for !rf.killed() {
			rf.logger.log(infoLogLevel, FollowerFlow, "[runAsFollower] wait for election timeout")
			rf.waitForElectionTimeout()

			rf.mu.Lock()
			rf.logger.log(infoLogLevel, FollowerFlow, fmt.Sprintf("[runAsFollower] follower election timeout: role=%v", rf.role))
			if rf.role != FollowerRole {
				rf.mu.Unlock()
				rf.logger.log(infoLogLevel, FollowerFlow, "[runAsFollower] exit follower flow")
				return
			}

			if rf.receivedMessageFromLeader || rf.grantedVoteToPeer {
				rf.logger.log(infoLogLevel, FollowerFlow, "[runAsFollower] received message from peer or granted vote to peer")
				rf.receivedMessageFromLeader = false
				rf.grantedVoteToPeer = false
				rf.mu.Unlock()
				continue
			}

			rf.role = CandidateRole
			rf.mu.Unlock()
			rf.runAsCandidate()
			return
		}

		if rf.killed() {
			rf.logger.log(infoLogLevel, FollowerFlow, "[runAsFollower] follower is killed")
		}
	}()
}

func (rf *Raft) replicateLogInBackground() {
	replicateLogToPeerChs := rf.replicateLogToPeerChs
	for PeerID := 0; PeerID < len(rf.peers); PeerID++ {
		if PeerID == rf.me {
			continue
		}

		rf.replicateLogToPeerChs[PeerID] = make(chan int)
		go func(PeerID int) {
			for replicationTerm := range replicateLogToPeerChs[PeerID] {
				if rf.killed() {
					return
				}

				rf.mu.Lock()
				if rf.role != LeaderRole {
					rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogInBackground] not leader, skipping log replication: role=%v", rf.role))
					rf.mu.Unlock()
					continue
				}

				if replicationTerm != rf.currentTerm {
					rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogInBackground] log replication outdated: currentTerm=%v replicationTerm=%v", rf.currentTerm, replicationTerm))
					rf.mu.Unlock()
					continue
				}

				rf.mu.Unlock()
				rf.replicateLogToPeer(PeerID, replicationTerm)
			}
		}(PeerID)
	}
}

func (rf *Raft) replicateLogToPeers() {
	currentTerm := rf.currentTerm
	for PeerID := 0; PeerID < len(rf.peers); PeerID++ {
		if PeerID == rf.me {
			continue
		}

		replicateLogToPeerCh := rf.replicateLogToPeerChs[PeerID]
		go func() {
			replicateLogToPeerCh <- currentTerm
		}()
	}
}

func (rf *Raft) replicateLogToPeer(PeerID int, replicationTerm int) {
	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogToPeer] start log replication for peer: PeerID=%v replicationTerm=%v", PeerID, replicationTerm))

	for !rf.killed() {
		rf.mu.Lock()

		if rf.role != LeaderRole {
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogToPeer] server is not leader anymore: PeerID=%v", PeerID))
			rf.mu.Unlock()
			return
		}

		if replicationTerm != rf.currentTerm {
			rf.logger.log(debugLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogToPeer] log replication outdated: PeerID=%v replicationTerm=%v currentTerm=%v", PeerID, replicationTerm, rf.currentTerm))
			rf.mu.Unlock()
			return
		}

		lastLogIndex := rf.absoluteIndex(len(rf.logEntries))
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogToPeer] try replicate log to peer: PeerID=%v replicationTerm=%v lastLogIndex=%v", PeerID, replicationTerm, lastLogIndex))

		nextLogIndex := rf.nextLogToSendIndices[PeerID]
		if nextLogIndex > lastLogIndex {
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogToPeer] log is up to date: PeerID=%v replicationTerm=%v nextLogIndex=%v lastLogIndex=%v", PeerID, replicationTerm, nextLogIndex, lastLogIndex))
			rf.mu.Unlock()
			return
		}

		if nextLogIndex <= rf.snapshotLastIncludedIndex {
			args := InstallSnapshotArgs{
				LeaderTerm:                rf.currentTerm,
				LeaderID:                  rf.me,
				SnapshotLastIncludedIndex: rf.snapshotLastIncludedIndex,
				SnapshotLastIncludedTerm:  rf.snapshotLastIncludedTerm,
				SnapshotData:              rf.persister.ReadSnapshot(),
			}

			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogToPeer] start installing snapshot to peer: PeerID=%v snapshotLastTerm=%v snapshotLastIndex=%v",
				PeerID,
				rf.snapshotLastIncludedTerm,
				rf.snapshotLastIncludedIndex))
			rf.mu.Unlock()

			reply := InstallSnapshotReply{}

			requestSucceed := rf.sendInstallSnapshot(PeerID, &args, &reply)
			if !requestSucceed {
				// The peer is likely disconnected, retry later with the latest snapshot
				rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogToPeer] fail to replicate log entries to peer(RPC): PeerID=%v", PeerID))
				time.Sleep(replicateLogRetryBackOff)
				continue
			}

			rf.mu.Lock()
			if rf.role != LeaderRole {
				rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogToPeer] server is not leader anymore: PeerID=%v", PeerID))
				rf.mu.Unlock()
				return
			}

			if replicationTerm != rf.currentTerm {
				rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogToPeer] log replication outdated: PeerID=%v replicationTerm=%v currentTerm=%v", PeerID, replicationTerm, rf.currentTerm))
				rf.mu.Unlock()
				return
			}

			rf.nextLogToSendIndices[PeerID] = args.SnapshotLastIncludedIndex + 1
			rf.lastReplicatedLogIndices[PeerID] = args.SnapshotLastIncludedIndex
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogToPeer] installed snapshot to peer: PeerID=%v snapshotLastTerm=%v snapshotLastIndex=%v nextIndex=%v replicatedIndex=%v",
				PeerID,
				rf.snapshotLastIncludedTerm,
				rf.snapshotLastIncludedIndex,
				rf.nextLogToSendIndices[PeerID],
				rf.lastReplicatedLogIndices[PeerID],
			))
			rf.mu.Unlock()

			rf.updateCommitIndex()
			continue
		}

		logEntries := rf.logEntries[rf.relativeIndex(nextLogIndex)-1 : len(rf.logEntries)]
		entriesCopy := make([]LogEntry, len(logEntries))
		copy(entriesCopy, logEntries)

		prevLogIndex := nextLogIndex - 1
		prevLogTerm := rf.logEntryTerm(prevLogIndex)
		args := AppendEntriesArgs{
			LeaderTerm:        rf.currentTerm,
			LeaderID:          rf.me,
			PrevLogIndex:      prevLogIndex,
			PrevLogTerm:       prevLogTerm,
			Entries:           entriesCopy,
			LeaderCommitIndex: rf.commitLogIndex,
		}

		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogToPeer] replicating log entries to peer: PeerID=%v snapshotLastTerm=%v snapshotLastIndex=%v args=%v",
			PeerID,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			args))
		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		requestSucceed := rf.sendAppendEntries(LogReplicationFlow, PeerID, true, &args, &reply)
		if !requestSucceed {
			// The peer is likely disconnected, retry later with the latest log
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogToPeer] fail to replicate log entries to peer(RPC): PeerID=%v", PeerID))
			time.Sleep(replicateLogRetryBackOff)
			continue
		}

		if rf.killed() {
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogToPeer] leader is killed: PeerID=%v", PeerID))
			return
		}

		rf.mu.Lock()
		if rf.role != LeaderRole {
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogToPeer] server is not leader anymore: PeerID=%v", PeerID))
			rf.mu.Unlock()
			return
		}

		if replicationTerm != rf.currentTerm {
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogToPeer] log replication outdated: PeerID=%v replicationTerm=%v currentTerm=%v", PeerID, replicationTerm, rf.currentTerm))
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			rf.nextLogToSendIndices[PeerID] = lastLogIndex + 1
			rf.lastReplicatedLogIndices[PeerID] = lastLogIndex
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogToPeer] replicated log entries to peer: PeerID=%v lastReplicatedLogIndex=%v",
				PeerID,
				rf.lastReplicatedLogIndices[PeerID]))
			rf.mu.Unlock()

			rf.updateCommitIndex()
			return
		}

		rf.logger.log(errorLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogToPeer] fail to replicate log entries to peer: PeerID=%v snapshotLastTerm=%v snapshotLastIndex=%v nextLogIndex=%v conflictTerm=%v conflictIndex=%v",
			PeerID,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			nextLogIndex,
			reply.ConflictTerm,
			reply.ConflictIndex))
		rf.nextLogToSendIndices[PeerID] = rf.getNextLogIndex(reply.ConflictTerm, reply.ConflictIndex)
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogToPeer] update nextIndex: PeerID=%v snapshotLastTerm=%v snapshotLastIndex=%v conflictTerm=%v conflictIndex=%v nextIndex=%v",
			PeerID,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			reply.ConflictTerm,
			reply.ConflictIndex,
			rf.nextLogToSendIndices[PeerID],
		))
		rf.mu.Unlock()
	}

	if rf.killed() {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogToPeer] leader is killed: PeerID=%v", PeerID))
	}

	return
}

func (rf *Raft) sendPeriodicHeartbeat() {
	rf.mu.Lock()
	heartbeatTerm := rf.currentTerm
	rf.mu.Unlock()

	for PeerID := 0; PeerID < len(rf.peers) && !rf.killed(); PeerID++ {
		if PeerID == rf.me {
			continue
		}

		go func(PeerID int) {
			for !rf.killed() {
				rf.mu.Lock()
				if rf.role != LeaderRole {
					rf.mu.Unlock()
					rf.logger.log(errorLogLevel, LeaderFlow, fmt.Sprintf("[sendPeriodicHeartbeat] not leader anymore, stop sending heartbeat: heartbeatTerm=%v ", heartbeatTerm))
					return
				}

				if rf.currentTerm != heartbeatTerm {
					rf.logger.log(errorLogLevel, LeaderFlow, fmt.Sprintf("[sendPeriodicHeartbeat] heartbeat outdated: heartbeatTerm=%v currentTerm=%v", heartbeatTerm, rf.currentTerm))
					rf.mu.Unlock()
					return
				}

				prevLogIndex := rf.absoluteIndex(len(rf.logEntries))
				args := AppendEntriesArgs{
					LeaderTerm:        heartbeatTerm,
					LeaderID:          rf.me,
					PrevLogIndex:      prevLogIndex,
					PrevLogTerm:       rf.logEntryTerm(prevLogIndex),
					Entries:           []LogEntry{},
					LeaderCommitIndex: rf.commitLogIndex,
				}
				rf.logger.log(infoLogLevel, LeaderFlow, fmt.Sprintf("[sendPeriodicHeartbeat] send heartbeat to peer: PeerID=%v heartbeatTerm=%v snapshotLastTerm=%v snapshotLastIndex=%v args=%v",
					PeerID,
					heartbeatTerm,
					rf.snapshotLastIncludedTerm,
					rf.snapshotLastIncludedIndex,
					args))
				rf.mu.Unlock()
				go func() {
					var reply AppendEntriesReply
					rf.sendAppendEntries(LeaderElectionFlow, PeerID, false, &args, &reply)
				}()
				time.Sleep(heartbeatInterval)
			}

			if rf.killed() {
				rf.logger.log(infoLogLevel, LeaderFlow, "[sendPeriodicHeartbeat] leader is killed")
			}
		}(PeerID)
	}

	if rf.killed() {
		rf.logger.log(infoLogLevel, LeaderFlow, "[sendPeriodicHeartbeat] leader is killed")
	}
}

func (rf *Raft) updateCommitIndex() {
	requiredPeers := len(rf.peers) / 2
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for index := rf.absoluteIndex(len(rf.logEntries)); index > rf.snapshotLastIncludedIndex && index > rf.commitLogIndex; index-- {
		replicatedPeers := 0
		for PeerID := 0; PeerID < len(rf.peers); PeerID++ {
			if PeerID == rf.me {
				continue
			}

			if rf.lastReplicatedLogIndices[PeerID] >= index {
				replicatedPeers++
			}

			if replicatedPeers < requiredPeers {
				continue
			}

			if rf.logEntries[rf.relativeIndex(index)-1].LeaderReceivedTerm != rf.currentTerm {
				continue
			}

			oldCommitLogIndex := rf.commitLogIndex
			rf.commitLogIndex = index
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[updateCommitIndex] commit log entries: snapshotLastTerm=%v snapshotLastIndex=%v oldCommitLogIndex=%v newCommitLogIndex=%v",
				rf.snapshotLastIncludedTerm,
				rf.snapshotLastIncludedIndex,
				oldCommitLogIndex,
				rf.commitLogIndex))
			commitLogCh := rf.commitLogCh
			go func() {
				commitLogCh <- true
			}()

			return
		}
	}
}

func (rf *Raft) requestVoteFromPeers() int {
	rf.mu.Lock()
	requestedTerm := rf.currentTerm
	lastLogIndex := rf.absoluteIndex(len(rf.logEntries))
	lastLogTerm := rf.logEntryTerm(lastLogIndex)
	rf.mu.Unlock()

	receivedVotes := make([]bool, len(rf.peers))
	votesGranted := 0
	totalVotesReceived := 0
	waitForVote := true
	var voteMut sync.Mutex
	voteCond := sync.NewCond(&voteMut)

	for PeerID := 0; PeerID < len(rf.peers); PeerID++ {
		if PeerID == rf.me {
			continue
		}

		go func(PeerID int) {
			for {
				rf.mu.Lock()
				if rf.role != CandidateRole {
					rf.mu.Unlock()
					rf.logger.log(errorLogLevel, LeaderElectionFlow, fmt.Sprintf("[requestVoteFromPeers] not candidate, stop requesting vote: PeerID=%v requestedTerm=%v", PeerID, requestedTerm))

					voteMut.Lock()
					waitForVote = false
					voteMut.Unlock()
					voteCond.Signal()
					return
				}

				if rf.currentTerm != requestedTerm {
					rf.mu.Unlock()
					rf.logger.log(errorLogLevel, LeaderElectionFlow, fmt.Sprintf("[requestVoteFromPeers] request vote outdated: PeerID=%v requestedTerm=%v ", PeerID, requestedTerm))

					voteMut.Lock()
					waitForVote = false
					voteMut.Unlock()
					voteCond.Signal()
					return
				}

				args := RequestVoteArgs{
					Term:         requestedTerm,
					CandidateID:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				rf.mu.Unlock()

				rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[requestVoteFromPeers] request vote from peer: PeerID=%v requestedTerm=%v", PeerID, requestedTerm))
				var reply RequestVoteReply

				succeed := rf.sendRequestVote(PeerID, &args, &reply)

				voteMut.Lock()
				if !succeed {
					totalVotesReceived++
					voteMut.Unlock()
					return
				}

				if receivedVotes[PeerID] {
					voteMut.Unlock()
					return
				}

				receivedVotes[PeerID] = true
				if reply.PeerTerm > requestedTerm {
					rf.logger.log(errorLogLevel, LeaderElectionFlow, fmt.Sprintf("[requestVoteFromPeers] request vote outdated: PeerID=%v requestedTerm=%v", PeerID, requestedTerm))
					waitForVote = false
					voteMut.Unlock()
					voteCond.Signal()
					return
				}

				rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[requestVoteFromPeers] received vote from peer: PeerID=%v reply=%v", PeerID, reply))
				if reply.VoteGranted {
					rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[requestVoteFromPeers] granted vote from peer: PeerID=%v requestedTerm=%v votesGranted=%v totalVotesReceived=%v",
						PeerID,
						requestedTerm,
						votesGranted,
						totalVotesReceived))
					votesGranted++
				}

				voteMut.Unlock()
				voteCond.Signal()
				return
			}
		}(PeerID)
	}

	totalVotesRequested := len(rf.peers) - 1
	requiredVoteGrants := totalVotesRequested / 2

	voteMut.Lock()
	defer voteMut.Unlock()
	for waitForVote && votesGranted < requiredVoteGrants && totalVotesReceived < totalVotesRequested {
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[requestVoteFromPeers] waiting for new incoming vote: requestedTerm=%v votesGranted=%v requiredVoteGrants=%v totalVotesReceived=%v",
			requestedTerm,
			votesGranted,
			requiredVoteGrants,
			totalVotesReceived))
		voteCond.Wait()
	}

	rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[requestVoteFromPeers] exit requesting votes: requestedTerm=%v waitForVote=%v votesGranted=%v totalVotesReceived=%v totalVotesRequested=%v",
		requestedTerm,
		waitForVote,
		votesGranted,
		totalVotesReceived,
		totalVotesRequested))
	return votesGranted
}

func (rf *Raft) waitForElectionTimeout() {
	randTimeout := time.Duration(rand.Intn(maxElectionTimeoutOffsetRange)) * time.Millisecond
	electionTimeout := randTimeout + minElectionTimeout
	time.Sleep(electionTimeout)
}

func (rf *Raft) applyCommittedEntries() {
	rf.mu.Lock()
	commitLogCh := rf.commitLogCh
	rf.mu.Unlock()

	for range commitLogCh {
		for {
			rf.mu.Lock()
			if rf.installingSnapshot {
				rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[applyCommittedEntries] installing snapshot, skip applying commits: snapshotLastTerm=%v snapshotLastIndex=%v newCommitLogIndex=%v",
					rf.snapshotLastIncludedTerm,
					rf.snapshotLastIncludedIndex,
					rf.commitLogIndex))
				rf.mu.Unlock()
				break
			}

			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[applyCommittedEntries] processing commits: commitLogIndex=%v lastAppliedLogIndex=%v", rf.commitLogIndex, rf.lastAppliedLogIndex))
			if rf.commitLogIndex <= rf.lastAppliedLogIndex {
				rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[applyCommittedEntries] all commits are applied: commitLogIndex=%v lastAppliedLogIndex=%v", rf.commitLogIndex, rf.lastAppliedLogIndex))
				rf.mu.Unlock()
				break
			}

			relativeIndex := rf.relativeIndex(rf.lastAppliedLogIndex + 1)
			if relativeIndex < 1 {
				rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[applyCommittedEntries] log entry is inside snapshot, skipping: lastAppliedLogIndex=%v relativeIndex=%v", rf.lastAppliedLogIndex, relativeIndex))
				rf.mu.Unlock()
				break
			}

			rf.lastAppliedLogIndex++
			logEntry := rf.logEntries[relativeIndex-1]
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[applyCommittedEntries] applying commit: commitLogIndex=%v lastAppliedLogIndex=%v relativeIndex=%v", rf.commitLogIndex, rf.lastAppliedLogIndex, relativeIndex))

			commitLogIndex := rf.commitLogIndex
			lastAppliedLogIndex := rf.lastAppliedLogIndex
			rf.mu.Unlock()

			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Command,
				CommandIndex: logEntry.Index,
			}

			rf.applyCh <- applyMsg
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[applyCommittedEntries] applied commit: commitLogIndex=%v lastAppliedLogIndex=%v", commitLogIndex, lastAppliedLogIndex))
		}
	}
}

func (rf *Raft) enterNewTerm(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedForCandidateID = nil
	rf.persist()
}

// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	requestID := rf.nextRequestID
	rf.nextRequestID++
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	args.RequestID = requestID
	rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("(%v)[sendRequestVote] send request: peerID=%v currentTerm=%v args=%v", requestID, server, currentTerm, args))
	requestSucceed := rf.peers[server].Call(requestID, "Raft.RequestVote", args, reply)
	if !requestSucceed {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("(%v)[sendRequestVote] received response: peerID=%v peerTerm=%v currentTerm=%v reply=%v", requestID, server, reply.PeerTerm, rf.currentTerm, reply))
	if reply.PeerTerm > rf.currentTerm {
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[sendRequestVote] request outdated: peerID=%v peerTerm=%v currentTerm=%v", server, reply.PeerTerm, rf.currentTerm))
		rf.currentTerm = reply.PeerTerm
		rf.persist()
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[sendRequestVote] update currentTerm: currentTerm=%v", rf.currentTerm))

		if rf.role != FollowerRole {
			rf.role = FollowerRole
			rf.runAsFollower()
		}
	}

	return true
}

func (rf *Raft) sendAppendEntries(logFlow Flow, server int, isReliable bool, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	requestID := rf.nextRequestID
	rf.nextRequestID++
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	args.RequestID = requestID
	var requestSucceed bool
	if isReliable {
		reliableResponse := rf.reliableCall(requestID, func() rpcResponse {
			rf.logger.log(infoLogLevel, logFlow, fmt.Sprintf("(%v)[sendAppendEntries] send request: peerID=%v currentTerm=%v args=%v", requestID, server, currentTerm, args))
			tmpReply := AppendEntriesReply{}
			succeed := rf.peers[server].Call(requestID, "Raft.AppendEntries", args, &tmpReply)
			return rpcResponse{
				reply:   tmpReply,
				succeed: succeed,
			}
		}, rpcTimeout)
		requestSucceed = reliableResponse.succeed
		if requestSucceed {
			actualReply := reliableResponse.reply.(AppendEntriesReply)
			reply.Success = actualReply.Success
			reply.PeerTerm = actualReply.PeerTerm
			reply.ConflictTerm = actualReply.ConflictTerm
			reply.ConflictIndex = actualReply.ConflictIndex
		}
	} else {
		rf.logger.log(infoLogLevel, logFlow, fmt.Sprintf("(%v)[sendAppendEntries] send request: peerID=%v currentTerm=%v args=%v", requestID, server, currentTerm, args))
		requestSucceed = rf.peers[server].Call(requestID, "Raft.AppendEntries", args, reply)
	}

	if !requestSucceed {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.PeerTerm > rf.currentTerm {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[sendAppendEntries] request outdated: peerID=%v, peerTerm=%v, currentTerm=%v", server, reply.PeerTerm, rf.currentTerm))
		rf.currentTerm = reply.PeerTerm
		rf.persist()
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[sendAppendEntries] update currentTerm: currentTerm=%v", rf.currentTerm))
		if rf.role != FollowerRole {
			rf.role = FollowerRole
			rf.runAsFollower()
		}
	}

	return true
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	rf.mu.Lock()
	requestID := rf.nextRequestID
	rf.nextRequestID++
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	args.RequestID = requestID
	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[sendInstallSnapshot] send request: peerID=%v currentTerm=%v args=%v", requestID, server, currentTerm, args))
	var requestSucceed bool
	reliableResponse := rf.reliableCall(requestID, func() rpcResponse {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("(%v)[sendInstallSnapshot] send request: peerID=%v currentTerm=%v args=%v", requestID, server, currentTerm, args))
		tmpReply := InstallSnapshotReply{}
		succeed := rf.peers[server].Call(requestID, "Raft.InstallSnapshot", args, &tmpReply)
		return rpcResponse{
			reply:   tmpReply,
			succeed: succeed,
		}
	}, rpcTimeout)
	requestSucceed = reliableResponse.succeed
	if !requestSucceed {
		return false
	}

	actualReply := reliableResponse.reply.(InstallSnapshotReply)
	reply.PeerTerm = actualReply.PeerTerm

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.PeerTerm > rf.currentTerm {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[sendInstallSnapshot] outdated: peerID=%v, peerTerm=%v, currentTerm=%v", server, reply.PeerTerm, rf.currentTerm))
		rf.currentTerm = reply.PeerTerm
		rf.persist()
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[sendInstallSnapshot] update currentTerm: currentTerm=%v", rf.currentTerm))
		if rf.role != FollowerRole {
			rf.role = FollowerRole
			rf.runAsFollower()
		}
	}

	return true
}

func (rf *Raft) relativeIndex(index int) int {
	return index - rf.snapshotLastIncludedIndex
}

func (rf *Raft) absoluteIndex(index int) int {
	return rf.snapshotLastIncludedIndex + index
}

func (rf *Raft) logEntryTerm(index int) int {
	relativeIndex := rf.relativeIndex(index)
	if relativeIndex < 1 {
		return rf.snapshotLastIncludedTerm
	}

	return rf.logEntries[relativeIndex-1].LeaderReceivedTerm
}

func (rf *Raft) trimLogEntries(lastRelativeIndexToTrim int) {
	trimmedLogEntries := rf.logEntries[lastRelativeIndexToTrim:]
	logEntriesCopy := make([]LogEntry, len(trimmedLogEntries))
	copy(logEntriesCopy, trimmedLogEntries)

	rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("trimming log entries: oldLogEntries=%v newLogEntries=%v",
		rf.logEntries,
		logEntriesCopy))
	rf.logEntries = logEntriesCopy
}

func (rf *Raft) getNextLogIndex(conflictTerm int, conflictIndex int) int {
	if conflictTerm > 0 {
		index := rf.findIndexOfLastLogEntry(conflictTerm)
		if index > 0 {
			return index + 1
		}
	}

	return max(conflictIndex, 1)
}

func (rf *Raft) findIndexOfFirstLogEntry(term int) int {
	if term == rf.snapshotLastIncludedTerm {
		return rf.snapshotLastIncludedIndex
	}

	for index := 0; index < len(rf.logEntries); index++ {
		logEntry := rf.logEntries[index]
		if logEntry.LeaderReceivedTerm == term {
			return logEntry.Index
		}
	}

	return 0
}

func (rf *Raft) findIndexOfLastLogEntry(term int) int {
	for index := len(rf.logEntries) - 1; index >= 0; index-- {
		logEntry := rf.logEntries[index]
		if logEntry.LeaderReceivedTerm == term {
			return logEntry.Index
		}
	}

	if term == rf.snapshotLastIncludedTerm {
		return rf.snapshotLastIncludedIndex
	}

	return 0
}

type rpcResponse struct {
	args    interface{}
	reply   interface{}
	succeed bool
}

func (rf *Raft) reliableCall(requestID int, call func() rpcResponse, timeout time.Duration) rpcResponse {
	responseCh := make(chan rpcResponse)
	rf.logger.log(infoLogLevel, RPCFlow, fmt.Sprintf("(%v)[reliableCall] make initial RPC call", requestID))
	go func() {
		responseCh <- call()
	}()

	timeoutCh := time.After(timeout)
	select {
	case response := <-responseCh:
		if response.succeed {
			rf.logger.log(infoLogLevel, RPCFlow, fmt.Sprintf("(%v)[reliableCall] initial RPC succeed: reply=%v", requestID, response.reply))
		} else {
			rf.logger.log(infoLogLevel, RPCFlow, fmt.Sprintf("(%v)[reliableCall] initial RPC failed", requestID))
		}

		return response
	case <-time.After(rpcBackupDeadline):
	case <-timeoutCh:
		rf.logger.log(infoLogLevel, RPCFlow, fmt.Sprintf("(%v)[reliableCall] initial RPC timeout after %v", requestID, timeout))
		return rpcResponse{
			succeed: false,
		}
	}

	rf.logger.log(infoLogLevel, RPCFlow, fmt.Sprintf("(%v)[reliableCall] RPC timeout after %v, make backup RPC calls", requestID, rpcBackupInterval))
	for index := 0; index < 5; index++ {
		go func() {
			responseCh <- call()
		}()
	}

	for {
		select {
		case response := <-responseCh:
			if response.succeed {
				rf.logger.log(infoLogLevel, RPCFlow, fmt.Sprintf("(%v)backup RPC succeed: reply=%v", requestID, response.reply))
				return response
			} else {
				rf.logger.log(infoLogLevel, RPCFlow, fmt.Sprintf("(%v)backup RPC failed", requestID))
			}
		case <-time.After(rpcBackupInterval):
			rf.logger.log(infoLogLevel, RPCFlow, fmt.Sprintf("(%v)backup RPCs timeout after %v, make backup RPC calls", requestID, rpcBackupInterval))
			go func() {
				responseCh <- call()
			}()
		case <-timeoutCh:
			rf.logger.log(infoLogLevel, RPCFlow, fmt.Sprintf("(%v)backup RPC timeout after %v", requestID, timeout))
			return rpcResponse{
				succeed: false,
			}
		}
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	data, err := rf.encodeState()
	if err != nil {
		rf.logger.log(errorLogLevel, PersistenceFlow, fmt.Sprintf("encode state: err=%v", err))
		return
	}

	rf.persister.SaveRaftState(data)

	rf.logger.log(infoLogLevel, PersistenceFlow, "write start")
	rf.logger.log(infoLogLevel, PersistenceFlow, fmt.Sprintf("write currentTerm: currentTerm=%v",
		rf.currentTerm))
	rf.logger.log(infoLogLevel, PersistenceFlow, fmt.Sprintf("write currentTerm: votedForCandidateID=%v",
		intPtrToString(rf.votedForCandidateID)))
	rf.logger.log(infoLogLevel, PersistenceFlow, fmt.Sprintf("write currentTerm: logEntries=%v",
		rf.logEntries))
	rf.logger.log(infoLogLevel, PersistenceFlow, fmt.Sprintf("write currentTerm: snapshotLastIncludedIndex=%v",
		rf.snapshotLastIncludedIndex))
	rf.logger.log(infoLogLevel, PersistenceFlow, fmt.Sprintf("write currentTerm: snapshotLastIncludedTerm=%v",
		rf.snapshotLastIncludedTerm))
	rf.logger.log(infoLogLevel, PersistenceFlow, "write end")
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		rf.logger.log(infoLogLevel, PersistenceFlow, "initial state is empty")
		return
	}

	buf := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buf)
	var currentTerm int
	var votedForCandidateID int
	var logEntries []LogEntry

	err := decoder.Decode(&currentTerm)
	if err != nil {
		rf.logger.log(errorLogLevel, PersistenceFlow, fmt.Sprintf("read currentTerm: err=%v", err))
		return
	}

	err = decoder.Decode(&votedForCandidateID)
	if err != nil {
		rf.logger.log(errorLogLevel, PersistenceFlow, fmt.Sprintf("read votedForCandidateID: err=%v", err))
		return
	}

	err = decoder.Decode(&logEntries)
	if err != nil {
		rf.logger.log(errorLogLevel, PersistenceFlow, fmt.Sprintf("read logEntries: err=%v", err))
		return
	}

	err = decoder.Decode(&rf.snapshotLastIncludedIndex)
	if err != nil {
		rf.logger.log(errorLogLevel, PersistenceFlow, fmt.Sprintf("read snapshotLastIncludedIndex: err=%v", err))
		return
	}

	err = decoder.Decode(&rf.snapshotLastIncludedTerm)
	if err != nil {
		rf.logger.log(errorLogLevel, PersistenceFlow, fmt.Sprintf("read snapshotLastIncludedTerm: err=%v", err))
		return
	}

	rf.currentTerm = currentTerm
	if votedForCandidateID >= 0 {
		rf.votedForCandidateID = &votedForCandidateID
	}

	rf.logEntries = logEntries

	rf.logger.log(infoLogLevel, PersistenceFlow, "read start")
	rf.logger.log(infoLogLevel, PersistenceFlow, fmt.Sprintf("read currentTerm: currentTerm=%v",
		rf.currentTerm))
	rf.logger.log(infoLogLevel, PersistenceFlow, fmt.Sprintf("read currentTerm: votedForCandidateID=%v",
		intPtrToString(rf.votedForCandidateID)))
	rf.logger.log(infoLogLevel, PersistenceFlow, fmt.Sprintf("read currentTerm: logEntries=%v",
		rf.logEntries))
	rf.logger.log(infoLogLevel, PersistenceFlow, fmt.Sprintf("read snapshotLastIncludedIndex: snapshotLastIncludedIndex=%v",
		rf.snapshotLastIncludedIndex))
	rf.logger.log(infoLogLevel, PersistenceFlow, fmt.Sprintf("read snapshotLastIncludedTerm: snapshotLastIncludedTerm=%v",
		rf.snapshotLastIncludedTerm))
	rf.logger.log(infoLogLevel, PersistenceFlow, "read end")
}

func (rf *Raft) encodeState() ([]byte, error) {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	err := encoder.Encode(rf.currentTerm)
	if err != nil {
		rf.logger.log(errorLogLevel, PersistenceFlow, fmt.Sprintf("write currentTerm: err=%v", err))
		return nil, err
	}

	var votedForCandidateID = -1
	if rf.votedForCandidateID != nil {
		votedForCandidateID = *rf.votedForCandidateID
	}

	err = encoder.Encode(votedForCandidateID)
	if err != nil {
		rf.logger.log(errorLogLevel, PersistenceFlow, fmt.Sprintf("encode votedForCandidateID: err=%v", err))
		return nil, err
	}

	err = encoder.Encode(rf.logEntries)
	if err != nil {
		rf.logger.log(errorLogLevel, PersistenceFlow, fmt.Sprintf("encode logEntries: err=%v", err))
		return nil, err
	}

	err = encoder.Encode(rf.snapshotLastIncludedIndex)
	if err != nil {
		rf.logger.log(errorLogLevel, PersistenceFlow, fmt.Sprintf("encode snapshotLastIncludedIndex: err=%v", err))
		return nil, err
	}

	err = encoder.Encode(rf.snapshotLastIncludedTerm)
	if err != nil {
		rf.logger.log(errorLogLevel, PersistenceFlow, fmt.Sprintf("encode snapshotLastIncludedTerm: err=%v", err))
		return nil, err
	}

	return buf.Bytes(), nil
}

func min(num1 int, num2 int) int {
	if num1 <= num2 {
		return num1
	}

	return num2
}

func max(num1 int, num2 int) int {
	if num1 >= num2 {
		return num1
	}

	return num2
}

func intPtrToString(num *int) string {
	if num == nil {
		return "nil"
	}

	return strconv.FormatInt(int64(*num), 10)
}
