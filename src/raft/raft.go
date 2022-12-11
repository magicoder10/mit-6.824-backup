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
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

const heartbeatInterval = 150 * time.Millisecond
const minElectionTimeout = 400 * time.Millisecond

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

	replicateLogsCh       chan bool
	replicateLogsToPeerCh []chan bool
	commitLogCh           chan bool
	installingSnapshot    bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == LeaderRole
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
	rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] Snapshot")

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
		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] Snapshot 1")
		return
	}

	rf.snapshotLastIncludedTerm = rf.logEntryTerm(index)
	rf.trimLogEntries(rf.relativeIndex(index))
	rf.snapshotLastIncludedIndex = index
	raftState, err := rf.encodeState()
	if err != nil {
		rf.logger.log(errorLogLevel, SnapshotFlow, fmt.Sprintf("encode state: err=%v", err))
		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] Snapshot 2")
		return
	}

	rf.persister.SaveStateAndSnapshot(raftState, snapshot)
	rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("finish taking snapshot: lastIncludedTerm=%v", index))
	rf.logger.log(debugLogLevel, LockFlow, "[mu][release] Snapshot 3")
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[RequestVote] receive request vote: args=%v",
		args))

	rf.mu.Lock()
	rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] RequestVote")
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.PeerTerm = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[RequestVote] request outdated: snapshotLastTerm=%v snapshotLastIndex=%v currentTerm=%v peerTerm=%v",
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			rf.currentTerm,
			args.Term))
		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] RequestVote 2")
		return
	}

	if args.Term > rf.currentTerm {
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[RequestVote] enter new term: snapshotLastTerm=%v snapshotLastIndex=%v currentTerm=%v peerTerm=%v",
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			rf.currentTerm,
			args.Term))
		rf.enterNewTerm(args.Term)
		rf.persist()
		reply.PeerTerm = rf.currentTerm

		if rf.role != FollowerRole {
			rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[RequestVote] convert from %v to follower", rf.role))
			rf.role = FollowerRole
			defer rf.runAsFollower()
		}
	}

	if rf.votedForCandidateID != nil {
		if *rf.votedForCandidateID == args.CandidateId {
			rf.grantedVoteToPeer = true
			reply.VoteGranted = true
			rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[RequestVote] vote already granted to candidate: currentTerm=%v candidateId=%v",
				rf.currentTerm,
				args.CandidateId))
			rf.logger.log(debugLogLevel, LockFlow, "[mu][release] RequestVote 3")
			return
		}

		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[RequestVote] already voted for another candidate: term=%v votedFor=%v candidateId=%v",
			rf.currentTerm,
			intPtrToString(rf.votedForCandidateID),
			args.CandidateId))
		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] RequestVote 4")
		return
	}

	logLen := len(rf.logEntries)
	lastLogIndex := rf.absoluteIndex(logLen)
	lastLogTerm := rf.logEntryTerm(lastLogIndex)
	if lastLogTerm > args.LastLogTerm {
		rf.logger.log(errorLogLevel, LeaderElectionFlow, fmt.Sprintf("[RequestVote] candidate log is on old term: snapshotLastTerm=%v snapshotLastIndex=%v candidateId=%v candidateTerm=%v currentLastLogTerm=%v candidateLastLogTerm=%v",
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			args.CandidateId,
			args.Term,
			lastLogTerm,
			args.LastLogTerm))
		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] RequestVote 5")
		return
	}

	if lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex {
		rf.logger.log(errorLogLevel, LeaderElectionFlow, fmt.Sprintf("[RequestVote] candidate log is shorter: snapshotLastTerm=%v snapshotLastIndex=%v candidateId=%v candidateTerm=%v myLogLength=%v candidateLastLogIndex=%v",
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			args.CandidateId,
			args.Term,
			rf.absoluteIndex(logLen),
			args.LastLogIndex))
		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] RequestVote 6")
		return
	}

	rf.grantedVoteToPeer = true
	reply.VoteGranted = true
	rf.votedForCandidateID = &args.CandidateId
	rf.persist()
	rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[RequestVote] granted vote to candidate: term=%v candidateId=%v myLastLogTerm=%v myLastLogIndex=%v peerLastLogTerm=%v peerLastLogIndex=%v",
		rf.currentTerm,
		args.CandidateId,
		lastLogTerm,
		lastLogIndex,
		args.LastLogTerm,
		args.LastLogIndex))
	rf.logger.log(debugLogLevel, LockFlow, "[mu][release] RequestVote 7")
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] receive append entries: args=%v",
		args))

	rf.mu.Lock()
	rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] AppendEntries 1")
	defer rf.mu.Unlock()
	reply.Success = false
	reply.PeerTerm = rf.currentTerm
	if args.LeaderTerm < rf.currentTerm {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] outdated append entries: snapshotLastTerm=%v snapshotLastIndex=%v leaderTerm=%v currentTerm=%v",
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			args.LeaderTerm,
			rf.currentTerm))
		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] AppendEntries 3")
		return
	}

	if args.LeaderTerm > rf.currentTerm {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] update term: snapshotLastTerm=%v snapshotLastIndex=%v leaderTerm=%v currentTerm=%v",
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			args.LeaderTerm,
			rf.currentTerm))
		rf.enterNewTerm(args.LeaderTerm)
		rf.persist()
		reply.PeerTerm = rf.currentTerm

		if rf.role != FollowerRole {
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] convert from %v to follower", rf.role))
			rf.role = FollowerRole
			defer rf.runAsFollower()
		}
	} else if rf.role == CandidateRole {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] convert from %v to follower", rf.role))
		rf.role = FollowerRole
		defer rf.runAsFollower()
	}

	rf.receivedMessageFromLeader = true

	if len(rf.logEntries) < rf.relativeIndex(args.PrevLogIndex) {
		rf.logger.log(errorLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] prevLogIndex exceeds last log entry: snapshotLastTerm=%v snapshotLastIndex=%v leaderPrevLogIndex=%v",
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			args.PrevLogIndex))
		rf.logger.log(debugLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] logEntries=%v", rf.logEntries))
		reply.ConflictTerm = rf.findTermBeforeOrEqual(args.PrevLogTerm)
		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] AppendEntries 4")
		return
	}

	if args.PrevLogIndex > 0 {
		if rf.snapshotLastIncludedIndex <= args.PrevLogIndex {
			prevLogTerm := rf.logEntryTerm(args.PrevLogIndex)
			if prevLogTerm != args.PrevLogTerm {
				rf.logger.log(errorLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] previous log entry term mismatch: snapshotLastTerm=%v snapshotLastIndex=%v prevLogIndex=%v prevLogTerm=%v expectedPrevLogTerm=%v",
					rf.snapshotLastIncludedTerm,
					rf.snapshotLastIncludedIndex,
					args.PrevLogIndex,
					prevLogTerm,
					args.PrevLogTerm))
				rf.logger.log(debugLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] logEntries=%v", rf.logEntries))
				reply.ConflictTerm = rf.findTermBeforeOrEqual(args.PrevLogTerm)
				rf.logger.log(debugLogLevel, LockFlow, "[mu][release] AppendEntries 5")
				return
			}
		}
	}

	rf.logger.log(debugLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] current log entries: snapshotLastTerm=%v snapshotLastIndex=%v logEntries=%v",
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
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] removing conflicting entries: snapshotLastTerm=%v snapshotLastIndex=%v newEntryStartIndex=%v oldEntry=%v newEntry=%v",
				rf.snapshotLastIncludedTerm,
				rf.snapshotLastIncludedIndex,
				newEntriesStartIndex,
				oldEntry,
				newEntry))
			rf.logEntries = rf.logEntries[:relativeLogIndex-1]
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] removed conflicting entries: snapshotLastTerm=%v snapshotLastIndex=%v",
				rf.snapshotLastIncludedTerm,
				rf.snapshotLastIncludedIndex))
			rf.logger.log(debugLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] logEntries=%v", rf.logEntries))
			rf.persist()
			updated = true
			break
		}
	}

	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] processing newEntries: snapshotLastTerm=%v snapshotLastIndex=%v newEntryStartIndex=%v",
		rf.snapshotLastIncludedTerm,
		rf.snapshotLastIncludedIndex,
		newEntriesStartIndex))
	if newEntriesStartIndex < len(args.Entries) {
		rf.logEntries = append(rf.logEntries, args.Entries[newEntriesStartIndex:]...)
		rf.persist()
		updated = true
	}

	if updated {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] updated log entries: snapshotLastTerm=%v snapshotLastIndex=%v",
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex))
	} else {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] no change to log entries: snapshotLastTerm=%v snapshotLastIndex=%v",
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex))
	}

	rf.logger.log(debugLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] logEntries=%v", rf.logEntries))

	if args.LeaderCommitIndex > rf.commitLogIndex {
		lastIndex := args.PrevLogIndex
		if len(args.Entries) > 0 {
			lastIndex = args.Entries[len(args.Entries)-1].Index
		}

		rf.commitLogIndex = min(args.LeaderCommitIndex, lastIndex)
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] commit entries: snapshotLastTerm=%v snapshotLastIndex=%v commitLogIndex=%v",
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			rf.commitLogIndex))
		commitLogCh := rf.commitLogCh
		go func() {
			commitLogCh <- true
		}()
	}

	reply.Success = true
	rf.logger.log(debugLogLevel, LockFlow, "[mu][release] AppendEntries 6")
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("[InstallSnapshot] receive install snapshot: args=%v",
		args))
	rf.mu.Lock()
	rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] InstallSnapshot 1")
	reply.PeerTerm = rf.currentTerm
	if args.LeaderTerm < rf.currentTerm {
		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] InstallSnapshot 2")
		rf.mu.Unlock()
		return
	}

	if args.LeaderTerm > rf.currentTerm {
		rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("[InstallSnapshot] update term: snapshotLastTerm=%v snapshotLastIndex=%v leaderTerm=%v currentTerm=%v",
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			args.LeaderTerm,
			rf.currentTerm))
		rf.enterNewTerm(args.LeaderTerm)
		rf.persist()
		reply.PeerTerm = rf.currentTerm

		if rf.role != FollowerRole {
			rf.role = FollowerRole
			rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("[InstallSnapshot] convert from %v to follower", rf.role))
			defer rf.runAsFollower()
		}
	} else if rf.role == CandidateRole {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[InstallSnapshot] convert from %v to follower", rf.role))
		rf.role = FollowerRole
		defer rf.runAsFollower()
	}

	rf.receivedMessageFromLeader = true

	relativeSnapshotLastIncludedIndex := rf.relativeIndex(args.SnapshotLastIncludedIndex)
	if relativeSnapshotLastIncludedIndex < 1 {
		rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("[InstallSnapshot] snapshot up to date, skipping: currentLastIncludedIndex=%v newLastIncludedTerm=%v",
			rf.snapshotLastIncludedIndex,
			args.SnapshotLastIncludedIndex))
		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] InstallSnapshot 3")
		rf.mu.Unlock()
		return
	}

	if relativeSnapshotLastIncludedIndex < len(rf.logEntries) &&
		rf.logEntries[relativeSnapshotLastIncludedIndex-1].LeaderReceivedTerm == args.SnapshotLastIncludedTerm {
		rf.commitLogIndex = max(rf.commitLogIndex, args.SnapshotLastIncludedIndex-1)
	}

	rf.installingSnapshot = true

	rf.logger.log(debugLogLevel, LockFlow, "[mu][release] InstallSnapshot 4")
	rf.mu.Unlock()

	rf.commitLogCh <- true

	rf.mu.Lock()
	rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] InstallSnapshot 2")

	relativeSnapshotLastIncludedIndex = rf.relativeIndex(args.SnapshotLastIncludedIndex)
	if relativeSnapshotLastIncludedIndex < 1 {
		rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("[InstallSnapshot] snapshot up to date, skipping: currentLastIncludedIndex=%v newLastIncludedTerm=%v",
			rf.snapshotLastIncludedIndex,
			args.SnapshotLastIncludedIndex))
		rf.installingSnapshot = false
		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] InstallSnapshot 5")
		rf.mu.Unlock()
		return
	}

	if relativeSnapshotLastIncludedIndex >= len(rf.logEntries) ||
		rf.logEntries[relativeSnapshotLastIncludedIndex-1].LeaderReceivedTerm != args.SnapshotLastIncludedTerm {
		rf.logEntries = make([]LogEntry, 0)
	} else {
		rf.trimLogEntries(relativeSnapshotLastIncludedIndex)
	}

	rf.snapshotLastIncludedIndex = args.SnapshotLastIncludedIndex
	rf.snapshotLastIncludedTerm = args.SnapshotLastIncludedTerm
	raftState, err := rf.encodeState()
	if err != nil {
		rf.logger.log(errorLogLevel, SnapshotFlow, fmt.Sprintf("[InstallSnapshot] encode state: err=%v", err))
		rf.installingSnapshot = false
		rf.logger.log(infoLogLevel, LockFlow, "[mu][release] InstallSnapshot 6")
		rf.mu.Unlock()
		return
	}

	rf.persister.SaveStateAndSnapshot(raftState, args.SnapshotData)
	rf.commitLogIndex = max(rf.commitLogIndex, args.SnapshotLastIncludedIndex)
	rf.lastAppliedLogIndex = max(rf.lastAppliedLogIndex, args.SnapshotLastIncludedIndex)
	rf.mu.Unlock()

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.SnapshotData,
		SnapshotIndex: args.SnapshotLastIncludedIndex,
		SnapshotTerm:  args.SnapshotLastIncludedTerm,
	}
	rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("[InstallSnapshot] applying snapshot: applyMsg=%v", applyMsg))
	rf.applyCh <- applyMsg

	rf.mu.Lock()
	rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] InstallSnapshot 3")

	defer rf.mu.Unlock()
	rf.installingSnapshot = false
	rf.logger.log(infoLogLevel, SnapshotFlow, "[InstallSnapshot] snapshot installed")
	rf.logger.log(debugLogLevel, LockFlow, "[mu][release] InstallSnapshot 7")

}

func (rf *Raft) findTermBeforeOrEqual(peerTerm int) int {
	if peerTerm == 0 {
		return 0
	}

	for index := len(rf.logEntries) - 1; index >= 0; index-- {
		logEntry := rf.logEntries[index]
		if logEntry.LeaderReceivedTerm > peerTerm {
			continue
		}

		return logEntry.LeaderReceivedTerm
	}

	if rf.snapshotLastIncludedTerm <= peerTerm {
		return rf.snapshotLastIncludedTerm
	}

	return 0
}

func (rf *Raft) findIndexOfFirstLogEntry(term int) int {
	if term <= rf.snapshotLastIncludedTerm {
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
	rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] Start")

	defer rf.mu.Unlock()
	if rf.killed() {
		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] Start 1")
		return 0, rf.currentTerm, rf.role == LeaderRole
	}

	if rf.role != LeaderRole {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("only leader can receive client command: command=%v",
			command))
		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] Start 2")
		return 0, rf.currentTerm, false
	}

	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("received command from client: command=%v",
		command))
	nextLogIndex := rf.absoluteIndex(len(rf.logEntries) + 1)
	logEntry := LogEntry{
		Command:            command,
		Index:              nextLogIndex,
		LeaderReceivedTerm: rf.currentTerm,
	}
	rf.logEntries = append(rf.logEntries, logEntry)
	rf.persist()
	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("append entry to leader log: snapshotLastTerm=%v snapshotLastIndex=%v logEntry=%v",
		rf.snapshotLastIncludedTerm,
		rf.snapshotLastIncludedIndex,
		logEntry))

	replicateLogsCh := rf.replicateLogsCh
	go func() {
		replicateLogsCh <- true
	}()

	rf.logger.log(debugLogLevel, LockFlow, "[mu][release] Start 3")
	return nextLogIndex, rf.currentTerm, true
}

func (rf *Raft) replicateLogEntries() {
	rf.mu.Lock()
	rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] replicateLogEntries 1")

	requiredPeers := len(rf.peers) / 2
	lastLogIndexToReplicate := rf.absoluteIndex(len(rf.logEntries))
	leaderCommitLogIndex := rf.commitLogIndex
	replicateEntriesTerm := rf.currentTerm
	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogEntries] replicate log entries to all peers: requiredPeers=%v snapshotLastTerm=%v snapshotLastIndex=%v lastLogIndexToReplicate=%v",
		requiredPeers,
		rf.snapshotLastIncludedTerm,
		rf.snapshotLastIncludedIndex,
		lastLogIndexToReplicate))
	rf.logger.log(debugLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogEntries] logEntries=%v", rf.logEntries))
	rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntries 1")
	rf.mu.Unlock()

	var replicateLogsMut sync.Mutex
	replicateLogsCond := sync.NewCond(&replicateLogsMut)
	replicatedPeerCount := 0
	waitForReplication := true
	for peerId := 0; peerId < len(rf.peers) && !rf.killed(); peerId++ {
		if peerId == rf.me {
			continue
		}

		rf.mu.Lock()
		rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] replicateLogEntries 2")
		if rf.role != LeaderRole {
			rf.logger.log(infoLogLevel, LogReplicationFlow, "not leader anymore when replicating log")

			rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntries 2")
			rf.mu.Unlock()
			return
		}

		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntries 3")
		rf.mu.Unlock()

		go func(peerId int) {
			updateToDate := rf.replicateLogEntriesToPeer(peerId, lastLogIndexToReplicate, leaderCommitLogIndex)
			replicateLogsMut.Lock()

			if updateToDate {
				replicatedPeerCount++
			} else {
				waitForReplication = false
			}

			replicateLogsMut.Unlock()
			replicateLogsCond.Signal()
		}(peerId)
	}

	replicateLogsMut.Lock()
	for replicatedPeerCount < requiredPeers && waitForReplication {
		replicateLogsCond.Wait()
	}

	if !waitForReplication {
		rf.logger.log(infoLogLevel, LogReplicationFlow, "exit log replication")
		replicateLogsMut.Unlock()
		return
	}

	replicateLogsMut.Unlock()

	if rf.killed() {
		return
	}

	rf.mu.Lock()
	rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] replicateLogEntries 3")

	defer rf.mu.Unlock()
	if rf.currentTerm != replicateEntriesTerm {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("log replication is outdated: snapshotLastTerm=%v snapshotLastIndex=%v replicatingTerm=%v currentTerm=%v",
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			replicateEntriesTerm,
			rf.currentTerm))
		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntries 4")
		return
	}

	if rf.role != LeaderRole {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("not leader any more, stopped replicating log to peers: role=%v",
			rf.role))
		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntries 5")
		return
	}

	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("replicated log entries to peers: snapshotLastTerm=%v snapshotLastIndex=%v lastLogIndexToReplicate=%v",
		rf.snapshotLastIncludedTerm,
		rf.snapshotLastIncludedIndex,
		lastLogIndexToReplicate))
	if lastLogIndexToReplicate <= 0 {
		rf.logger.log(infoLogLevel, LogReplicationFlow, "no entry is replicated, skip committing")
		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntries 6")
		return
	}

	lastEntryTerm := rf.logEntryTerm(lastLogIndexToReplicate)
	if lastEntryTerm != rf.currentTerm {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("last replicated entry not from current term, skip committing: snapshotLastTerm=%v snapshotLastIndex=%v lastEntryTerm=%v, currentTerm=%v",
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			lastEntryTerm,
			rf.currentTerm))
		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntries 7")
		return
	}

	if lastLogIndexToReplicate > rf.commitLogIndex {
		oldCommitLogIndex := rf.commitLogIndex
		rf.commitLogIndex = lastLogIndexToReplicate
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("commit log entries: snapshotLastTerm=%v snapshotLastIndex=%v oldCommitLogIndex=%v newCommitLogIndex=%v",
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			oldCommitLogIndex,
			rf.commitLogIndex))
		if rf.installingSnapshot {
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("installing snapshot, skip applying commits: snapshotLastTerm=%v snapshotLastIndex=%v oldCommitLogIndex=%v newCommitLogIndex=%v",
				rf.snapshotLastIncludedTerm,
				rf.snapshotLastIncludedIndex,
				oldCommitLogIndex,
				rf.commitLogIndex))
			rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntries 8")
			return
		}

		commitLogCh := rf.commitLogCh
		go func() {
			commitLogCh <- true
		}()
	}

	rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntries 9")
}

func (rf *Raft) replicateLogEntriesToPeer(peerId int, lastLogIndexToReplicate int, leaderCommitLogIndex int) bool {
	for !rf.killed() {
		rf.mu.Lock()
		rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] replicateLogEntriesToPeer 1")

		if rf.role != LeaderRole {
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogEntriesToPeer] not leader any more, stop replicating log to peer: role=%v peerId=%v",
				rf.role,
				peerId))

			rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntriesToPeer 1")
			rf.mu.Unlock()
			return false
		}

		if rf.lastReplicatedLogIndices[peerId] > lastLogIndexToReplicate {
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogEntriesToPeer] log replication outdated, entries already replicated: peerId=%v snapshotLastTerm=%v snapshotLastIndex=%v lastReplicatedLogIndex=%v lastLogIndexToReplicate=%v",
				peerId,
				rf.snapshotLastIncludedTerm,
				rf.snapshotLastIncludedIndex,
				rf.lastReplicatedLogIndices[peerId],
				lastLogIndexToReplicate))
			rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntriesToPeer 2")
			rf.mu.Unlock()
			return false
		}

		if lastLogIndexToReplicate > rf.absoluteIndex(len(rf.logEntries)) {
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogEntriesToPeer] log replication outdated, lastLogIndexToReplicate too big: peerId=%v snapshotLastTerm=%v snapshotLastIndex=%v lastLogEntrIndex=%v lastLogIndexToReplicate=%v",
				peerId,
				rf.snapshotLastIncludedTerm,
				rf.snapshotLastIncludedIndex,
				rf.absoluteIndex(len(rf.logEntries)),
				lastLogIndexToReplicate))
			rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntriesToPeer 3")
			rf.mu.Unlock()
			return false
		}

		nextLogIndex := rf.nextLogToSendIndices[peerId]
		if nextLogIndex > lastLogIndexToReplicate+1 {
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogEntriesToPeer] log replication outdated, lastLogIndexToReplicate is lagged behined: peerId=%v snapshotLastTerm=%v snapshotLastIndex=%v lastReplicatedLogIndex=%v lastLogIndexToReplicate=%v",
				peerId,
				rf.snapshotLastIncludedTerm,
				rf.snapshotLastIncludedIndex,
				nextLogIndex,
				lastLogIndexToReplicate))
			rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntriesToPeer 4")
			rf.mu.Unlock()
			return false
		}

		if nextLogIndex <= rf.snapshotLastIncludedIndex {
			args := InstallSnapshotArgs{
				LeaderTerm:                rf.currentTerm,
				LeaderID:                  rf.me,
				SnapshotLastIncludedIndex: rf.snapshotLastIncludedIndex,
				SnapshotLastIncludedTerm:  rf.snapshotLastIncludedTerm,
				SnapshotData:              rf.persister.ReadSnapshot(),
			}

			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogEntriesToPeer] start installing snapshot to peer: peerId=%v snapshotLastTerm=%v snapshotLastIndex=%v",
				peerId,
				rf.snapshotLastIncludedTerm,
				rf.snapshotLastIncludedIndex))
			rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntriesToPeer 5")
			rf.mu.Unlock()

			reply := InstallSnapshotReply{}
			requestSucceed := rf.sendInstallSnapshot(peerId, &args, &reply)

			rf.mu.Lock()
			rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] replicateLogEntriesToPeer 2")
			if !requestSucceed {
				rf.logger.log(errorLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogEntriesToPeer] fail to install snapshot to peer(RPC): peerId=%v snapshotLastTerm=%v snapshotLastIndex=%v",
					peerId,
					rf.snapshotLastIncludedTerm,
					rf.snapshotLastIncludedIndex))
				rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntriesToPeer 6")
				rf.mu.Unlock()
				continue
			}

			if rf.role != LeaderRole {
				rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntriesToPeer 7")
				rf.mu.Unlock()
				return false
			}

			if rf.currentTerm != args.LeaderTerm {
				rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntriesToPeer 8")
				rf.mu.Unlock()
				return false
			}

			rf.nextLogToSendIndices[peerId] = args.SnapshotLastIncludedIndex + 1
			rf.lastReplicatedLogIndices[peerId] = args.SnapshotLastIncludedIndex
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogEntriesToPeer] installed snapshot to peer: peerId=%v snapshotLastTerm=%v snapshotLastIndex=%v nextIndex=%v replicatedIndex=%v",
				peerId,
				rf.snapshotLastIncludedTerm,
				rf.snapshotLastIncludedIndex,
				rf.nextLogToSendIndices[peerId],
				rf.lastReplicatedLogIndices[peerId],
			))
			rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntriesToPeer 9")
			rf.mu.Unlock()
			return true
		}

		rf.logger.log(debugLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogEntriesToPeer] before replicating log entries to peer: peerId=%v snapshotLastTerm=%v snapshotLastIndex=%v lastLogIndexToReplicate=%v nextLogIndex=%v logEntries=%v",
			peerId,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			lastLogIndexToReplicate,
			nextLogIndex,
			rf.logEntries))
		prevLogTerm := rf.logEntryTerm(nextLogIndex - 1)
		logEntriesCopy := make([]LogEntry, lastLogIndexToReplicate-nextLogIndex+1)
		copy(logEntriesCopy, rf.logEntries[rf.relativeIndex(nextLogIndex)-1:rf.relativeIndex(lastLogIndexToReplicate)])

		args := AppendEntriesArgs{
			LeaderTerm:        rf.currentTerm,
			LeaderID:          rf.me,
			PrevLogIndex:      nextLogIndex - 1,
			PrevLogTerm:       prevLogTerm,
			Entries:           logEntriesCopy,
			LeaderCommitIndex: leaderCommitLogIndex,
		}

		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogEntriesToPeer] replicating log entries to peer: peerId=%v snapshotLastTerm=%v snapshotLastIndex=%v args=%v",
			peerId,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			args))
		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntriesToPeer 10")
		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		requestSucceed := rf.sendAppendEntries(peerId, &args, &reply)

		rf.mu.Lock()
		rf.logger.log(infoLogLevel, LockFlow, "[mu][acquire] replicateLogEntriesToPeer 3")
		if !requestSucceed {
			rf.logger.log(errorLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogEntriesToPeer] fail to replicate log entries to peer(RPC): peerId=%v snapshotLastTerm=%v snapshotLastIndex=%v nextLogIndex=%v",
				peerId,
				rf.snapshotLastIncludedTerm,
				rf.snapshotLastIncludedIndex,
				rf.nextLogToSendIndices[peerId]))
			rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntriesToPeer 11")
			rf.mu.Unlock()
			continue
		}

		if rf.role != LeaderRole {
			rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntriesToPeer 12")
			rf.mu.Unlock()
			return false
		}

		if rf.currentTerm != args.LeaderTerm {
			rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntriesToPeer 13")
			rf.mu.Unlock()
			return false
		}

		if reply.Success {
			rf.nextLogToSendIndices[peerId] = lastLogIndexToReplicate + 1
			rf.lastReplicatedLogIndices[peerId] = lastLogIndexToReplicate
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogEntriesToPeer] replicated log entries to peer: peerId=%v nextLogIndex=%v lastReplicatedLogIndex=%v",
				peerId,
				rf.nextLogToSendIndices[peerId],
				rf.lastReplicatedLogIndices[peerId]))
			rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntriesToPeer 14")
			rf.mu.Unlock()
			return true
		}

		rf.logger.log(errorLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogEntriesToPeer] fail to replicate log entries to peer: peerId=%v snapshotLastTerm=%v snapshotLastIndex=%v nextLogIndex=%v conflictTerm=%v",
			peerId,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			rf.nextLogToSendIndices[peerId],
			reply.ConflictTerm))

		conflictTerm := rf.findTermBeforeOrEqual(reply.ConflictTerm)
		firstEntryIndex := rf.findIndexOfFirstLogEntry(conflictTerm)
		rf.logger.log(debugLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogEntriesToPeer] before update nextIndex: peerId=%v snapshotLastTerm=%v snapshotLastIndex=%v nextIndex=%v conflictTerm=%v firstEntryIndex=%v lastReplicatedIndex=%v logEntries=%v",
			peerId,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			rf.nextLogToSendIndices[peerId],
			conflictTerm,
			firstEntryIndex,
			rf.lastReplicatedLogIndices[peerId],
			rf.logEntries,
		))

		if firstEntryIndex == nextLogIndex {
			firstEntryIndex--
		}

		rf.nextLogToSendIndices[peerId] = max(firstEntryIndex, rf.lastReplicatedLogIndices[peerId]+1)
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogEntriesToPeer] update nextIndex(conflictTerm): peerId=%v snapshotLastTerm=%v snapshotLastIndex=%v conflictTerm=%v firstEntryIndex=%v nextIndex=%v",
			peerId,
			rf.snapshotLastIncludedTerm,
			rf.snapshotLastIncludedIndex,
			conflictTerm,
			firstEntryIndex,
			rf.nextLogToSendIndices[peerId],
		))
		rf.logger.log(debugLogLevel, LogReplicationFlow, fmt.Sprintf("[replicateLogEntriesToPeer] logEntries=%v", rf.logEntries))
		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] replicateLogEntriesToPeer 15")
		rf.mu.Unlock()
	}

	return false
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

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) runAsFollower() {
	rf.logger.log(infoLogLevel, FollowerFlow, "running as follower in background")
	go func() {
		for !rf.killed() {
			rf.logger.log(infoLogLevel, FollowerFlow, "wait for election timeout")
			rf.waitForElectionTimeout()

			rf.mu.Lock()
			rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] runAsFollower 1")
			rf.logger.log(infoLogLevel, FollowerFlow, fmt.Sprintf("follower election timeout: role=%v", rf.role))
			if rf.role != FollowerRole {
				rf.mu.Unlock()
				rf.logger.log(infoLogLevel, FollowerFlow, "exit follower flow")
				return
			}

			if rf.receivedMessageFromLeader || rf.grantedVoteToPeer {
				rf.logger.log(infoLogLevel, FollowerFlow, "received message from peer or granted vote to peer")
				rf.receivedMessageFromLeader = false
				rf.grantedVoteToPeer = false
				rf.logger.log(debugLogLevel, LockFlow, "[mu][release] runAsFollower 2")
				rf.mu.Unlock()
				continue
			}

			rf.role = CandidateRole
			rf.logger.log(debugLogLevel, LockFlow, "[mu][release] runAsFollower 3")
			rf.mu.Unlock()
			rf.runAsCandidate()
			return
		}

		if rf.killed() {
			rf.logger.log(infoLogLevel, FollowerFlow, "follower is killed")
		}
	}()
}

func (rf *Raft) enterNewTerm(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedForCandidateID = nil
	rf.persist()
}

func (rf *Raft) runAsCandidate() {
	rf.logger.log(infoLogLevel, CandidateFlow, "running as candidate in background")
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] runAsCandidate 1")
			if rf.role != CandidateRole {
				rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("not candidate any more, exit election: role=%v", rf.role))
				rf.logger.log(debugLogLevel, LockFlow, "[mu][release] runAsCandidate 1")
				rf.mu.Unlock()
				return
			}

			if !rf.grantedVoteToPeer {
				rf.currentTerm++
				rf.votedForCandidateID = &rf.me
				rf.persist()
				currentTerm := rf.currentTerm

				rf.logger.log(debugLogLevel, LockFlow, "[mu][release] runAsCandidate 2")
				rf.mu.Unlock()

				rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("start election: newTerm=%v", currentTerm))
				halfVotes := len(rf.peers) / 2
				go func(electionTerm int) {
					rf.logger.log(infoLogLevel, LeaderElectionFlow, "request votes from peers")
					grantedVotes := rf.requestVoteFromPeers() + 1
					rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("received votes from peers: grantedVotes=%v, requiredVotes=%v",
						grantedVotes,
						halfVotes+1))

					rf.mu.Lock()
					rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] runAsCandidate 2")
					rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("electionTerm=%v, currentTerm=%v",
						electionTerm,
						rf.currentTerm))
					if electionTerm < rf.currentTerm {
						// new election started after election timeout
						rf.logger.log(infoLogLevel, LeaderElectionFlow, "votes outdated")
						rf.logger.log(debugLogLevel, LockFlow, "[mu][release] runAsCandidate 3")
						rf.mu.Unlock()
						return
					}

					if rf.role != CandidateRole {
						// received heartbeat from new leader & fallback to follower
						rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("not candidate, abandoned vote result: role=%s", rf.role))
						rf.logger.log(debugLogLevel, LockFlow, "[mu][release] runAsCandidate 4")
						rf.mu.Unlock()
						return
					}

					if grantedVotes > halfVotes {
						rf.logger.log(infoLogLevel, LeaderElectionFlow, "received enough votes, elected as leader")
						rf.role = LeaderRole
						rf.logger.log(debugLogLevel, LockFlow, "[mu][release] runAsCandidate 5")
						rf.mu.Unlock()
						rf.runAsLeader()
						return
					}

					if grantedVotes < halfVotes {
						rf.logger.log(infoLogLevel, LeaderElectionFlow, "not enough votes, fallback to follower")
						rf.role = FollowerRole
						rf.logger.log(debugLogLevel, LockFlow, "[mu][release] runAsCandidate 6")
						rf.mu.Unlock()
						rf.runAsFollower()
						return
					}

					rf.logger.log(debugLogLevel, LockFlow, "[mu][release] runAsCandidate 7")
					rf.mu.Unlock()
					rf.logger.log(infoLogLevel, LeaderElectionFlow, "split vote, no leader")
				}(currentTerm)
			} else {
				rf.logger.log(debugLogLevel, LockFlow, "[mu][release] runAsCandidate 8")
				rf.mu.Unlock()
			}

			rf.logger.log(infoLogLevel, LeaderElectionFlow, "wait for election timeout")
			rf.waitForElectionTimeout()

			rf.mu.Lock()
			rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] runAsCandidate 3")
			rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("candidate election timeout: role=%v", rf.role))
			rf.logger.log(debugLogLevel, LockFlow, "[mu][release] runAsCandidate 9")
			rf.mu.Unlock()
		}

		if rf.killed() {
			rf.logger.log(infoLogLevel, CandidateFlow, "candidate is killed")
		}
	}()
}

func (rf *Raft) waitForElectionTimeout() {
	randTimeout := time.Duration(rand.Intn(200)) * time.Millisecond
	electionTimeout := randTimeout + minElectionTimeout
	time.Sleep(electionTimeout)
}

func (rf *Raft) runAsLeader() {
	rf.mu.Lock()
	rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] runAsLeader 1")
	rf.logger.log(infoLogLevel, LeaderFlow, fmt.Sprintf("running as leader: term=%v", rf.currentTerm))

	numPeers := len(rf.peers)
	for peerId := 0; peerId < numPeers; peerId++ {
		rf.nextLogToSendIndices[peerId] = rf.absoluteIndex(len(rf.logEntries) + 1)
	}

	rf.lastReplicatedLogIndices = make([]int, numPeers)
	replicateLogsCh := rf.replicateLogsCh

	rf.logger.log(debugLogLevel, LockFlow, "[mu][release] runAsLeader 1")
	rf.mu.Unlock()

	go func() {
		rf.sendPeriodicHeartbeats()
	}()

	go func() {
		for range replicateLogsCh {
			if rf.killed() {
				rf.logger.log(infoLogLevel, LogReplicationFlow, "leader is killed")
				return
			}

			rf.mu.Lock()
			rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] runAsLeader 2")

			if rf.role != LeaderRole {
				rf.logger.log(infoLogLevel, LogReplicationFlow, "replicate is not leader anymore")
				rf.logger.log(debugLogLevel, LockFlow, "[mu][release] runAsLeader 1")
				rf.mu.Unlock()
				return
			}

			rf.logger.log(debugLogLevel, LockFlow, "[mu][release] runAsLeader 2")
			rf.mu.Unlock()
			rf.replicateLogEntries()
		}
	}()
}

func (rf *Raft) sendPeriodicHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] sendPeriodicHeartbeats")

		if rf.role != LeaderRole {
			rf.logger.log(debugLogLevel, LockFlow, "[mu][release] sendPeriodicHeartbeats 1")
			rf.mu.Unlock()
			return
		}

		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] sendPeriodicHeartbeats 2")
		rf.mu.Unlock()
		go func() {
			rf.logger.log(infoLogLevel, LeaderElectionFlow, "send heartbeat")
			rf.replicateLogEntries()
		}()
		time.Sleep(heartbeatInterval)
	}

	if rf.killed() {
		rf.logger.log(infoLogLevel, LeaderFlow, "leader is killed")
	}
}

func (rf *Raft) requestVoteFromPeers() int {
	rf.mu.Lock()
	rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] requestVoteFromPeers 1")

	requestedTerm := rf.currentTerm
	requestVoteArgs := RequestVoteArgs{
		Term:        requestedTerm,
		CandidateId: rf.me,
	}

	requestVoteArgs.LastLogIndex = rf.absoluteIndex(len(rf.logEntries))
	requestVoteArgs.LastLogTerm = rf.logEntryTerm(requestVoteArgs.LastLogIndex)

	rf.logger.log(debugLogLevel, LockFlow, "[mu][release] requestVoteFromPeers 1")
	rf.mu.Unlock()

	receivedVotes := make([]bool, len(rf.peers))
	votesGranted := 0
	totalVotesReceived := 0
	waitForVote := true
	var voteMut sync.Mutex
	voteCond := sync.NewCond(&voteMut)

	for peerIndex := 0; peerIndex < len(rf.peers); peerIndex++ {
		if peerIndex == rf.me {
			continue
		}

		go func(peerIndex int) {
			for {
				rf.mu.Lock()
				rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] requestVoteFromPeers 2")

				if rf.role != CandidateRole {
					rf.logger.log(debugLogLevel, LockFlow, "[mu][release] requestVoteFromPeers 2")
					rf.mu.Unlock()
					rf.logger.log(errorLogLevel, LeaderElectionFlow, fmt.Sprintf("not candidate, stop requesting vote: peerId=%v requestedTerm=%v", peerIndex, requestedTerm))

					voteMut.Lock()
					waitForVote = false
					voteMut.Unlock()
					voteCond.Signal()
					return
				}

				if rf.currentTerm != requestedTerm {
					rf.logger.log(debugLogLevel, LockFlow, "[mu][release] requestVoteFromPeers 3")
					rf.mu.Unlock()
					rf.logger.log(errorLogLevel, LeaderElectionFlow, fmt.Sprintf("request vote outdated: peerId=%v requestedTerm=%v ", peerIndex, requestedTerm))

					voteMut.Lock()
					waitForVote = false
					voteMut.Unlock()
					voteCond.Signal()
					return
				}

				rf.logger.log(debugLogLevel, LockFlow, "[mu][release] requestVoteFromPeers 4")
				rf.mu.Unlock()

				rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("request vote from peer: peerId=%v requestedTerm=%v", peerIndex, requestedTerm))
				var reply RequestVoteReply
				rpcSucceed := rf.sendRequestVote(peerIndex, &requestVoteArgs, &reply)
				if !rpcSucceed {
					rf.logger.log(errorLogLevel, LeaderElectionFlow, fmt.Sprintf("fail to request vote from peer(RPC): peerId=%v requestedTerm=%v",
						peerIndex,
						requestedTerm))
					continue
				}

				voteMut.Lock()
				if receivedVotes[peerIndex] {
					voteMut.Unlock()
					return
				}

				receivedVotes[peerIndex] = true
				if reply.PeerTerm > requestedTerm {
					rf.logger.log(errorLogLevel, LeaderElectionFlow, fmt.Sprintf("request vote outdated: peerId=%v requestedTerm=%v", peerIndex, requestedTerm))
					waitForVote = false
					voteMut.Unlock()
					voteCond.Signal()
					return
				}

				totalVotesReceived++

				rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("received vote from peer: peerId=%v reply=%v", peerIndex, reply))
				if reply.VoteGranted {
					rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("granted vote from peer: peerId=%v requestedTerm=%v votesGranted=%v totalVotesReceived=%v",
						peerIndex,
						requestedTerm,
						votesGranted,
						totalVotesReceived))
					votesGranted++
				}

				voteMut.Unlock()
				voteCond.Signal()
				return
			}
		}(peerIndex)
	}

	totalVotesRequested := len(rf.peers) - 1
	requiredVoteGrants := totalVotesRequested / 2

	voteMut.Lock()
	defer voteMut.Unlock()
	for waitForVote && votesGranted < requiredVoteGrants && totalVotesReceived < totalVotesRequested {
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("waiting for new incoming vote: requestedTerm=%v votesGranted=%v requiredVoteGrants=%v totalVotesReceived=%v",
			requestedTerm,
			votesGranted,
			requiredVoteGrants,
			totalVotesReceived))
		voteCond.Wait()
	}

	rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("exit requesting votes: requestedTerm=%v waitForVote=%v votesGranted=%v totalVotesReceived=%v totalVotesRequested=%v",
		requestedTerm,
		waitForVote,
		votesGranted,
		totalVotesReceived,
		totalVotesRequested))
	return votesGranted
}

func (rf *Raft) applyCommittedEntries() {
	rf.mu.Lock()
	rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] applyCommittedEntries 1")
	commitLogCh := rf.commitLogCh
	rf.logger.log(debugLogLevel, LockFlow, "[mu][release] applyCommittedEntries 1")
	rf.mu.Unlock()

	for range commitLogCh {
		for {
			rf.mu.Lock()
			rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] applyCommittedEntries 2")
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("processing commits: commitLogIndex=%v lastAppliedLogIndex=%v", rf.commitLogIndex, rf.lastAppliedLogIndex))
			if rf.commitLogIndex <= rf.lastAppliedLogIndex {
				rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("all commits are applied: commitLogIndex=%v lastAppliedLogIndex=%v", rf.commitLogIndex, rf.lastAppliedLogIndex))
				rf.logger.log(debugLogLevel, LockFlow, "[mu][release] applyCommittedEntries 2")
				rf.mu.Unlock()
				break
			}

			relativeIndex := rf.relativeIndex(rf.lastAppliedLogIndex + 1)
			if relativeIndex < 1 {
				rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("log entry is inside snapshot, skipping: lastAppliedLogIndex=%v relativeIndex=%v", rf.lastAppliedLogIndex, relativeIndex))
				rf.logger.log(debugLogLevel, LockFlow, "[mu][release] applyCommittedEntries 3")
				rf.mu.Unlock()
				break
			}

			rf.lastAppliedLogIndex++
			logEntry := rf.logEntries[relativeIndex-1]
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("applying commit: commitLogIndex=%v lastAppliedLogIndex=%v", rf.commitLogIndex, rf.lastAppliedLogIndex))
			rf.logger.log(debugLogLevel, LockFlow, "[mu][release] applyCommittedEntries 4")
			rf.mu.Unlock()

			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Command,
				CommandIndex: logEntry.Index,
			}

			rf.applyCh <- applyMsg
		}
	}
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
	logger := NewLogger(infoLogLevel, map[Flow]bool{
		FollowerFlow:       true,
		CandidateFlow:      true,
		LeaderFlow:         true,
		LeaderElectionFlow: false,
		LogReplicationFlow: true,
		PersistenceFlow:    false,
		SnapshotFlow:       false,
		LockFlow:           false,
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
		replicateLogsCh:          make(chan bool),
		commitLogCh:              make(chan bool),
	}

	rf.logger.log(infoLogLevel, PersistenceFlow, "loading state")
	rf.readPersist(persister.ReadRaftState())
	rf.logger.log(infoLogLevel, FollowerFlow, "start as follower")
	go func() {
		rf.runAsFollower()
	}()

	lastIncludedIndex := rf.snapshotLastIncludedIndex
	lastIncludedTerm := rf.snapshotLastIncludedTerm
	go func() {
		if lastIncludedIndex > 0 {
			snapshotData := rf.persister.ReadSnapshot()

			rf.mu.Lock()
			rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] Make")
			rf.commitLogIndex = lastIncludedIndex
			rf.lastAppliedLogIndex = lastIncludedIndex
			rf.logger.log(debugLogLevel, LockFlow, "[mu][release] Make")
			rf.mu.Unlock()

			applyMsg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapshotData,
				SnapshotIndex: lastIncludedIndex,
				SnapshotTerm:  lastIncludedTerm,
			}
			rf.logger.log(infoLogLevel, SnapshotFlow, fmt.Sprintf("Applying snapshot: applyMsg=%v", applyMsg))
			rf.applyCh <- applyMsg
		}
		rf.applyCommittedEntries()
	}()
	return rf
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
	rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] sendRequestVote 1")
	rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("(%v)[sendRequestVote] send request: peerId=%v currentTerm=%v args=%v", rf.peers[server].Endname, server, rf.currentTerm, args))
	rf.logger.log(debugLogLevel, LockFlow, "[mu][release] sendRequestVote 1")
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] sendRequestVote 2")
	defer rf.mu.Unlock()

	rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("(%v)[sendRequestVote] received response: peerId=%v peerTerm=%v currentTerm=%v reply=%v", rf.peers[server].Endname, server, reply.PeerTerm, rf.currentTerm, reply))
	if reply.PeerTerm > rf.currentTerm {
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[sendRequestVote] request outdated: peerId=%v peerTerm=%v currentTerm=%v", server, reply.PeerTerm, rf.currentTerm))
		rf.currentTerm = reply.PeerTerm
		rf.persist()
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[sendRequestVote] update currentTerm: currentTerm=%v", rf.currentTerm))

		if rf.role != FollowerRole {
			rf.role = FollowerRole
			rf.runAsFollower()
		}

		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] sendRequestVote 2")
		return true
	}

	rf.logger.log(debugLogLevel, LockFlow, "[mu][release] sendRequestVote 3")
	return true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] sendAppendEntries")

	defer rf.mu.Unlock()
	if reply.PeerTerm > rf.currentTerm {
		rf.logger.log(infoLogLevel, LeaderFlow, fmt.Sprintf("[sendAppendEntries] request outdated: peerId=%v, peerTerm=%v, currentTerm=%v", server, reply.PeerTerm, rf.currentTerm))
		rf.currentTerm = reply.PeerTerm
		rf.persist()
		rf.logger.log(infoLogLevel, LeaderFlow, fmt.Sprintf("[sendAppendEntries] update currentTerm: currentTerm=%v", rf.currentTerm))
		if rf.role != FollowerRole {
			rf.role = FollowerRole
			rf.runAsFollower()
		}

		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] sendAppendEntries 1")
		return true
	}

	rf.logger.log(debugLogLevel, LockFlow, "[mu][release] sendAppendEntries 2")
	return true
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	rf.logger.log(debugLogLevel, LockFlow, "[mu][acquire] sendInstallSnapshot")
	defer rf.mu.Unlock()
	if reply.PeerTerm > rf.currentTerm {
		rf.logger.log(infoLogLevel, LeaderFlow, fmt.Sprintf("[sendInstallSnapshot] outdated: peerId=%v, peerTerm=%v, currentTerm=%v", server, reply.PeerTerm, rf.currentTerm))
		rf.currentTerm = reply.PeerTerm
		rf.persist()
		rf.logger.log(infoLogLevel, LeaderFlow, fmt.Sprintf("[sendInstallSnapshot] update currentTerm: currentTerm=%v", rf.currentTerm))
		if rf.role != FollowerRole {
			rf.role = FollowerRole
			rf.runAsFollower()
		}

		rf.logger.log(debugLogLevel, LockFlow, "[mu][release] sendInstallSnapshot 1")
		return true
	}

	rf.logger.log(debugLogLevel, LockFlow, "[mu][release] sendInstallSnapshot 2")
	return true
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
