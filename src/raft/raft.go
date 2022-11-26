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

var roleToFlow = map[Role]Flow{
	LeaderRole:    LeaderFlow,
	FollowerRole:  FollowerFlow,
	CandidateRole: CandidateFlow,
}

const heartbeatInterval = 150 * time.Millisecond
const minElectionTimeout = 250 * time.Millisecond
const electionTimeoutRange = 7

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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (a ApplyMsg) String() string {
	return fmt.Sprintf("[ApplyMsg Command=%v Index=%v]",
		a.Command,
		a.CommandIndex)
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
	currentTerm         int
	votedForCandidateID *int

	// Index starts from 1
	logEntries []LogEntry

	// Volatile on all servers
	role                 Role
	commitLogIndex       int
	lastAppliedLogIndex  int
	receivedValidMessage bool

	// Volatile state on leaders
	nextLogToSendIndices     []int
	lastReplicatedLogIndices []int

	replicateLogsCh       chan bool
	replicateLogsToPeerCh []chan bool
	commitLogCh           chan bool
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
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	err := encoder.Encode(rf.currentTerm)
	if err != nil {
		rf.logger.log(errorLogLevel, PersistenceFlow, fmt.Sprintf("write currentTerm: err=%v", err))
		return
	}

	var votedForCandidateID = -1
	if rf.votedForCandidateID != nil {
		votedForCandidateID = *rf.votedForCandidateID
	}

	err = encoder.Encode(votedForCandidateID)
	if err != nil {
		rf.logger.log(errorLogLevel, PersistenceFlow, fmt.Sprintf("write votedForCandidateID: err=%v", err))
		return
	}

	err = encoder.Encode(rf.logEntries)
	if err != nil {
		rf.logger.log(errorLogLevel, PersistenceFlow, fmt.Sprintf("write logEntries: err=%v", err))
		return
	}

	data := buf.Bytes()
	rf.persister.SaveRaftState(data)

	rf.logger.log(infoLogLevel, PersistenceFlow, "write start")
	rf.logger.log(infoLogLevel, PersistenceFlow, fmt.Sprintf("write currentTerm: currentTerm=%v",
		rf.currentTerm))
	rf.logger.log(infoLogLevel, PersistenceFlow, fmt.Sprintf("write currentTerm: votedForCandidateID=%v",
		intPtrToString(rf.votedForCandidateID)))
	rf.logger.log(infoLogLevel, PersistenceFlow, fmt.Sprintf("write currentTerm: logEntries=%v",
		rf.logEntries))
	rf.logger.log(infoLogLevel, PersistenceFlow, "write end")
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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
	// Your code here (2D).

}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	rf.logger.log(infoLogLevel, roleToFlow[rf.role], fmt.Sprintf("[RequestVote] receive request vote: args=%v",
		args))

	reply.VoteGranted = false
	reply.PeerTerm = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[RequestVote] request outdated: currentTerm=%v peerTerm=%v",
			rf.currentTerm,
			args.Term))
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[RequestVote] enter new term: currentTerm=%v peerTerm=%v",
			rf.currentTerm,
			args.Term))
		rf.enterNewTerm(args.Term)
		rf.persist()
		reply.PeerTerm = rf.currentTerm

		if rf.role != FollowerRole {
			rf.role = FollowerRole
			rf.logger.log(infoLogLevel, LeaderElectionFlow, "[RequestVote] fallback to follower")
			defer rf.runAsFollower()
		}
	}

	if rf.votedForCandidateID != nil && *rf.votedForCandidateID != args.CandidateId {
		rf.receivedValidMessage = true
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[RequestVote] already voted for another candidate: votedFor=%v",
			intPtrToString(rf.votedForCandidateID)))
		rf.mu.Unlock()
		return
	}

	logLen := len(rf.logEntries)
	if logLen > 0 {
		logEntry := rf.logEntries[logLen-1]
		if logEntry.LeaderReceivedTerm > args.LastLogTerm {
			rf.logger.log(errorLogLevel, LeaderElectionFlow, fmt.Sprintf("[RequestVote] candidate log is on old term: candidateId=%v candidateTerm=%v currentTerm=%v candidateLastLogTerm=%v",
				args.CandidateId,
				args.Term,
				logEntry.LeaderReceivedTerm,
				args.LastLogIndex))
			rf.mu.Unlock()
			return
		}

		if logEntry.LeaderReceivedTerm == args.LastLogTerm && logLen > args.LastLogIndex {
			rf.logger.log(errorLogLevel, LeaderElectionFlow, fmt.Sprintf("[RequestVote] candidate log is shorter: candidateId=%v candidateTerm=%v myLogLength=%v candidateLastLogIndex=%v",
				args.CandidateId,
				args.Term,
				logLen,
				args.LastLogIndex))
			rf.mu.Unlock()
			return
		}
	}

	rf.receivedValidMessage = true
	rf.votedForCandidateID = &args.CandidateId
	rf.persist()
	rf.mu.Unlock()
	reply.VoteGranted = true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] receive append entries: args=%v",
		args))

	rf.mu.Lock()
	reply.Success = false
	reply.PeerTerm = rf.currentTerm
	if rf.killed() {
		rf.mu.Unlock()
		return
	}

	if args.LeaderTerm < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	rf.receivedValidMessage = true
	if args.LeaderTerm > rf.currentTerm {
		rf.logger.log(infoLogLevel, roleToFlow[rf.role], fmt.Sprintf("[AppendEntries] update term: leaderTerm=%v currentTerm=%v",
			args.LeaderTerm,
			rf.currentTerm))
		rf.enterNewTerm(args.LeaderTerm)
		rf.persist()
		reply.PeerTerm = rf.currentTerm
	}

	if rf.role != FollowerRole {
		rf.role = FollowerRole
		defer rf.runAsFollower()
		rf.logger.log(infoLogLevel, roleToFlow[rf.role], "[AppendEntries] convert to follower")
	}

	if len(rf.logEntries) < args.PrevLogIndex {
		rf.logger.log(errorLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] prevLogIndex exceeds last log entry: leaderPrevLogIndex=%v logEntries=%v",
			args.PrevLogIndex,
			rf.logEntries))
		reply.ConflictTerm = rf.findTermBeforeOrEqual(args.PrevLogTerm)
		rf.mu.Unlock()
		return
	}

	if args.PrevLogIndex > 0 {
		prevLogEntry := rf.logEntries[args.PrevLogIndex-1]
		if prevLogEntry.LeaderReceivedTerm != args.PrevLogTerm {
			rf.logger.log(errorLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] previous log entry term mismatch: prevLogIndex=%v prevLogTerm=%v expectedPrevLogTerm=%v logEntries=%v",
				args.PrevLogIndex,
				prevLogEntry.LeaderReceivedTerm,
				args.PrevLogTerm,
				rf.logEntries))
			reply.ConflictTerm = rf.findTermBeforeOrEqual(args.PrevLogTerm)
			rf.mu.Unlock()
			return
		}
	}

	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] current log entries: logEntries=%v",
		rf.logEntries))
	newEntriesStartIndex := 0
	updated := false

	for ; newEntriesStartIndex < len(args.Entries); newEntriesStartIndex++ {
		newEntry := args.Entries[newEntriesStartIndex]
		if len(rf.logEntries) < newEntry.Index {
			break
		}

		oldEntry := rf.logEntries[newEntry.Index-1]
		if oldEntry.LeaderReceivedTerm != newEntry.LeaderReceivedTerm {
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] removing conflicting entries: newEntryStartIndex=%v oldEntry=%v newEntry=%v",
				newEntriesStartIndex,
				oldEntry,
				newEntry))
			rf.logEntries = rf.logEntries[:newEntry.Index-1]
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] removed conflicting entries: currentEntries=%v",
				rf.logEntries))
			rf.persist()
			updated = true
			break
		}
	}

	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] processing newEntries: newEntryStartIndex=%v",
		newEntriesStartIndex))
	if newEntriesStartIndex < len(args.Entries) {
		rf.logEntries = append(rf.logEntries, args.Entries[newEntriesStartIndex:]...)
		rf.persist()
		updated = true
	}

	if updated {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] updated log entries: updatedLogEntries=%v",
			rf.logEntries))
	} else {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] no change to log entries: logEntries=%v",
			rf.logEntries))
	}

	if args.LeaderCommitIndex > rf.commitLogIndex {
		rf.commitLogIndex = args.LeaderCommitIndex
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] commit entries: commitLogIndex=%v",
			rf.commitLogIndex))
		commitLogCh := rf.commitLogCh
		go func() {
			commitLogCh <- true
		}()
	}

	rf.mu.Unlock()
	reply.Success = true
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

	return 0
}

func (rf *Raft) findIndexOfFirstLogEntry(term int) int {
	if term == 0 {
		return 0
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
	defer rf.mu.Unlock()

	if rf.killed() {
		return 0, rf.currentTerm, rf.role == LeaderRole
	}

	if rf.role != LeaderRole {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("only leader can receive client command: command=%v",
			command))
		return 0, rf.currentTerm, false
	}

	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("received command from client: command=%v",
		command))
	nextLogIndex := len(rf.logEntries) + 1
	logEntry := LogEntry{
		Command:            command,
		Index:              nextLogIndex,
		LeaderReceivedTerm: rf.currentTerm,
	}
	rf.logEntries = append(rf.logEntries, logEntry)
	rf.persist()
	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("append entry to leader log: logEntry=%v",
		logEntry))

	replicateLogsCh := rf.replicateLogsCh
	go func() {
		replicateLogsCh <- true
	}()

	return nextLogIndex, rf.currentTerm, true
}

func (rf *Raft) replicateLogEntries() {
	rf.mu.Lock()
	requiredPeers := len(rf.peers) / 2
	lastLogIndexToReplicate := len(rf.logEntries)
	leaderCommitLogIndex := rf.commitLogIndex
	replicateEntriesTerm := rf.currentTerm
	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("replicate log entries to all peers: requiredPeers=%v lastLogIndexToReplicate=%v logEntries=%v",
		requiredPeers,
		lastLogIndexToReplicate,
		rf.logEntries))
	rf.mu.Unlock()

	var replicateLogsMut sync.Mutex
	replicateLogsCond := sync.NewCond(&replicateLogsMut)
	replicatedPeerCount := 0
	for peerId := 0; peerId < len(rf.peers) && !rf.killed(); peerId++ {
		if peerId == rf.me {
			continue
		}

		rf.mu.Lock()
		if rf.role != LeaderRole {
			rf.logger.log(infoLogLevel, LogReplicationFlow, "not leader anymore when replicating log")
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()

		go func(peerId int) {
			rf.replicateLogEntriesToPeer(peerId, lastLogIndexToReplicate, leaderCommitLogIndex)
			replicateLogsMut.Lock()
			replicatedPeerCount++
			replicateLogsCond.Signal()
			replicateLogsMut.Unlock()
		}(peerId)
	}

	replicateLogsMut.Lock()
	for replicatedPeerCount < requiredPeers {
		replicateLogsCond.Wait()
	}

	replicateLogsMut.Unlock()

	if rf.killed() {
		return
	}

	rf.mu.Lock()
	if rf.currentTerm != replicateEntriesTerm {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("log replication is outdated: replicatingTerm=%v currentTerm=%v",
			replicateEntriesTerm,
			rf.currentTerm))
		rf.mu.Unlock()
		return
	}

	if rf.role != LeaderRole {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("not leader any more, stopped replicating log to peers: role=%v",
			rf.role))
		rf.mu.Unlock()
		return
	}

	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("replicated log entries to peers: lastLogIndexToReplicate=%v",
		lastLogIndexToReplicate))
	if lastLogIndexToReplicate <= 0 {
		rf.logger.log(infoLogLevel, LogReplicationFlow, "no entry is replicated, skip committing")
		rf.mu.Unlock()
		return
	}

	if rf.logEntries[lastLogIndexToReplicate-1].LeaderReceivedTerm != rf.currentTerm {
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("last replicated entry not from current term, skip committing: lastEntryTerm=%v, currentTerm=%v",
			rf.logEntries[lastLogIndexToReplicate-1].LeaderReceivedTerm,
			rf.currentTerm))
		rf.mu.Unlock()
		return
	}

	if lastLogIndexToReplicate > rf.commitLogIndex {
		oldCommitLogIndex := rf.commitLogIndex
		rf.commitLogIndex = lastLogIndexToReplicate
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("commit log entries: oldCommitLogIndex=%v newCommitLogIndex=%v",
			oldCommitLogIndex,
			rf.commitLogIndex))
		commitLogCh := rf.commitLogCh
		go func() {
			commitLogCh <- true
		}()
	}

	rf.mu.Unlock()
}

func (rf *Raft) replicateLogEntriesToPeer(peerId int, lastLogIndexToReplicate int, leaderCommitLogIndex int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != LeaderRole {
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("not leader any more, stop replicating log to peer: role=%v peerId=%v",
				rf.role,
				peerId))
			rf.mu.Unlock()
			return
		}

		if rf.lastReplicatedLogIndices[peerId] > lastLogIndexToReplicate {
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("log replication outdated: peerId=%v lastReplicatedLogIndex=%v lastLogIndexToReplicate=%v",
				peerId,
				rf.lastReplicatedLogIndices[peerId],
				lastLogIndexToReplicate))
			rf.mu.Unlock()
			return
		}

		nextLogIndex := rf.nextLogToSendIndices[peerId]
		prevLogTerm := 0
		if nextLogIndex > 1 {
			prevLogTerm = rf.logEntries[nextLogIndex-2].LeaderReceivedTerm
		}

		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("replicate log entries to peer: peerId=%v lastLogIndexToReplicate=%v nextLogIndex=%v",
			peerId,
			lastLogIndexToReplicate,
			nextLogIndex))
		logEntriesCopy := make([]LogEntry, lastLogIndexToReplicate-nextLogIndex+1)
		copy(logEntriesCopy, rf.logEntries[nextLogIndex-1:lastLogIndexToReplicate])

		args := AppendEntriesArgs{
			LeaderTerm:        rf.currentTerm,
			LeaderId:          rf.me,
			PrevLogIndex:      nextLogIndex - 1,
			PrevLogTerm:       prevLogTerm,
			Entries:           logEntriesCopy,
			LeaderCommitIndex: leaderCommitLogIndex,
		}

		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("replicate log entries to peer: peerId=%v args=%v",
			peerId,
			args))
		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		requestSucceed := rf.sendAppendEntries(peerId, &args, &reply)

		rf.mu.Lock()
		if rf.role != LeaderRole {
			rf.mu.Unlock()
			return
		}

		if rf.currentTerm != args.LeaderTerm {
			rf.mu.Unlock()
			return
		}

		if requestSucceed {
			if reply.Success {
				rf.nextLogToSendIndices[peerId] = lastLogIndexToReplicate + 1
				rf.lastReplicatedLogIndices[peerId] = lastLogIndexToReplicate
				rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("replicated log entries to peer: peerId=%v nextLogIndex=%v lastReplicatedLogIndex=%v",
					peerId,
					rf.nextLogToSendIndices[peerId],
					rf.lastReplicatedLogIndices[peerId]))
				rf.mu.Unlock()
				return
			}

			rf.logger.log(errorLogLevel, LogReplicationFlow, fmt.Sprintf("fail to replicate log entries to peer: peerId=%v nextLogIndex=%v conflictTerm=%v",
				peerId,
				rf.nextLogToSendIndices[peerId],
				reply.ConflictTerm))

			conflictTerm := reply.ConflictTerm
			if conflictTerm != prevLogTerm {
				conflictTerm = rf.findTermBeforeOrEqual(reply.ConflictTerm)
			}

			firstEntryIndex := rf.findIndexOfFirstLogEntry(conflictTerm)
			rf.logger.log(errorLogLevel, LogReplicationFlow, fmt.Sprintf("before update nextIndex: peerId=%v conflictTerm=%v firstEntryIndex=%v logEntries=%v",
				peerId,
				conflictTerm,
				firstEntryIndex,
				rf.logEntries,
			))

			if firstEntryIndex == nextLogIndex {
				firstEntryIndex = firstEntryIndex - 1
			}

			firstEntryIndex = max(rf.lastReplicatedLogIndices[peerId]+1, firstEntryIndex)
			rf.logger.log(errorLogLevel, LogReplicationFlow, fmt.Sprintf("update nextIndex: peerId=%v conflictTerm=%v firstEntryIndex=%v logEntries=%v",
				peerId,
				conflictTerm,
				firstEntryIndex,
				rf.logEntries,
			))
			rf.nextLogToSendIndices[peerId] = firstEntryIndex
		} else {
			rf.logger.log(errorLogLevel, LogReplicationFlow, fmt.Sprintf("fail to replicate log entries to peer(RPC): peerId=%v nextLogIndex=%v",
				peerId,
				rf.nextLogToSendIndices[peerId]))
		}

		rf.mu.Unlock()
	}
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
	rf.logger.log(infoLogLevel, FollowerFlow, "run as follower in background")
	go func() {
		for !rf.killed() {
			rf.logger.log(infoLogLevel, FollowerFlow, "wait for election timeout")
			rf.waitForElectionTimeout()

			rf.mu.Lock()
			rf.logger.log(infoLogLevel, FollowerFlow, fmt.Sprintf("follower election timeout: role=%v", rf.role))
			if rf.role != FollowerRole {
				rf.mu.Unlock()
				rf.logger.log(infoLogLevel, FollowerFlow, "exist follower flow")
				return
			}

			if rf.receivedValidMessage {
				rf.logger.log(infoLogLevel, FollowerFlow, "received message from peer")
				rf.receivedValidMessage = false
				rf.mu.Unlock()
				continue
			}

			rf.role = CandidateRole
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
	rf.logger.log(infoLogLevel, CandidateFlow, "run as candidate in background")
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			if rf.role != CandidateRole {
				rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("not candidate any more, exit election: role=%v", rf.role))
				rf.mu.Unlock()
				return
			}

			rf.currentTerm++
			rf.votedForCandidateID = &rf.me
			rf.persist()
			currentTerm := rf.currentTerm
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
				rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("electionTerm=%v, currentTerm=%v",
					electionTerm,
					rf.currentTerm))
				if electionTerm < rf.currentTerm {
					// new election started after election timeout
					rf.logger.log(infoLogLevel, LeaderElectionFlow, "votes outdated")
					rf.mu.Unlock()
					return
				}

				if rf.role != CandidateRole {
					// received heartbeat from new leader & fallback to follower
					rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("not candidate, abandoned vote result: role=%s", rf.role))
					rf.mu.Unlock()
					return
				}

				if grantedVotes > halfVotes {
					rf.logger.log(infoLogLevel, LeaderElectionFlow, "received enough votes, elected as leader")
					rf.role = LeaderRole
					rf.mu.Unlock()
					rf.runAsLeader()
					return
				}

				if grantedVotes < halfVotes {
					rf.logger.log(infoLogLevel, LeaderElectionFlow, "not enough votes, fallback to follower")
					rf.role = FollowerRole
					rf.mu.Unlock()
					rf.runAsFollower()
					return
				}

				rf.mu.Unlock()
				rf.logger.log(infoLogLevel, LeaderElectionFlow, "split vote, no leader")
			}(currentTerm)

			rf.logger.log(infoLogLevel, LeaderElectionFlow, "wait for election timeout")
			rf.waitForElectionTimeout()

			rf.mu.Lock()
			rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("candidate election timeout: role=%v", rf.role))
			rf.mu.Unlock()
		}

		if rf.killed() {
			rf.logger.log(infoLogLevel, CandidateFlow, "candidate is killed")
		}
	}()
}

func (rf *Raft) waitForElectionTimeout() {
	randTimeout := time.Duration(rand.Intn(electionTimeoutRange)) * 20 * time.Millisecond
	electionTimeout := randTimeout + minElectionTimeout
	time.Sleep(electionTimeout)
}

func (rf *Raft) runAsLeader() {
	rf.mu.Lock()
	rf.logger.log(infoLogLevel, LeaderFlow, fmt.Sprintf("running as leader: term=%v", rf.currentTerm))

	numPeers := len(rf.peers)
	for peerId := 0; peerId < numPeers; peerId++ {
		rf.nextLogToSendIndices[peerId] = len(rf.logEntries) + 1
	}

	rf.lastReplicatedLogIndices = make([]int, numPeers)
	replicateLogsCh := rf.replicateLogsCh
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
			if rf.role != LeaderRole {
				rf.logger.log(infoLogLevel, LogReplicationFlow, "replicate is not leader anymore")
				rf.mu.Unlock()
				return
			}

			rf.mu.Unlock()
			rf.replicateLogEntries()
		}
	}()
}

func (rf *Raft) sendPeriodicHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != LeaderRole {
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()
		go func() {
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
	requestVoteArgs := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	if len(rf.logEntries) > 0 {
		logLen := len(rf.logEntries)
		lastLog := rf.logEntries[logLen-1]
		requestVoteArgs.LastLogIndex = lastLog.Index
		requestVoteArgs.LastLogTerm = lastLog.LeaderReceivedTerm
	}

	rf.mu.Unlock()

	votesGranted := 1
	totalVotesReceived := 0
	var voteMut sync.Mutex
	voteCond := sync.NewCond(&voteMut)
	for peerIndex := 0; peerIndex < len(rf.peers); peerIndex++ {
		if peerIndex == rf.me {
			continue
		}

		go func(peerIndex int) {
			rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("request vote from peer: peerId=%v", peerIndex))
			var reply RequestVoteReply

			succeed := rf.sendRequestVote(peerIndex, &requestVoteArgs, &reply)
			if !succeed {
				rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("fail to request vote from peer(RPC): peerId=%v", peerIndex))
				time.Sleep(10 * time.Millisecond)
				return
			}

			defer voteMut.Unlock()
			defer voteCond.Signal()

			voteMut.Lock()
			totalVotesReceived++
			rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("received vote from peer: peerId=%v reply=%v", peerIndex, reply))
			if reply.VoteGranted {
				rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("granted vote from peer: peerId=%v, votesGranted=%v, totalVotesReceived=%v",
					peerIndex,
					votesGranted,
					totalVotesReceived))
				votesGranted++
			}
		}(peerIndex)
	}

	totalVotes := len(rf.peers)
	requiredVoteGrants := totalVotes/2 + 1

	voteMut.Lock()
	defer voteMut.Unlock()
	for votesGranted < requiredVoteGrants && totalVotesReceived < totalVotes {
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("waiting for new incoming vote: votesGranted=%v requiredVoteGrants=%v totalVotesReceived=%v",
			votesGranted,
			requiredVoteGrants,
			totalVotesReceived))
		voteCond.Wait()
	}

	rf.logger.log(infoLogLevel, LeaderElectionFlow, "collected all votes or granted enough votes")
	return votesGranted
}

func (rf *Raft) applyCommittedEntries() {
	rf.mu.Lock()
	commitLogCh := rf.commitLogCh
	rf.mu.Unlock()

	for range commitLogCh {
		for {
			rf.mu.Lock()
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("processing commit entries: commitLogIndex=%v lastAppliedLogIndex=%v", rf.commitLogIndex, rf.lastAppliedLogIndex))
			if rf.commitLogIndex <= rf.lastAppliedLogIndex {
				rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("commits are applied: commitLogIndex=%v lastAppliedLogIndex=%v", rf.commitLogIndex, rf.lastAppliedLogIndex))
				rf.mu.Unlock()
				break
			}

			rf.lastAppliedLogIndex++
			logEntry := rf.logEntries[rf.lastAppliedLogIndex-1]
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
	logger := NewLogger(offLogLevel, map[Flow]bool{
		FollowerFlow:       true,
		CandidateFlow:      true,
		LeaderFlow:         true,
		LeaderElectionFlow: true,
		LogReplicationFlow: true,
		PersistenceFlow:    true,
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

	rf.readPersist(persister.ReadRaftState())
	rf.logger.log(infoLogLevel, FollowerFlow, "start as follower")
	go func() {
		rf.runAsFollower()
	}()
	go func() {
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	if reply.PeerTerm > rf.currentTerm {
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("sendRequestVote outdated: peerId=%v, peerTerm=%v, currentTerm=%v", server, reply.PeerTerm, rf.currentTerm))
		rf.currentTerm = reply.PeerTerm
		rf.persist()
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("update currentTerm: currentTerm=%v", rf.currentTerm))
		rf.role = FollowerRole
		rf.mu.Unlock()
		rf.runAsFollower()
		return true
	}

	rf.mu.Unlock()
	return true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	if reply.PeerTerm > rf.currentTerm {
		rf.logger.log(infoLogLevel, LeaderFlow, fmt.Sprintf("sendAppendEntries outdated: peerId=%v, peerTerm=%v, currentTerm=%v", server, reply.PeerTerm, rf.currentTerm))
		rf.currentTerm = reply.PeerTerm
		rf.persist()
		rf.logger.log(infoLogLevel, LeaderFlow, fmt.Sprintf("update currentTerm: currentTerm=%v", rf.currentTerm))
		rf.role = FollowerRole
		rf.mu.Unlock()
		rf.runAsFollower()
		return true
	}

	rf.mu.Unlock()
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
