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
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

var roleToFlow = map[Role]Flow{
	LeaderRole:    LeaderFlow,
	FollowerRole:  FollowerFlow,
	CandidateRole: CandidateFlow,
}

const heartbeatInterval = 150 * time.Millisecond
const minElectionTimeout = 250 * time.Millisecond
const electionTimeoutRangeMillis = 150

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
	return fmt.Sprintf("[ApplyMsg Command=%v Index=%v]", a.Command, a.CommandIndex)
}

type LogEntry struct {
	Command            interface{}
	Index              int
	LeaderReceivedTerm int
}

func (l LogEntry) String() string {
	return fmt.Sprintf("[LogEntry Command=%v Index=%v LeaderReceivedTerm=%v]", l.Command, l.Index, l.LeaderReceivedTerm)
}

type Role string

const (
	FollowerRole  Role = "Follower"
	CandidateRole Role = "Candidate"
	LeaderRole    Role = "Leader"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	logger    Logger
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent on all servers
	currentTerm         int
	votedForCandidateID *int

	// Index starts from 1
	logEntries []LogEntry

	// Volatile on all servers
	role                      Role
	commitLogIndex            int
	lastAppliedLogIndex       int
	receivedMessageFromLeader bool

	// Volatile state on leaders
	nextLogToSendIndices     []int
	lastReplicatedLogIndices []int

	replicateLogsCh             chan bool
	commitLogCh                 chan bool
	replicateLogsChUseWaitGroup sync.WaitGroup
	commitLogChUseWaitGroup     sync.WaitGroup
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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	rf.logger.log(infoLogLevel, roleToFlow[rf.role], fmt.Sprintf("receive request vote: args=%v", args))

	reply.VoteGranted = false
	reply.PeerTerm = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[RequestVote] request outdated: currentTerm=%v peerTerm=%v", rf.currentTerm, args.Term))
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[RequestVote] enter new term: currentTerm=%v peerTerm=%v", rf.currentTerm, args.Term))
		rf.enterNewTerm(args.Term)
		reply.PeerTerm = rf.currentTerm

		if rf.role != FollowerRole {
			rf.role = FollowerRole
			rf.logger.log(infoLogLevel, LeaderElectionFlow, "[RequestVote] fallback to follower")
			defer rf.runAsFollower()
		}
	}

	if rf.votedForCandidateID != nil && *rf.votedForCandidateID != args.CandidateId {
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[RequestVote] already voted for another candidate: votedFor=%v", rf.votedForCandidateID))
		rf.mu.Unlock()
		return
	}

	logLen := len(rf.logEntries)
	if logLen > 0 {
		logEntry := rf.logEntries[logLen-1]
		if logEntry.LeaderReceivedTerm > args.LastLogTerm {
			rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[RequestVote] candidate is on old term: currentTerm=%v candidateLastLogTerm=%v", logEntry.LeaderReceivedTerm, args.LastLogIndex))
			rf.mu.Unlock()
			return
		}

		if logEntry.LeaderReceivedTerm == args.LastLogTerm && logLen > args.LastLogIndex {
			rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("[RequestVote] candidate has shorter log: myLogLength=%v candidateLastLogIndex=%v", logLen, args.LastLogIndex))
			rf.mu.Unlock()
			return
		}
	}

	rf.votedForCandidateID = &args.CandidateId
	rf.mu.Unlock()
	reply.VoteGranted = true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] receive append entries: args=%v", args))

	reply.Success = false
	reply.PeerTerm = rf.currentTerm
	if args.LeaderTerm < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.LeaderTerm > rf.currentTerm {
		rf.logger.log(infoLogLevel, roleToFlow[rf.role], fmt.Sprintf("[AppendEntries] update term: leaderTerm=%v currentTerm=%v", args.LeaderTerm, rf.currentTerm))
		rf.enterNewTerm(args.LeaderTerm)
		reply.PeerTerm = rf.currentTerm
	}

	rf.receivedMessageFromLeader = true

	if rf.role != FollowerRole {
		rf.role = FollowerRole
		defer rf.runAsFollower()
		rf.logger.log(infoLogLevel, roleToFlow[rf.role], "[AppendEntries] convert to follower")
	}

	if len(rf.logEntries) < args.PrevLogIndex {
		rf.logger.log(infoLogLevel, LogReplicationFlow, "[AppendEntries] prevLogIndex exceeds last log entry")
		rf.mu.Unlock()
		return
	}

	if args.PrevLogIndex > 0 {
		prevLogEntry := rf.logEntries[args.PrevLogIndex-1]
		if rf.logEntries[args.PrevLogIndex-1].LeaderReceivedTerm != args.PrevLogTerm {
			rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] previous log entry term mismatch: prevLogEntry=%v expectedPrevLogTerm=%v", prevLogEntry, args.PrevLogTerm))
			rf.mu.Unlock()
			return
		}
	}

	rf.logEntries = rf.logEntries[:args.PrevLogIndex]
	rf.logEntries = append(rf.logEntries, args.Entries...)
	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] received log entries: logEntries=%v", args.Entries))

	if args.LeaderCommitIndex > rf.commitLogIndex {
		commitLogIndex := args.LeaderCommitIndex
		if len(args.Entries) > 0 {
			commitLogIndex = min(commitLogIndex, args.Entries[len(args.Entries)-1].Index)
		}

		rf.commitLogIndex = commitLogIndex
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("[AppendEntries] commit entries: commitLogIndex=%v", commitLogIndex))
		rf.commitLogChUseWaitGroup.Add(1)
		go func() {
			defer rf.commitLogChUseWaitGroup.Done()
			rf.commitLogCh <- true
		}()
	}

	rf.mu.Unlock()
	reply.Success = true
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
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("only leader can receive client command: command=%v", command))
		return 0, rf.currentTerm, false
	}

	nextLogIndex := len(rf.logEntries) + 1
	logEntry := LogEntry{
		Command:            command,
		Index:              nextLogIndex,
		LeaderReceivedTerm: rf.currentTerm,
	}
	rf.logEntries = append(rf.logEntries, logEntry)
	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("append entry to log: logEntry=%v", logEntry))
	rf.replicateLogsChUseWaitGroup.Add(1)
	go func() {
		defer rf.replicateLogsChUseWaitGroup.Done()
		rf.replicateLogsCh <- true
	}()

	return nextLogIndex, rf.currentTerm, true
}

func (rf *Raft) replicateLogEntries() {
	rf.mu.Lock()
	requiredPeers := len(rf.peers) / 2
	lastLogIndexToReplicate := len(rf.logEntries)
	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("replicate log entries to all peers: requiredPeers=%v lastLogIndexToReplicate=%v", requiredPeers, lastLogIndexToReplicate))
	rf.mu.Unlock()

	var replicateLogsMut sync.Mutex
	replicateLogsCond := sync.NewCond(&replicateLogsMut)
	replicatedPeerCount := 0
	for peerId := 0; peerId < len(rf.peers) && !rf.killed(); peerId++ {
		rf.mu.Lock()
		if rf.role != LeaderRole {
			rf.logger.log(infoLogLevel, LogReplicationFlow, "not leader anymore when replicating log")
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()
		if peerId == rf.me {
			continue
		}

		go func(peerId int) {
			rf.replicateLogEntriesToPeer(peerId, lastLogIndexToReplicate)

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

	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("replicated log entries to peers: lastLogIndexToReplicate=%v", lastLogIndexToReplicate))
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	if rf.role != LeaderRole {
		rf.mu.Unlock()
		return
	}

	if lastLogIndexToReplicate > rf.commitLogIndex {
		oldCommitLogIndex := rf.commitLogIndex
		rf.commitLogIndex = lastLogIndexToReplicate
		rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("commit log entries: oldCommitLogIndex=%v newCommitLogIndex=%v", oldCommitLogIndex, rf.commitLogIndex))
		go func() {
			rf.commitLogCh <- true
		}()
	}

	rf.mu.Unlock()
}

func (rf *Raft) replicateLogEntriesToPeer(peerId int, lastLogIndexToReplicate int) {
	rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("replicate log entries to peer: peerId=%v lastLogIndexToReplicate=%v", peerId, lastLogIndexToReplicate))
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != LeaderRole {
			rf.mu.Unlock()
			return
		}

		nextLogIndex := rf.nextLogToSendIndices[peerId]
		prevLogTerm := 0
		if nextLogIndex > 1 {
			prevLogTerm = rf.logEntries[nextLogIndex-2].LeaderReceivedTerm
		}

		logEntriesCopy := make([]LogEntry, len(rf.logEntries)-nextLogIndex+1)
		copy(logEntriesCopy, rf.logEntries[nextLogIndex-1:])

		args := AppendEntriesArgs{
			LeaderTerm:        rf.currentTerm,
			LeaderId:          rf.me,
			PrevLogIndex:      nextLogIndex - 1,
			PrevLogTerm:       prevLogTerm,
			Entries:           logEntriesCopy,
			LeaderCommitIndex: rf.commitLogIndex,
		}

		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		requestSucceed := rf.sendAppendEntries(peerId, &args, &reply)

		rf.mu.Lock()
		if rf.role != LeaderRole {
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

			rf.nextLogToSendIndices[peerId]--
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
	go func() {
		rf.replicateLogsChUseWaitGroup.Wait()
		rf.commitLogChUseWaitGroup.Wait()
		close(rf.replicateLogsCh)
		close(rf.commitLogCh)
	}()
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
			rf.logger.log(infoLogLevel, FollowerFlow, fmt.Sprintf("election timeout: role=%v", rf.role))
			if rf.role != FollowerRole {
				rf.mu.Unlock()
				rf.logger.log(infoLogLevel, FollowerFlow, "exist follower flow")
				return
			}

			if rf.receivedMessageFromLeader {
				rf.logger.log(infoLogLevel, FollowerFlow, "received message from leader")
				rf.receivedMessageFromLeader = false
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
}

func (rf *Raft) runAsCandidate() {
	rf.logger.log(infoLogLevel, CandidateFlow, "run as candidate in background")
	go func() {
		for !rf.killed() {
			rf.mu.Lock()

			rf.currentTerm++
			rf.votedForCandidateID = &rf.me
			currentTerm := rf.currentTerm
			rf.mu.Unlock()
			rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("start election: newTerm=%v", currentTerm))

			halfVotes := len(rf.peers) / 2
			go func(electionTerm int) {
				rf.logger.log(infoLogLevel, LeaderElectionFlow, "request votes from peers")
				grantedVotes := rf.requestVoteFromPeers() + 1
				rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("received votes from peers: grantedVotes=%v, requiredVotes=%v", grantedVotes, halfVotes+1))

				rf.mu.Lock()
				rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("electionTerm=%v, currentTerm=%v", electionTerm, rf.currentTerm))
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
			rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("election timeout: role=%v", rf.role))
			if rf.role != CandidateRole {
				rf.mu.Unlock()
				return
			}

			rf.mu.Unlock()
		}

		if rf.killed() {
			rf.logger.log(infoLogLevel, CandidateFlow, "candidate is killed")
		}
	}()
}

func (rf *Raft) waitForElectionTimeout() {
	randTimeoutMillis := rand.Intn(electionTimeoutRangeMillis)
	electionTimeout := time.Duration(randTimeoutMillis)*time.Millisecond + minElectionTimeout
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
	rf.mu.Unlock()

	go func() {
		rf.sendPeriodicHeartbeats()
	}()

	go func() {
		for range rf.replicateLogsCh {
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
		rf.replicateLogsChUseWaitGroup.Add(1)
		go func() {
			defer rf.replicateLogsChUseWaitGroup.Done()
			rf.replicateLogsCh <- true
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
			rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("received vote from peer: peerId=%v reply=%v", peerIndex, reply))

			voteMut.Lock()
			defer voteCond.Signal()
			defer voteMut.Unlock()
			totalVotesReceived++

			if !succeed {
				rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("fail to request vote from peer: peerId=%v", peerIndex))
				return
			}

			if reply.VoteGranted {
				rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("granted vote from peer: peerId=%v, votesGranted=%v, totalVotesReceived=%v", peerIndex, votesGranted, totalVotesReceived))
				votesGranted++
			}
		}(peerIndex)
	}

	totalVotes := len(rf.peers)
	requiredVoteGrants := totalVotes/2 + 1

	voteMut.Lock()
	defer voteMut.Unlock()
	for votesGranted < requiredVoteGrants && totalVotesReceived < totalVotes {
		rf.logger.log(infoLogLevel, LeaderElectionFlow, "waiting for new incoming vote")
		voteCond.Wait()
	}

	rf.logger.log(infoLogLevel, LeaderElectionFlow, "collected all votes or granted enough votes")
	return votesGranted
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
	}, me)

	// Your initialization code here (2A, 2B, 2C).
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.logger.log(infoLogLevel, FollowerFlow, "start as follower")
	go func() {
		rf.runAsFollower()
	}()
	go func() {
		for range rf.commitLogCh {
			for {
				rf.mu.Lock()
				rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("processing commit entries: commitLogIndex=%v lastAppliedLogIndex=%v", rf.commitLogIndex, rf.lastAppliedLogIndex))
				if rf.commitLogIndex <= rf.lastAppliedLogIndex {
					rf.mu.Unlock()
					break
				}

				rf.lastAppliedLogIndex++
				logEntry := rf.logEntries[rf.lastAppliedLogIndex-1]
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      logEntry.Command,
					CommandIndex: logEntry.Index,
				}
				rf.mu.Unlock()

				rf.logger.log(infoLogLevel, LogReplicationFlow, fmt.Sprintf("applying committed log entry: %v", applyMsg))
				rf.applyCh <- applyMsg
			}
		}
	}()
	return rf
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
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
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	if reply.PeerTerm > rf.currentTerm {
		rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("sendRequestVote outdated: peerId=%v, peerTerm=%v, currentTerm=%v", server, reply.PeerTerm, rf.currentTerm))
		rf.currentTerm = reply.PeerTerm
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
