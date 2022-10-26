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

type LogEntry struct {
	Command            interface{}
	LeaderReceivedTerm int
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent on all servers
	currentTerm         int
	votedForCandidateID *int
	logEntries          []LogEntry

	// Volatile on all servers
	role                Role
	commitLogIndex      *int
	lastAppliedLogIndex *int
	receivedMessage     bool

	// Volatile state on leaders
	nextLogToSendIndices     []int
	lastReplicatedLogIndices []*int
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
	rf.logger.log(infoLogLevel, roleToFlow[rf.role], fmt.Sprintf("receive request vote: candidateID=%v, leaderTerm=%v", args.CandidateId, args.Term))

	// Your code here (2A, 2B).
	reply.VoteGranted = false
	reply.PeerTerm = rf.currentTerm
	if args.Term < rf.currentTerm {
		// Outdated request
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.enterNewTerm(args.Term)
		reply.PeerTerm = rf.currentTerm

		if rf.role != FollowerRole {
			rf.receivedMessage = true
			rf.role = FollowerRole
			defer rf.runAsFollower()
		}
	}

	if rf.votedForCandidateID != nil && *rf.votedForCandidateID != args.CandidateId {
		rf.receivedMessage = true
		// Already voted for another candidate
		rf.mu.Unlock()
		return
	}

	if args.LastLogIndex == nil {
		rf.receivedMessage = true
		rf.votedForCandidateID = &args.CandidateId
		reply.VoteGranted = true
		rf.mu.Unlock()
		return
	}

	if *args.LastLogIndex >= len(rf.logEntries) || rf.logEntries[*args.LastLogIndex].LeaderReceivedTerm != *args.LastLogTerm {
		rf.mu.Unlock()
		return
	}

	rf.receivedMessage = true
	rf.votedForCandidateID = &args.CandidateId
	rf.mu.Unlock()
	reply.VoteGranted = true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.logger.log(infoLogLevel, roleToFlow[rf.role], fmt.Sprintf("receive append entries: leaderID=%v, leaderTerm=%v", args.LeaderId, args.LeaderTerm))

	// Your code here (2A, 2B).
	reply.Success = false
	reply.PeerTerm = rf.currentTerm
	if args.LeaderTerm < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.LeaderTerm >= rf.currentTerm {
		if args.LeaderTerm > rf.currentTerm {
			rf.enterNewTerm(args.LeaderTerm)
			reply.PeerTerm = rf.currentTerm
		}

		if rf.role != FollowerRole {
			rf.receivedMessage = true
			rf.role = FollowerRole
			defer rf.runAsFollower()
		}
	}

	if args.PrevLogIndex == nil || args.PrevLogTerm == nil {
		rf.receivedMessage = true
		reply.Success = true
		rf.mu.Unlock()
		return
	}

	if len(rf.logEntries) < *args.PrevLogIndex {
		rf.mu.Unlock()
		return
	}

	if rf.logEntries[*args.PrevLogIndex].LeaderReceivedTerm != *args.PrevLogTerm {
		rf.mu.Unlock()
		return
	}

	rf.receivedMessage = true
	firstLogEntryIndex := *args.PrevLogIndex + 1
	newLogEntryRelativeIndex := 0
	for ; newLogEntryRelativeIndex < len(args.Entries); newLogEntryRelativeIndex++ {
		logEntryIndex := firstLogEntryIndex + newLogEntryRelativeIndex
		if logEntryIndex >= len(rf.logEntries) {
			break
		}

		if rf.logEntries[logEntryIndex].LeaderReceivedTerm != args.Entries[newLogEntryRelativeIndex].LeaderReceivedTerm {
			if logEntryIndex == 0 {
				rf.logEntries = []LogEntry{}
			} else {
				rf.logEntries = rf.logEntries[logEntryIndex-1:]
			}

			break
		}
	}

	rf.logEntries = append(rf.logEntries, args.Entries[newLogEntryRelativeIndex:]...)
	if args.LeaderCommitIndex != nil && rf.commitLogIndex != nil {
		if *args.LeaderCommitIndex > *rf.commitLogIndex {
			index := min(len(rf.logEntries)-1, *args.LeaderCommitIndex)
			rf.commitLogIndex = &index
		}

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
	index := -1
	// Your code here (2B).
	return index, rf.currentTerm, rf.role == LeaderRole
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
	// Your code here, if desired.
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

			if rf.receivedMessage {
				rf.logger.log(infoLogLevel, FollowerFlow, "received message from peer")
				rf.receivedMessage = false
				rf.mu.Unlock()
				continue
			}

			rf.role = CandidateRole
			rf.mu.Unlock()
			rf.runAsCandidate()
			return
		}

		rf.logger.log(infoLogLevel, FollowerFlow, "follower is killed")
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
					// TODO: start replicating logs
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

		rf.logger.log(infoLogLevel, CandidateFlow, "candidate is killed")
	}()
}

func (rf *Raft) waitForElectionTimeout() {
	randTimeoutMillis := rand.Intn(electionTimeoutRangeMillis)
	electionTimeout := time.Duration(randTimeoutMillis)*time.Millisecond + minElectionTimeout
	time.Sleep(electionTimeout)
}

func (rf *Raft) runAsLeader() {
	go func() {
		rf.sendPeriodicHeartbeats()
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
		for peerIndex := 0; !rf.killed() && peerIndex < len(rf.peers); peerIndex++ {
			if peerIndex == rf.me {
				continue
			}

			go func(peerIndex int) {
				rf.mu.Lock()
				prevLogIndex := rf.lastReplicatedLogIndices[peerIndex]
				var prevLogTerm *int
				if prevLogIndex != nil {
					prevLogTerm = &rf.logEntries[*prevLogIndex].LeaderReceivedTerm
				}

				args := AppendEntriesArgs{
					LeaderTerm:        rf.currentTerm,
					LeaderId:          rf.me,
					PrevLogIndex:      prevLogIndex,
					PrevLogTerm:       prevLogTerm,
					Entries:           []LogEntry{},
					LeaderCommitIndex: rf.commitLogIndex,
				}
				var reply AppendEntriesReply
				rf.logger.log(infoLogLevel, LeaderFlow, fmt.Sprintf("send heartbeat: leaderId=%v, leaderTerm=%v, peerIndex=%v", rf.me, rf.currentTerm, peerIndex))
				rf.mu.Unlock()
				rf.sendAppendEntries(peerIndex, &args, &reply)
			}(peerIndex)
		}

		time.Sleep(heartbeatInterval)
	}
}

func (rf *Raft) requestVoteFromPeers() int {
	rf.mu.Lock()
	requestVoteArgs := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	if len(rf.logEntries) > 0 {
		lastLogIndex := len(rf.logEntries) - 1
		requestVoteArgs.LastLogIndex = &lastLogIndex
		lastLog := rf.logEntries[lastLogIndex]
		requestVoteArgs.LastLogIndex = &lastLog.LeaderReceivedTerm
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
			rf.logger.log(infoLogLevel, LeaderElectionFlow, fmt.Sprintf("received vote from peer: peerId=%v", peerIndex))

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
	logger := NewLogger(infoLogLevel, map[Flow]bool{
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
		me:                       me,
		role:                     FollowerRole,
		nextLogToSendIndices:     make([]int, len(peers)),
		lastReplicatedLogIndices: make([]*int, len(peers)),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.logger.log(infoLogLevel, FollowerFlow, "start as follower")
	go func() {
		rf.runAsFollower()
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
		return false
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
		return false
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
