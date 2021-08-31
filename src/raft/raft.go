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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// Schedule Interval for election timeout and heartbeat period
//
const (
	ElectionTimeOutBase = time.Millisecond * 600
	HeartBeatPeriod     = time.Millisecond * 100
	ScheduleInterval    = time.Millisecond * 100
)

//
// Status constant
//
const (
	LeaderStatus = iota
	FollowerStatus
	CandidateStatus
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A object store the log entries
//
type LogEntry struct {
	TermNum  int
	LogIndex int
	Command  interface{}
}

//
//Generate random election timeout between 400ms ~ 800ms
//
func genElenctionTimeout() time.Duration {
	return ElectionTimeOutBase + time.Duration(rand.Int63n(int64(ElectionTimeOutBase)))
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm  int
	currentState int
	voteFor      int
	peerCount    int
	logs         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	//Manage election
	lastTimeOut     time.Time
	electionTimeOut time.Duration
	voteCollected   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.currentState == LeaderStatus
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	TermNum      int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	TermNum     int
	VoteGranted bool
}

type AppendEntryArgs struct {
	TermNum      int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	TermNum int
	Success bool
}

//
// Get end of the log
// return 0, 0 if logs are empty
//
func (rf *Raft) getEndofLog() (int, int) {
	lastIndex := len(rf.logs) - 1
	if lastIndex >= 0 {
		return lastIndex, rf.logs[lastIndex].TermNum
	} else {
		return 0, 0
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.TermNum < rf.currentTerm {
		// reply falsue and return the updated term number
		reply.TermNum = rf.currentTerm
		reply.VoteGranted = false
	} else if args.TermNum == rf.currentTerm {
		// Grant vote if voteFor matches or is NULL, and the log is at least up-to-date
		lastIndex, lastTerm := rf.getEndofLog()

		if (rf.voteFor == args.CandidateID || rf.voteFor == -1) && (args.LastLogTerm < args.LastLogTerm || (args.LastLogIndex <= lastIndex && args.LastLogTerm == lastTerm)) {
			reply.TermNum = rf.currentTerm
			reply.VoteGranted = true
			rf.resetTimeOut()
		}
	} else {
		// Convert to follower and grant the vote
		// https://stackoverflow.com/questions/66673136/raft-leader-election-when-the-candidater-receive-a-voterequest-with-higher-ter
		// Should I check if the requester's logs are at least up-to-date?
		rf.changeTerm(args.TermNum)
		if rf.currentState != FollowerStatus {
			rf.becameFollower()
		}
		rf.voteFor = args.CandidateID
		reply.TermNum = rf.currentTerm
		reply.VoteGranted = true
	}
}

//
// AppendEntryHandler:
//
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.TermNum < rf.currentTerm {
		reply.TermNum = rf.currentTerm
		reply.Success = false
	} else {
		// Check current status, convert to follower if it is not follower state
		if rf.currentState != FollowerStatus {
			rf.becameFollower()
		}
		rf.resetTimeOut()
		// Update term if the current term is not up-to-date
		if rf.currentTerm < args.TermNum {
			rf.changeTerm(args.TermNum)
		}

		// Lab 2B: append entry
	}

}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetTimeOut() {
	rf.lastTimeOut = time.Now()
	rf.electionTimeOut = genElenctionTimeout()
}

//
// Step to leader: change state, initialize volatile state and broadcast heartbeat
//
func (rf *Raft) becameLeader() {
	rf.currentState = LeaderStatus
	lastIndex := len(rf.logs)
	for i := 0; i < rf.peerCount; i++ {
		rf.nextIndex[i] = lastIndex
		rf.matchIndex[i] = 0
	}
	rf.broadCastHeatBeat()
}

//
// convert to follower: change state and reset some property
//
func (rf *Raft) becameFollower() {

	rf.currentState = FollowerStatus
	rf.resetTimeOut()
	rf.voteFor = -1
}

//
// convert to candidate: change state and start election
//
func (rf *Raft) becameCandidate() {
	rf.currentState = CandidateStatus
	rf.startElection()
}

//
// change term, reset some property
//
func (rf *Raft) changeTerm(term int) {
	rf.currentTerm = term
	rf.voteFor = -1
}

//
// Start Election:
// increment term, vote for self, reset timeout, broadcast request vote
//
func (rf *Raft) startElection() {
	rf.changeTerm(rf.currentTerm + 1)
	rf.voteCollected = 1
	rf.voteFor = rf.me
	rf.resetTimeOut()
	rf.broadCastRequestVote()
}

//
// Request Vote from server x until successful
// If x reply with an larger term number, convert to follower
//
func (rf *Raft) requestVoteFrom(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	for rf.currentState == CandidateStatus {
		rf.mu.Unlock()
		ok := rf.sendRequestVote(server, args, reply)
		if ok == true {
			rf.mu.Lock()
			break
		}
		time.Sleep(ScheduleInterval / 10)
		rf.mu.Lock()
	}
	defer rf.mu.Unlock()
	if reply.TermNum > rf.currentTerm {
		rf.changeTerm(reply.TermNum)
		rf.becameFollower()
	} else if reply.VoteGranted {
		rf.voteCollected++
	}
}

//
// Append Entry to server x until successful
// If x reply with a higher term num, convert to follower
// Keep sending rpc until successful
//
func (rf *Raft) appendEntryTo(server int, args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	for rf.currentState == LeaderStatus {
		rf.mu.Unlock()
		ok := rf.sendAppendEntry(server, args, reply)
		if ok == true {
			rf.mu.Lock()
			break
		}
		time.Sleep(ScheduleInterval / 10)
		rf.mu.Lock()
	}
	defer rf.mu.Unlock()
	if reply.TermNum > rf.currentTerm {
		rf.changeTerm(reply.TermNum)
		rf.becameFollower()
	}
}

//
// Broadcast Request Vote
// For each request vote, create an individual gorotine to prevent
// the block because of an unsuccessful request vote
//
func (rf *Raft) broadCastRequestVote() {
	lastLogEntry, lastLogTerm := rf.getEndofLog()

	var args = make([]RequestVoteArgs, rf.peerCount)
	var replies = make([]RequestVoteReply, rf.peerCount)

	for i := 0; i < len(args); i++ {
		replies[i] = RequestVoteReply{}
		args[i] = RequestVoteArgs{rf.currentTerm, rf.me, lastLogEntry, lastLogTerm}
	}
	for i := 0; i < rf.peerCount; i++ {
		if i != rf.me {
			go rf.requestVoteFrom(i, &args[i], &replies[i])
		}
	}
}

//
// BroadCast HeartBeat: Send appendentry request with empty log
//
func (rf *Raft) broadCastHeatBeat() {
	lastLogEntry, lastLogTerm := rf.getEndofLog()

	// The args and replies for different gorotines must be seperated
	// to avoid data race.
	var args = make([]AppendEntryArgs, rf.peerCount)
	var replies = make([]AppendEntryReply, rf.peerCount)

	for i := 0; i < rf.peerCount; i++ {
		replies[i] = AppendEntryReply{}
		args[i] = AppendEntryArgs{rf.currentTerm, rf.me, lastLogEntry, lastLogTerm, nil, rf.commitIndex}
	}

	for i := 0; i < rf.peerCount; i++ {
		if i != rf.me {
			go rf.appendEntryTo(i, &args[i], &replies[i])
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		switch rf.currentState {
		// For Candidate Status, first count the vote gained
		// then determine whether became leader or not.
		// If didn't obtain enough votes and time elapsed,
		// Start a new election.
		case CandidateStatus:
			if rf.voteCollected > rf.peerCount/2 {
				rf.becameLeader()
			} else if time.Now().Sub(rf.lastTimeOut) > rf.electionTimeOut {
				rf.becameCandidate()
			}

		//For Follower Status, convert to Candidate if time elapsed
		//without receive any appendEnntry request
		case FollowerStatus:
			if time.Now().Sub(rf.lastTimeOut) > rf.electionTimeOut {
				rf.becameCandidate()
			}

		//For leader status, broadcast heartbeat periodly
		case LeaderStatus:
			rf.broadCastHeatBeat()
		}
		rf.mu.Unlock()
		time.Sleep(ScheduleInterval)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.currentState = CandidateStatus
	rf.peerCount = len(peers)
	rf.voteFor = -1
	rf.logs = []LogEntry{}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, rf.peerCount)
	rf.matchIndex = make([]int, rf.peerCount)
	rf.lastTimeOut = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
