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
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type NodeRole int

const (
	Follower NodeRole = iota
	Candidate
	Leader
)

func (r NodeRole) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

const (
	// heartbeatInterval is how often the leader sends AppendEntries.
	// Must stay <= 100ms to respect the tester's 10/sec cap; a small
	// margin below the cap absorbs scheduling jitter.
	heartbeatInterval = 100 * time.Millisecond

	// electionTimeoutMin/Max bound the randomized election timeout.
	// Must be comfortably larger than heartbeatInterval (since the
	// tester caps heartbeats at 10/sec, the paper's 150-300ms range
	// is too tight) while still allowing a new leader to be elected
	// well within the tester's 5-second window.
	electionTimeoutMin = 300 * time.Millisecond
	electionTimeoutMax = 600 * time.Millisecond
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	// persistant state
	currentRole NodeRole
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state
	commitIndex   int
	lastApplied   int
	lastHeartbeat time.Time

	// volatile state on leader
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.currentRole == Leader
}

// lastLogInfo returns the index and term of the last entry in rf.log.
// Caller must hold rf.mu.
func (rf *Raft) lastLogInfo() (index, term int) {
	index = len(rf.log) - 1
	if index >= 0 {
		term = rf.log[index].Term
	}
	return
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

type RequestVoteArgs struct {
	Term        int
	CandidateID int
	LastLogIdx  int
	LastLogTerm int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentRole = Follower
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm

	myLastLogIdx, myLastLogTerm := rf.lastLogInfo()

	logUpToDate := args.LastLogTerm > myLastLogTerm || (args.LastLogTerm == myLastLogTerm && args.LastLogIdx >= myLastLogIdx)

	reply.VoteGranted = (args.Term >= rf.currentTerm) && (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && logUpToDate

	if reply.VoteGranted {
		rf.votedFor = args.CandidateID
		rf.lastHeartbeat = time.Now()
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIdx   int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.currentRole == Candidate) {
		rf.currentTerm = args.Term
		rf.currentRole = Follower
		rf.votedFor = -1
	}

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.lastHeartbeat = time.Now()

	// Conflict optimization: log is too short
	if len(rf.log) <= args.PrevLogIdx {
		reply.Success = false
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		return
	}

	// Conflict optimization: log terms mismatch at PrevLogIdx
	if rf.log[args.PrevLogIdx].Term != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.log[args.PrevLogIdx].Term
		reply.ConflictIndex = args.PrevLogIdx
		// Back up to the first index of this conflicting term
		for i := args.PrevLogIdx; i > 0 && rf.log[i].Term == reply.ConflictTerm; i-- {
			reply.ConflictIndex = i
		}
		return
	}

	reply.Success = true

	for i, entry := range args.Entries {
		currentIdx := args.PrevLogIdx + 1 + i

		if currentIdx < len(rf.log) {
			if rf.log[currentIdx].Term != entry.Term {
				rf.log = rf.log[:currentIdx]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		indexOfLastNewEntry := args.PrevLogIdx + len(args.Entries)

		if args.LeaderCommit < indexOfLastNewEntry {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = indexOfLastNewEntry
		}
		rf.applyCond.Signal()
	}
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
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// assumes caller already holds rf.mu
func (rf *Raft) startAgreement() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		prevLogIdx := rf.nextIndex[i] - 1
		prevLogTerm := rf.log[prevLogIdx].Term

		var entries []LogEntry
		if len(rf.log) > rf.nextIndex[i] {
			entries = append([]LogEntry{}, rf.log[rf.nextIndex[i]:]...)
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIdx:   prevLogIdx,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		go func(peer int, args AppendEntriesArgs) {
			var reply AppendEntriesReply
			ok := rf.sendAppendEntries(peer, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.currentTerm != args.Term || rf.currentRole != Leader {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.currentRole = Follower
				rf.votedFor = -1
				rf.lastHeartbeat = time.Now()
				return
			}

			if reply.Success {
				newMatch := args.PrevLogIdx + len(args.Entries)
				if newMatch > rf.matchIndex[peer] {
					rf.matchIndex[peer] = newMatch
					rf.nextIndex[peer] = newMatch + 1
					rf.updateLeaderCommitIndex()
				}
			} else {
				// Accelerated log backtracking
				if reply.ConflictTerm == -1 {
					rf.nextIndex[peer] = reply.ConflictIndex
				} else {
					lastIndexForTerm := -1
					// Search leader's log for the conflicting term
					for j := len(rf.log) - 1; j > 0; j-- {
						if rf.log[j].Term == reply.ConflictTerm {
							lastIndexForTerm = j
							break
						}
					}
					if lastIndexForTerm > 0 {
						rf.nextIndex[peer] = lastIndexForTerm + 1
					} else {
						rf.nextIndex[peer] = reply.ConflictIndex
					}
				}

				// Safety bound to ensure we don't drop below log index 1
				if rf.nextIndex[peer] < 1 {
					rf.nextIndex[peer] = 1
				}
			}
		}(i, args)
	}
}

func (rf *Raft) updateLeaderCommitIndex() {
	if rf.currentRole != Leader {
		return
	}

	matches := make([]int, len(rf.peers))
	copy(matches, rf.matchIndex)
	matches[rf.me] = len(rf.log) - 1

	sort.Ints(matches)
	majorityN := matches[len(rf.peers)/2]

	if majorityN > rf.commitIndex && rf.log[majorityN].Term == rf.currentTerm {
		rf.commitIndex = majorityN
		rf.applyCond.Signal()
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.killed() {
		return -1, -1, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentRole != Leader {
		return -1, rf.currentTerm, false
	}

	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	index := len(rf.log) - 1

	rf.startAgreement() // caller must hold rf.mu

	return index, rf.currentTerm, true
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
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	if rf.currentRole != Candidate {
		rf.mu.Unlock()
		return
	}
	rf.currentRole = Leader
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()

	go rf.heartbeatLoop()
}

func (rf *Raft) heartbeatLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.currentRole != Leader {
			rf.mu.Unlock()
			return
		}
		rf.startAgreement()
		rf.mu.Unlock()

		time.Sleep(heartbeatInterval)
	}
}

func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()

	rf.currentRole = Candidate

	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()

	lastLogIndex, lastLogTerm := rf.lastLogInfo()

	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
		LastLogIdx:  lastLogIndex,
		LastLogTerm: lastLogTerm,
	}

	votes := 1 // protected by rf.mu; only ever touched while holding it
	termAtStart := rf.currentTerm
	rf.mu.Unlock()

	quorumTarget := (len(rf.peers) / 2) + 1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(peer int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(peer, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()

			if rf.currentRole != Candidate || rf.currentTerm != termAtStart {
				rf.mu.Unlock()
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentRole = Follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.mu.Unlock()
				return
			}

			becameLeader := false
			if reply.VoteGranted {
				votes++
				if votes == quorumTarget {
					becameLeader = true
				}
			}

			rf.mu.Unlock()

			if becameLeader {
				rf.convertToLeader()
			}
		}(i)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		span := int64(electionTimeoutMax - electionTimeoutMin)
		timeout := electionTimeoutMin + time.Duration(rand.Int63n(span))

		time.Sleep(timeout)

		rf.mu.Lock()
		timeElapsed := time.Since(rf.lastHeartbeat)
		isLeader := rf.currentRole == Leader
		rf.mu.Unlock()

		if timeElapsed >= timeout && !isLeader {
			rf.convertToCandidate()
		}
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}

		startIdx := rf.lastApplied + 1
		endIdx := rf.commitIndex

		entries := make([]LogEntry, endIdx-startIdx+1)
		copy(entries, rf.log[startIdx:endIdx+1])
		rf.mu.Unlock()

		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: startIdx + i,
			}
		}

		rf.mu.Lock()
		if startIdx+len(entries)-1 > rf.lastApplied {
			rf.lastApplied = startIdx + len(entries) - 1
		}
		rf.mu.Unlock()
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.votedFor = -1

	rf.log = []LogEntry{{Term: 0}}
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()
	go rf.ticker()
	go rf.applier()

	return rf
}
