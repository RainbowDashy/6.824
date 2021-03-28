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

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

type Log struct {
	Term int
	Cmd  interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	currentTerm int
	votedFor    int
	log         []Log
	commitIndex int
	lastApplied int
	state       int
	prevTime    time.Time
	nextIndex   []int
	matchIndex  []int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// Is A more up-to-date than B ?
// or at least as up-to-date as B

func leastUpToDate(termA, indexA, termB, indexB int) bool {
	return (termA > termB) || (termA == termB && indexA >= indexB)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	rf.checkTerm(args.Term)
	if rf.state != FOLLOWER {
		return
	}
	if rf.currentTerm <= args.Term && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && leastUpToDate(args.LastLogTerm, args.LastLogIndex, rf.log[len(rf.log)-1].Term, len(rf.log)-1) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.prevTime = time.Now()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return -1, -1, false
	}

	index := len(rf.log)
	term := rf.currentTerm
	isLeader := true
	// Your code here (2B).

	rf.log = append(rf.log, Log{term, command})
	DPrintf("Term %v: Append %v", rf.currentTerm, command)

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
	rand.Seed(time.Now().UTC().UnixNano())
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.log = make([]Log, 1)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.checkLeader()
	go rf.checkApplied(applyCh)
	return rf
}

func (rf *Raft) candidateTimer() {
	timeout := time.Millisecond * time.Duration(450+rand.Intn(100))
	for {
		if !rf.checkState(CANDIDATE) {
			return
		}
		rf.mu.Lock()
		if time.Since(rf.prevTime) > timeout {
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) leaderElection() {
	// call with lock held

	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.prevTime = time.Now()
	vote := 1
	count := 1
	DPrintf("Term %v: %v start to elect!", rf.currentTerm, rf.me)

	go rf.candidateTimer()

	rf.mu.Unlock()
	c := make(chan bool)
	args := RequestVoteArgs{rf.currentTerm, rf.me, len(rf.log) - 1, rf.log[len(rf.log)-1].Term}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if !rf.checkState(CANDIDATE) {
			return
		}
		go func(id int) {
			if !rf.checkState(CANDIDATE) {
				return
			}
			reply := RequestVoteReply{}
			if rf.sendRequestVote(id, &args, &reply) {
				if reply.VoteGranted {
					DPrintf("Term %v: %v got one vote from %v", rf.currentTerm, rf.me, id)
				}
				rf.mu.Lock()
				rf.checkTerm(reply.Term)
				rf.mu.Unlock()
			}
			c <- reply.VoteGranted
		}(i)
	}

	rf.mu.Lock()
	DPrintf("Term %v: %v waiting...", rf.currentTerm, rf.me)
	rf.mu.Unlock()

	for vote <= len(rf.peers)/2 && count != len(rf.peers) {
		if <-c {
			vote++
		}
		count++
		if !rf.checkState(CANDIDATE) {
			return
		}
	}

	rf.mu.Lock()
	DPrintf("Term %v: %v get %v votes!", rf.currentTerm, rf.me, vote)
	rf.mu.Unlock()

	if vote > len(rf.peers)/2 {
		rf.mu.Lock()
		DPrintf("Term %v: %v is leader!", rf.currentTerm, rf.me)
		rf.state = LEADER
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.log)
		}
		go rf.checkCommit()
		rf.heartbeat()
	}
}

type KeyValue struct {
	key   int
	value int
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].key > a[j].key }

func (rf *Raft) checkCommit() {
	for {
		if !rf.checkState(LEADER) {
			return
		}
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		cnt := make(map[int]int)
		for i, v := range rf.matchIndex {
			if i == rf.me {
				cnt[len(rf.log)]++
			} else {
				cnt[v]++
			}
		}
		kv := make([]KeyValue, len(cnt))
		for i, v := range cnt {
			kv = append(kv, KeyValue{i, v})
		}
		sort.Sort(ByKey(kv))
		t := 0
		for _, v := range kv {
			t += v.value
			if v.key <= rf.commitIndex {
				break
			}
			if t > len(rf.peers)/2 && rf.log[v.key].Term == rf.currentTerm {
				DPrintf("Term %v: Commit Index %v -> %v", rf.currentTerm, rf.commitIndex, v.key)
				rf.commitIndex = v.key
				break
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 33)
	}
}

func (rf *Raft) checkApplied(applyCh chan ApplyMsg) {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			// rf apply to state machine
			index := rf.lastApplied
			msg := ApplyMsg{true, rf.log[index].Cmd, index}
			applyCh <- msg
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) sendHeartbeat(x, term int) {
	prevLogIndex := rf.nextIndex[x] - 1
	prevLogTerm := -1
	entries := make([]Log, 0)
	if rf.nextIndex[x] < len(rf.log) {
		entries = append(entries, rf.log[rf.nextIndex[x]])
	}
	if prevLogIndex < len(rf.log) { //always true?
		prevLogTerm = rf.log[prevLogIndex].Term
	}
	args := AppendEntriesArgs{term, rf.me, prevLogIndex, prevLogTerm, entries, rf.commitIndex}
	reply := AppendEntriesReply{}
	go func(x int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
		if rf.sendAppendEntries(x, args, reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			DPrintf("Term %v: %v->%v %v (index %v)", rf.currentTerm, rf.me, x, reply.Success, prevLogIndex)
			rf.checkTerm(reply.Term)
			if reply.Success {
				if rf.nextIndex[x] < len(rf.log) {
					rf.nextIndex[x]++
				}
				rf.matchIndex[x] = max(rf.matchIndex[x], args.PrevLogIndex+len(args.Entries))
				// go rf.checkcommit()
			} else {
				DPrintf("Term %v: %v and %v disagree on index %v cause %v", rf.currentTerm, rf.me, x, args.PrevLogIndex, reply.Cause)
				if rf.nextIndex[x] > 1 {
					rf.nextIndex[x]--
				}

			}
		}
	}(x, &args, &reply)
}

func (rf *Raft) heartbeat() {
	// call with lock held
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(x, term int) {

			for {
				if rf.killed() {
					return
				}
				rf.mu.Lock()
				if rf.state != LEADER {
					rf.mu.Unlock()
					return
				}
				if rf.currentTerm > term {
					rf.mu.Unlock()
					return
				}
				DPrintf("Term %v: %v->%v", term, rf.me, x)
				rf.sendHeartbeat(x, term)
				rf.mu.Unlock()
				time.Sleep(time.Millisecond * 100)
			}

		}(i, rf.currentTerm)
	}
	rf.mu.Unlock()
}

func (rf *Raft) checkLeader() {
	const sleepTime = time.Millisecond * 10
	debugTime := time.Now()
	electionTimeout := time.Millisecond * time.Duration(rand.Intn(100)+300)
	name := []string{"FOLLOWER", "CANDIDATE", "LEADER"}
	for {
		time.Sleep(sleepTime)
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if time.Since(debugTime) > time.Millisecond*100 {
			DPrintf("Term %v: %v is %v", rf.currentTerm, rf.me, name[rf.state])
			debugTime = time.Now()
		}

		if rf.state == FOLLOWER && time.Since(rf.prevTime) > electionTimeout {
			rf.leaderElection()
		} else {
			rf.mu.Unlock()
		}

	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Cause   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Cause = 0
		return
	}
	if !rf.checkTerm(args.Term) {
		rf.votedFor = args.LeaderId
	}
	if rf.state == CANDIDATE {
		rf.state = FOLLOWER
		rf.votedFor = args.LeaderId
	}
	if rf.state == FOLLOWER {
		if rf.votedFor != args.LeaderId {
			reply.Cause = 1
			DPrintf("Term %v: %v is no the leader of %v", args.Term, args.LeaderId, rf.me)
			return
		}
		rf.prevTime = time.Now()
		logIndex := args.PrevLogIndex
		if logIndex >= len(rf.log) {
			reply.Cause = 2
			return
		}
		logTerm := rf.log[logIndex].Term
		if logTerm != args.PrevLogTerm {
			reply.Cause = 3
			return
		}
		pos := logIndex + 1
		for i, v := range args.Entries {
			if pos >= len(rf.log) {
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
			if rf.log[pos].Term != v.Term {
				rf.log = rf.log[:pos]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
			pos++
		}
		if args.LeaderCommit > rf.commitIndex {
			DPrintf("Term %v: %v's commit index %v->%v", rf.currentTerm, rf.me, rf.commitIndex, min(args.LeaderCommit, len(rf.log)-1))
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		}
		reply.Success = true
		DPrintf("%v recognize %v as Leader", rf.me, args.LeaderId)
		return
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) print(format string, a ...interface{}) {
	rf.mu.Lock()
	DPrintf(format, a...)
	rf.mu.Unlock()
}

func (rf *Raft) checkTerm(term int) bool {
	if rf.currentTerm < term {
		rf.state = FOLLOWER
		rf.currentTerm = term
		rf.votedFor = -1
		return false
	}
	return true
}

func (rf *Raft) checkState(state int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == state
}
