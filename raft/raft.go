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
	"myraft/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

const (
	HeartBeatTime       = time.Millisecond * 150
	ElectionTimeOutLow  = time.Millisecond * 300
	ElectionTimeOutHigh = time.Millisecond * 800
)

const (
	Leader = iota
	Candidate
	Follower
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the Apply	reply.Term = args.Term
//	reply.Success = trueyMsg contains a newly
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
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	onHeartbeat bool //标记一次选举超时时间内是否收到心跳信号
	r           *rand.Rand

	applyCh chan ApplyMsg

	logs        Logs
	role        int
	currentTerm int
	voteFor     int
	votes       int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A)
	rf.mu.Lock()
	var term = rf.currentTerm
	var flag = rf.role == Leader
	rf.mu.Unlock()
	return term, flag
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
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool //是否赢得投票
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int

	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) updateRaft(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.voteFor = -1
		rf.role = Follower
	}
}

//isMoreNew  判断candidate的Log是否比当前raft新
func (rf *Raft) isMoreNew(args *RequestVoteArgs) bool {
	var lastLogTerm = rf.logs.GetLastLogTerm()
	if args.LastLogTerm < lastLogTerm {
		return false
	}
	if args.LastLogTerm == lastLogTerm && args.LastLogIndex < rf.logs.GetLastLogIndex() {
		return false
	}
	return true
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.updateRaft(args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

	} else {
		reply.Term = args.Term
		if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && rf.isMoreNew(args) {
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	}
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term >= rf.currentTerm { //心跳任期号大于等于当前任期 接受领导
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.role = Follower

		if args.Entries == nil {
			reply.Term = args.Term
			reply.Success = true
			return
		}

		if args.PrevLogIndex < len(rf.logs.Data) && args.PrevLogTerm != rf.logs.Data[args.PrevLogIndex].Term {
			reply.Term = args.Term
			reply.Success = false
		} else {
			rf.logs.Data = append(rf.logs.Data[:args.PrevLogIndex], args.Entries...)
			rf.onHeartbeat = true
			rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)-1)
			rf.run()

			reply.Term = args.Term
			reply.Success = true
		}
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
	}
	return
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
// if you're having trouble getting RPC to work, check 	that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.updateRaft(reply.Term)

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.VoteGranted {
			rf.votes++
			if rf.role == Candidate {
				if rf.votes > ((len(rf.peers)) >> 1) {
					rf.role = Leader
					for i := 0; i < len(rf.peers); i++ {
						//raft确定为leader后,初始化nextIndex与matchIndex
						rf.nextIndex[i] = len(rf.logs.Data)
						rf.matchIndex[i] = 0
					}
					go rf.LeaderWork()
				}
			}
		}
	}
	return ok
}

func (rf *Raft) LeaderWork() {
	var ticker = time.NewTicker(HeartBeatTime)
	for rf.killed() == false {
		<-ticker.C
		rf.mu.Lock()
		if rf.role == Follower {
			rf.mu.Unlock()
			return
		}
		var args = &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				var reply = &AppendEntriesReply{}

				rf.mu.Lock()
				if rf.logs.GetLastLogIndex() >= rf.nextIndex[i] {

					args.PrevLogIndex = rf.nextIndex[i]
					args.PrevLogTerm = rf.logs.Data[args.PrevLogIndex].Term
					args.Entries = rf.logs.Data[rf.nextIndex[i]:]

				}
				rf.mu.Unlock()

				go rf.sendAppendEntries(i, args, reply)
				go rf.CommitIndexUpdate()
			}
		}
	}
}

func (rf *Raft) CommitIndexUpdate() {

	for index := len(rf.logs.Data) - 1; index > rf.commitIndex; index-- {

		var matchSize = 0
		if rf.matchIndex[index] >= index {
			matchSize++
		}

		if rf.logs.Data[index].Term == rf.currentTerm && matchSize > ((len(rf.peers))>>1) {
			rf.commitIndex = index
			go rf.run()
			return
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.voteFor = -1
		rf.role = Follower
	} else {
		if !reply.Success {
			rf.nextIndex[server]--
		} else {
			rf.nextIndex[server] = rf.logs.GetLastLogIndex() + 1
			rf.matchIndex[server] = rf.logs.GetLastLogIndex()
		}
	}
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
func (rf *Raft) run() {
	//存在问题
	for index := rf.lastApplied; index < rf.commitIndex; index++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			CommandIndex: index,
			Command:      rf.logs.Data[index].Command,
		}
		rf.lastApplied++
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.role != Leader {
		return -1, -1, false
	}

	var rfLog = Log{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs.Add(rfLog)

	return rf.logs.GetLastLogIndex(), rf.currentTerm, true
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(ElectionTimeOutLow + time.Duration(rf.r.Int63()%int64(ElectionTimeOutHigh-ElectionTimeOutLow)))
		rf.mu.Lock()
		if (rf.role == Follower || rf.role == Candidate) && (rf.voteFor == -1 || rf.voteFor == rf.me) && !rf.onHeartbeat {
			rf.mu.Unlock()
			go rf.electionHandle()

			rf.mu.Lock()
			rf.onHeartbeat = false
			rf.mu.Unlock()

		} else {
			rf.onHeartbeat = false
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) electionHandle() {
	rf.mu.Lock()

	rf.role = Candidate
	rf.votes = 1
	rf.currentTerm++
	rf.voteFor = rf.me
	var args = &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.logs.GetLastLogTerm(),
		LastLogIndex: rf.logs.GetLastLogIndex(),
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			var reply = &RequestVoteReply{}
			go rf.sendRequestVote(i, args, reply)
		}
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
	rf.role = Follower
	rf.onHeartbeat = false
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.dead = 0
	rf.votes = 0
	rf.r = rand.New(rand.NewSource(int64(rf.me + time.Now().Nanosecond())))
	rf.commitIndex = 0
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.lastApplied = 0
	rf.logs = Logs{
		Data: make([]Log, 1, 10),
	}
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
