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
	"log"
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

// RaftBaseInfo raft基本信息
type RaftBaseInfo struct {
	baseMu sync.Mutex

	onHeartbeat bool  //标记一次选举超时时间内是否收到心跳信号
	dead        int32 // set by Kill()
	role        int
	currentTerm int
	voteFor     int
	votes       int
	commitIndex int //已知可被提交的最高日志标签
	lastApplied int //本机已被提交的最高日志标签
}

// RaftLeaderInfo raft领导人专用信息
type RaftLeaderInfo struct {
	leaderMu sync.Mutex

	nextIndex  []int //向每台服务器发送的首日志标签 appendEntries重试自减
	matchIndex []int //领导人与跟随者一致的最大日志标签
}

// A Go object implementing a single Raft peer.
type Raft struct {
	//写后只读数据
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	me        int                 // this peer's index into peers[]
	r         *rand.Rand
	persister *Persister // Object to hold this peer's persisted state
	applyCh   chan ApplyMsg
	logs      *Logs //logs内部具有专用锁

	RaftBaseInfo
	RaftLeaderInfo
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A)
	rf.baseMu.Lock()
	var term = rf.currentTerm
	var flag = rf.role == Leader
	rf.baseMu.Unlock()
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
	rf.baseMu.Lock()
	defer rf.baseMu.Unlock()

	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.voteFor = -1
		rf.role = Follower
	}
}

// isLogMoreNew 比较候选人与本机的日志新鲜程度
func (rf *Raft) isLogMoreNew(args *RequestVoteArgs) bool {
	var lastLogTerm = rf.logs.GetLastLog().Term
	var lastLogIndex = rf.logs.Len() - 1

	if args.LastLogTerm < lastLogTerm {
		return false
	} else if args.LastLogTerm > lastLogTerm {
		return true
	} else {
		return args.LastLogIndex >= lastLogIndex
	}
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.updateRaft(args.Term)

	rf.baseMu.Lock()
	defer rf.baseMu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		reply.Term = args.Term
		if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && rf.isLogMoreNew(args) {
			rf.voteFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	}
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.baseMu.Lock()
	defer rf.baseMu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//log.Println("id",rf.me,args)
	if args.Term > rf.currentTerm {
		rf.voteFor = -1
	}
	rf.currentTerm = args.Term
	rf.role = Follower
	rf.onHeartbeat = true
	reply.Term = args.Term

	if args.PrevLogIndex < rf.logs.Len() && rf.logs.GetLog(args.PrevLogIndex).Term == args.PrevLogTerm {
		rf.logs.Rewrite(args.PrevLogIndex+1, args.Entries)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = Min(args.LeaderCommit, rf.logs.Len()-1)
		}

		reply.Success = true
	} else {
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

		rf.baseMu.Lock()
		defer rf.baseMu.Unlock()

		if reply.VoteGranted {
			rf.votes++
			if rf.role == Candidate {
				if rf.votes > ((len(rf.peers)) >> 1) {
					rf.role = Leader
					log.Println(rf.me, "任期", rf.currentTerm, "赢得选举,选票数: ", rf.votes)
					rf.leaderMu.Lock()
					rf.matchIndex = make([]int, len(rf.peers))
					rf.nextIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.logs.Len()
					}
					rf.leaderMu.Unlock()

					go rf.leaderAppendLogTicker()
					go rf.leaderCommitTicker()
				}
			}
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.updateRaft(reply.Term)

	if args.Entries != nil {

		rf.leaderMu.Lock()
		if reply.Success {
			var matchIndex = args.PrevLogIndex + len(args.Entries) //更新matchIndex与nextIndex
			rf.matchIndex[server] = matchIndex
			rf.nextIndex[server] = matchIndex + 1
		} else {
			if rf.nextIndex[server] > rf.matchIndex[server]+1 {
				rf.nextIndex[server]-- //nextIndex-- 重试
			}
		}
		rf.leaderMu.Unlock()

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

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.baseMu.Lock()
	defer rf.baseMu.Unlock()

	if rf.role != Leader {
		return -1, -1, false
	}
	//log.Println(rf.me,"接受指令",command)
	rf.logs.Add(
		Log{
			Term:    rf.currentTerm,
			Command: command,
		})
	return rf.logs.Len() - 1, rf.currentTerm, true
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		time.Sleep(ElectionTimeOutLow + time.Duration(rf.r.Int63()%int64(ElectionTimeOutHigh-ElectionTimeOutLow)))
		rf.baseMu.Lock()
		fmt.Println("Id", rf.me, "term", rf.currentTerm, "commitIndex", rf.commitIndex, "role", rf.role, rf.logs.ToString())
		if (rf.role == Follower || rf.role == Candidate) && !rf.onHeartbeat {
			rf.baseMu.Unlock()
			go rf.electionHandle()

			rf.baseMu.Lock()
			rf.onHeartbeat = false
			rf.baseMu.Unlock()

		} else {
			rf.onHeartbeat = false
			rf.baseMu.Unlock()
		}
	}
}

// applyCommandTicker
// 将标签小于commitIndex且未Applied的日志应用于处理机
func (rf *Raft) applyCommandTicker() {
	for rf.killed() == false {
		rf.baseMu.Lock()
		if rf.commitIndex > rf.lastApplied {
			for rf.lastApplied < rf.commitIndex {
				//log.Printf("id %d term %d applied command %v\n",rf.me,rf.currentTerm,rf.logs.GetLog(rf.lastApplied+1).Command)
				rf.lastApplied++
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.logs.GetLog(rf.lastApplied).Command,
					CommandIndex: rf.lastApplied,
				}
			}
		}
		rf.baseMu.Unlock()
		time.Sleep(HeartBeatTime)
	}
}

func (rf *Raft) leaderAppendLogTicker() {
	for rf.killed() == false {
		rf.baseMu.Lock()
		if rf.role == Follower || rf.role == Candidate {
			rf.baseMu.Unlock()
			return
		}
		//log.Println("id",rf.me,"match",rf.matchIndex,"next",rf.nextIndex)
		var argsList = make([]*AppendEntriesArgs, len(rf.peers))
		for i := range argsList {
			var lastIndex = rf.logs.Len() - 1

			rf.leaderMu.Lock()
			var nextIndex = rf.nextIndex[i]
			rf.leaderMu.Unlock()

			argsList[i] = &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: nextIndex - 1,
				PrevLogTerm:  rf.logs.GetLog(nextIndex - 1).Term,
			}
			//log.Println(rf.me, nextIndex, lastIndex, argsList[i])
			if lastIndex >= nextIndex {
				argsList[i].Entries = rf.logs.GetRangeLog(nextIndex, lastIndex)
			}
		}
		rf.baseMu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {

				var reply = &AppendEntriesReply{}
				go rf.sendAppendEntries(i, argsList[i], reply)
			}
		}
		time.Sleep(HeartBeatTime)
	}
}

func (rf *Raft) leaderCommitTicker() {
	for rf.killed() == false {
		rf.baseMu.Lock()
		if rf.role == Follower || rf.role == Candidate {
			rf.baseMu.Unlock()
			return
		}

		rf.leaderMu.Lock()
		for index := rf.logs.Len() - 1; index > rf.commitIndex; index-- {
			var matchSize = 1
			//log.Println("id",rf.me,"term",rf.currentTerm,rf.matchIndex)
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= index {
					matchSize++
				}
			}
			if rf.logs.GetLog(index).Term == rf.currentTerm && matchSize > (len(rf.peers)>>1) {
				rf.commitIndex = index
				break
			}
		}
		rf.leaderMu.Unlock()
		rf.baseMu.Unlock()

		time.Sleep(HeartBeatTime)
	}
}

func (rf *Raft) electionHandle() {
	rf.baseMu.Lock()

	rf.role = Candidate
	rf.votes = 1
	rf.currentTerm++
	rf.voteFor = rf.me

	var args = &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.logs.Len() - 1,
		LastLogTerm:  rf.logs.GetLastLog().Term,
	}
	rf.baseMu.Unlock()

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
	rf.applyCh = applyCh

	rf.role = Follower
	rf.onHeartbeat = false
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.dead = 0
	rf.votes = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.logs = NewLogs()
	rf.r = rand.New(rand.NewSource(int64(rf.me + time.Now().Nanosecond())))

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	// start ticker goroutine to apply command
	go rf.applyCommandTicker()

	return rf
}
