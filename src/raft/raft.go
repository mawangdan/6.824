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

const (
	BroadcastTime      int64 = int64(time.Millisecond) * 3
	MaxElectionTimeout int64 = int64(time.Millisecond)
	MinElectionTimeout int64 = int64(time.Millisecond)
)

//If a candidate or leader discovers
//that its term is out of date, it immediately reverts to follower state.
type ServerState int32

const (
	Leader ServerState = iota
	Candidate
	Follower
)

type ClusterState int

const (
	C_o ClusterState = iota
	C_o_n
	C_n
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
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	dead              int32               // set by Kill()
	peerNumber        int
	majority          int
	lastHeartBeatTime int64
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state ServerState

	//use for membership changes
	oldClusters  map[int]int
	newClusters  map[int]int
	clusterState ClusterState

	//persistent state
	currentTerm int
	votedFor    int //candidateId that received vote in current term (or null if none)
	log         []LogEntry

	//volatile state
	commitIndex int
	lastApplied int

	//only leader Reinitialized after election
	nextIndex  []int
	matchIndex []int
}

// Log Entry
type LogEntry struct {
	command interface{}
	term    int
}

type AppendEntriesArgs struct {
	term     int //leader's term
	leaderId int
	//下面两项用于检查冲突
	prevLogIndex int
	pervLogTerm  int

	entries      []LogEntry
	leaderCommit int
}

type AppendEntriesReply struct {
	term    int  //currentTerm, for leader to update itself
	success bool //true if follower contained entry matching prevLogIndex and prevLogTerm

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
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
	Term        int //candidate’s term
	CandidateId int //candidate requesting vote
	//The next two fields are used for elect restriction
	LastLogIndex int //index of candidate’s last log entry (§5.4)
	LastLogTerm  int //term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

type ListenOnRPC struct {
	seq   int
	ok    bool
	reply *RequestVoteReply
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
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

func (rf *Raft) getState() ServerState {
	return ServerState(atomic.LoadInt32((*int32)(&rf.state)))
}

func (rf *Raft) getLastHeartBeatTime() int64 {
	return atomic.LoadInt64(&rf.lastHeartBeatTime)
}

//需要原子操作，否则可能越界
func (rf *Raft) getLastLog() LogEntry {
	lastLog := rf.log[len(rf.log)-1]
	return lastLog
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rand.Seed(time.Now().UnixNano())
	electionTimeout := rand.Int63n(MaxElectionTimeout-MinElectionTimeout) + MinElectionTimeout
	for rf.killed() == false {
		//选举过程中发现任何leader发出的newterm都变成follower
		//发现的任何newterm变成follower
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//至少要睡这么久
		sleepTime := electionTimeout - (time.Now().UnixNano() - rf.getLastHeartBeatTime())
		if sleepTime >= 0 {
			time.Sleep(time.Duration(sleepTime))
		}
		//follower 超时没有心跳,开始选举,
		//这里要考虑选举中途会不会身份发生改变
		//1.follower->leader 不可能
		//2.follower->candidate 只可能是下面的代码
		//所以if不需要加锁
		if time.Now().UnixNano()-rf.getLastHeartBeatTime() >= electionTimeout && (rf.getState() == Follower || rf.getState() == Candidate) {
			//这个时候收到投票会怎么样
			//会votefor这个term的leader，假设这个时候选出了leader
			//不影响，因为后面term++了，所以根据状态转移，前面的leader收到更新的term会变成follower

			rf.mu.Lock()
			rf.currentTerm++
			rf.state = Candidate
			//vote for itself,这个时候
			//TODO:假设引入membership changes
			//如果状态属于C_o,C_o_n则可以为自己投票
			//如果C_n则需要知道自己是否属于new cluster
			rf.votedFor = rf.me
			rf.mu.Unlock()

			//如果在ask vote的前中Candidate收到RPC而变成Follower,那么永远无法获得majority
			//如果是过程中,要特殊处理，不然选举完会以为他是newterm的leader（因为前面oldterm的票会当成newterm的票）:
			// While waiting for votes, a candidate may receive an
			// AppendEntries RPC from another server claiming to be
			// leader. If the leader’s term (included in its RPC) is at least
			// as large as the candidate’s current term, then the candidate
			// recognizes the leader as legitimate and returns to follower
			// state.
			countVote := 1     //初始1是他自己
			countAllReply := 1 //初始1是他自己

			startElectTime := time.Now().UnixNano()
			rf.mu.Lock()
			for i, _ := range rf.peers {
				if i != rf.me {
					lastLog := rf.getLastLog()
					args := &RequestVoteArgs{rf.currentTerm, rf.me, len(rf.log) - 1, lastLog.term}
					reply := &RequestVoteReply{}
					go func() {
						ret := rf.sendRequestVote(i, args, reply)
						rf.mu.Lock()
						if ret {
							countAllReply++
							if reply.VoteGranted {
								countVote++
							}
							if reply.Term > rf.currentTerm { //discover server with highter term
								rf.state = Follower
							}
						}
						rf.mu.Unlock()
					}()
				}
			}
			rf.mu.Unlock()

			//接收
			for true {
				flag := false
				rf.mu.Lock()
				//有可能收到AE,或者AV而变成follower，可以直接退出,或者断定肯定没有没有majority
				last := (rf.peerNumber - countAllReply)
				if rf.getState() == Follower { //收到AE变成follower
					flag = true
				} else if time.Now().UnixNano()-startElectTime > electionTimeout {
					rf.state = Candidate
				} else if countVote+last < rf.majority { //肯定输保持Candidate
					flag = true
				} else if countVote >= rf.majority {
					rf.state = Follower
					flag = true
				} else if countAllReply == rf.peerNumber && countVote < rf.majority { //所有票到，一定输了
					flag = true
				}
				rf.mu.Unlock()
				if flag { //dont forget releasing the lock
					break
				}
			}
			//如果不成功就要继续选举
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

	// Your initialization code here (2A, 2B, 2C).
	rf.peerNumber = len(peers)
	rf.majority = (rf.peerNumber / 2) + 1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{}) //让第一个log下标为1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.lastHeartBeatTime = time.Now().UnixNano()
	rf.clusterState = C_o
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
