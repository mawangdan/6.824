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
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	BroadcastTime       time.Duration = time.Second / 8 //一秒8次heartbeat
	BroadcastTime1_5    time.Duration = BroadcastTime * 3 / 2
	MaxElectionTimeout  time.Duration = BroadcastTime * 3
	MinElectionTimeout  time.Duration = BroadcastTime * 2
	ElectionMaxInterval time.Duration = MaxElectionTimeout - MinElectionTimeout
	Millisecond10       time.Duration = 10 * time.Millisecond
	Millisecond3        time.Duration = 3 * time.Millisecond
)

//If a candidate or leader discovers
//that its term is out of date, it immediately reverts to follower state.
type ServerState int32

const (
	Leader ServerState = iota
	Candidate
	Follower
)

func (ss ServerState) String() string {
	if ss == Follower {
		return "Follower"
	} else if ss == Candidate {
		return "Candidate"
	} else if ss == Leader {
		return "Leader"
	} else {
		return "Unknown"
	}
}

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
	me                int64               // this peer's index into peers[]
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
	nextIndex  []int //下一个log将要放的位置
	matchIndex []int
	//help client to redirect the leader
	leaderId int
	applyCh  chan ApplyMsg
}

//need lock
func (rf *Raft) String() string {
	s := fmt.Sprintf("Raft{currentTerm: %d,votedFor: %d,commitIndex: %d,lastApplied: %d}", rf.currentTerm, rf.votedFor, rf.commitIndex, rf.lastApplied)
	return s
}

// Log Entry
type LogEntry struct {
	Command interface{}
	Term    int
}

func (le *LogEntry) String() string {
	s := fmt.Sprintf("LogEntry{Command :%d,Term :%d}", le.Command, le.Term)
	return s
}

type AppendEntriesArgs struct {
	Seq int //每个Rpc交互一个

	Term     int //leader's term
	LeaderId int
	//下面两项用于检查冲突
	//index of log entry which is immediately preceding new ones
	PrevLogIndex int
	PrevLogTerm  int

	Entries      []LogEntry
	LeaderCommit int
}

func (aea *AppendEntriesArgs) String() string {
	e := "<"
	for i, v := range aea.Entries {
		le := fmt.Sprintf("[%d]%v,", i, v)
		e += le
	}
	e += ">"
	s := fmt.Sprintf("AppendEntriesArgs{Seq :%d,Term :%d,LeaderId :%d,PrevLogIndex:%d,PervLogTerm :%d,Entries :%v,LeaderCommit:%d}",
		aea.Seq,
		aea.Term,
		aea.LeaderId,
		aea.PrevLogIndex,
		aea.PrevLogTerm,
		e,
		aea.LeaderCommit)
	return s
}

type AppendEntriesReply struct {
	Seq int //每个Rpc交互一个

	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm

}

func (aer *AppendEntriesReply) String() string {
	s := fmt.Sprintf("AppendEntriesReply{Seq :%d,Term :%d,Success :%t}", aer.Seq, aer.Term, aer.Success)
	return s
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
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
	Seq int //每个Rpc交互一个
	// Your data here (2A, 2B).
	Term        int //candidate’s term
	CandidateId int //candidate requesting vote
	//The next two fields are used for elect restriction
	LastLogIndex int //index of candidate’s last log entry (§5.4)
	LastLogTerm  int //term of candidate’s last log entry (§5.4)
}

func (rva *RequestVoteArgs) String() string {
	s := fmt.Sprintf("RequestVoteArgs{Seq: %d,Term: %d,CandidateId: %d,LastLogIndex: %d,LastLogTerm: %d}", rva.Seq, rva.Term, rva.CandidateId, rva.LastLogIndex, rva.LastLogTerm)
	return s
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Seq int //每个Rpc交互一个
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

func (rvr *RequestVoteReply) String() string {
	s := fmt.Sprintf("RequestVoteReply{Seq: %d,Term: %d,VoteGranted: %t}", rvr.Seq, rvr.Term, rvr.VoteGranted)
	return s
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Seq = args.Seq
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.pLog(LogRVRev, "%d<--%d  %v", rf.getMe(), args.CandidateId, args)
	//defer这里顺序错会出问题
	defer func() {
		rf.pLog(LogRVSend, "%d-->%d  %v", rf.getMe(), args.CandidateId, reply)
	}()
	reply.Term = rf.currentTerm
	// Your code here (2A, 2B).
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}

	oldTerm := rf.currentTerm
	rf.currentTerm = args.Term

	//Election restriction
	// the
	// voter denies its vote if its own log is more up-to-date than
	// that of the candidate.
	if rf.getLastLog().Term > args.LastLogTerm {
		reply.VoteGranted = false
		return
	} else if rf.getLastLog().Term == args.LastLogTerm && rf.getLastLogIndex() > args.LastLogIndex {
		reply.VoteGranted = false
		return
	}

	//其他情况可能同意
	if oldTerm < args.Term {
		rf.votedFor = args.CandidateId
		rf.state = Follower //老的leader收到新投票的请求应该变成follower
		reply.VoteGranted = true
		rf.pLog(LogRVBody, "%v", rf)
	} else if oldTerm == args.Term && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) { //或者还没投或者已经投给他了
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else { //否则不投给他
		reply.VoteGranted = false
	}
	return
}

//AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Seq = args.Seq
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.pLog(LogAERev, "%d<--%d  %v", rf.getMe(), args.LeaderId, args)
	//defer这里顺序错会出问题
	defer func() {
		//避免defer预计算参数
		rf.pLog(LogAESend, "%d-->%d  %v", rf.getMe(), args.LeaderId, reply)
	}()

	reply.Success = true
	reply.Term = rf.currentTerm
	// 	If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = Follower
	} else if rf.currentTerm > args.Term {
		reply.Success = false
		return
	} else { //eq discovers current leader
		rf.state = Follower
		rf.lastHeartBeatTime = time.Now().UnixNano()
	}
	// 	If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	//如果index of last new entry理解为rf.getLastLogIndex()
	// 果然这里有问题:如果心跳包收到一个大的cmtIndex,而自己的log还是旧的还没和leader形成共识
	//论文放在LR(AE)的最后一步没看清楚
	// if args.LeaderCommit > rf.commitIndex {
	// 	rf.setCommitIndex(min(args.LeaderCommit, rf.getLastLogIndex()))
	// }

	//TODO:
	// 	If commitIndex > lastApplied: increment lastApplied, apply
	// 	log[lastApplied] to state machine (§5.3)
	//	无论是不是心跳包，都需要apply

	if args.Entries == nil { //heart beat
		rf.lastHeartBeatTime = time.Now().UnixNano()
		rf.leaderId = args.LeaderId
		return
	} else { //否则是正常报文
		//TODO:other cases

		//TODO:Reply false if log doesn’t contain an entry at prevLogIndex
		// whose(That Entry) term matches prevLogTerm (§5.3)
		if rf.getLastLogIndex() < args.PrevLogIndex ||
			rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { //
			reply.Success = false
			return
		} else { //前面的没有矛盾了
			// If an existing entry conflicts with a new one (same index
			// 	but different terms), delete the existing entry and all that
			// 	follow it (§5.3)
			//TODO:首先处理后面的矛盾
			//子切片左闭右开
			rf.log = rf.log[0 : args.PrevLogIndex+1]
			//直接接上去
			rf.log = append(rf.log, args.Entries...)
			reply.Success = true
			rf.pLog(LogAEBody, "%v", rf)
			//论文的第五步
			// 	If leaderCommit > commitIndex, set commitIndex =
			// min(leaderCommit, index of last new entry)
			if args.LeaderCommit > rf.commitIndex {
				rf.setCommitIndex(min(args.LeaderCommit, rf.getLastLogIndex()))
			}
		}
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
	seq := getRpcn()
	args.Seq = seq
	rf.pLogLock(LogRVSend, "%d-->%d  %v", rf.getMe(), server, args)

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	defer func() {
		if ok {
			rf.mu.Lock()
			rf.pLog(LogRVRev, "%d<--%d  %v", rf.getMe(), server, reply)
			if reply.Term > rf.currentTerm { //discover server with highter term
				rf.state = Follower
				rf.currentTerm = reply.Term
			}
			rf.mu.Unlock()
		} else {
			rf.pLogLock(LogRVRev, "%d<--%d  %v", rf.getMe(), server, "报文接收失败")
		}

	}()
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	seq := getRpcn()
	args.Seq = seq
	rf.mu.Lock()
	rf.pLog(LogAESend, "%d-->%d  %v %v", rf.getMe(), server, args, rf)
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	defer func() {
		if ok {
			rf.mu.Lock()
			rf.pLog(LogAERev, "%d<--%d  %v", rf.getMe(), server, reply)
			if reply.Success == false && reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
			}
			rf.mu.Unlock()
		} else {
			rf.pLogLock(LogAERev, "%d<--%d  %v", rf.getMe(), server, "报文接收失败")
		}
	}()
	return ok
}

//send heart beat
func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	rf.pLog(LogHeartBeat, "sendHeartBeat")
	pn := rf.peerNumber
	me := rf.getMe()
	for i := 0; i < pn; i++ {
		server := i
		//对每个follower发送心跳
		if server == me { //给自己发心跳
			go func() {
				for true {
					if rf.killed() {
						break
					}
					rf.mu.Lock()
					state := rf.state
					rf.mu.Unlock()
					if state != Leader {
						break
					}
					rf.mu.Lock()
					rf.lastHeartBeatTime = time.Now().UnixNano()
					rf.mu.Unlock()
					time.Sleep(BroadcastTime)
				}
			}()
			continue
		}
		go func() {
			for true {
				//死亡或者就停止发心跳
				if rf.killed() {
					break
				}

				rf.mu.Lock()
				state := rf.state
				currentTerm := rf.currentTerm
				commitIndex := rf.commitIndex
				rf.mu.Unlock()
				//不是leader停止发心跳
				if state != Leader {
					break
				}
				args := &AppendEntriesArgs{-1, currentTerm, rf.getMe(), -1, -1, nil, commitIndex}
				reply := &AppendEntriesReply{}
				go func() {
					rf.sendAppendEntries(server, args, reply)
				}()

				<-hbChan//等hb回复，超时或者失败也有回复
				//两种情况，一种是term出问题，一种是perIdx冲突
				//RPC里处理了情况1,情况二下面处理
				rf.mu.Lock()
				if reply.Success == false && rf.state == Leader {
					go rf.LogReplicaToServer(server, nil, nil)
				}
				rf.mu.Unlock()
				//ul
				time.Sleep(BroadcastTime)
			}
		}()
	}
	rf.mu.Unlock()
}

//需要锁
func (rf *Raft) commitIndexChange(c int) {
	if c > rf.commitIndex {
		rf.commitIndex = c
	}
	la := rf.lastApplied
	for i := la + 1; i <= rf.commitIndex; i++ {
		rf.pLog(LogApply, "send apply %v", rf.log[i])
		rf.lastApplied = i
		rf.applyCh <- ApplyMsg{true, rf.log[i].Command, i, false, nil, -1, -1}
		rf.pLog(LogApply, "finish apply %v", rf.log[i])
	}
}

func (rf *Raft) LogReplication(index int) {
	rf.mu.Lock()
	rf.pLog(LogRP, "开始LogReplication")
	cntReplicated := 1 //1 for itself
	waitChn := make(chan bool, rf.peerNumber)
	for i := 0; i < rf.peerNumber; i++ {
		server := i
		if server != rf.getMe() {
			// 	the leader must find the latest log entry where the two
			// logs agree, delete any entries in the follower’s log after
			// that point, and send the follower all of the leader’s entries
			// after that point.
			go func() {
				for true {
					if rf.killed() {
						break
					}
					rf.mu.Lock()
					state := rf.state
					if state != Leader {
						rf.mu.Unlock()
						break
					}

					args := &AppendEntriesArgs{}
					reply := &AppendEntriesReply{}
					args.LeaderCommit = rf.commitIndex
					args.LeaderId = rf.getMe()
					args.PrevLogIndex = rf.nextIndex[server] - 1
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term

					args.Term = rf.currentTerm
					for eIndex := rf.nextIndex[server]; eIndex <= rf.getLastLogIndex(); eIndex++ {
						args.Entries = append(args.Entries, rf.log[eIndex])
					}
					rf.mu.Unlock()
					ok := rf.sendAppendEntries(server, args, reply)
					rf.mu.Lock()
					if ok {
						if reply.Success {
							//被正常replicate
							cntReplicated++
							rf.mu.Unlock()
							break
						} else {
							// After a rejection, the leader decrements nextIndex and retries
							// the AppendEntries RPC. Eventually nextIndex will reach
							// a point where the leader and follower logs match.
							rf.nextIndex[server]--
						}
					}
					rf.mu.Unlock()
				}
				//wakeup 下面的代码
				waitChn <- true
			}()
		}
	}
	rf.mu.Unlock()

	//等待majority
	for i := 0; i < rf.peerNumber-1; i++ {
		<-waitChn //阻塞
		if rf.killed() {
			break
		}

		flag := false

		rf.mu.Lock()
		state := rf.state
		if state != Leader {
			flag = true
		}
		if state == Leader && cntReplicated >= rf.majority {
			rf.commitIndexChange(index)
			flag = true
		}
		rf.mu.Unlock()

		if flag {
			break
		}
	}

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
	// Your code here (2B).
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	if state != Leader {
		return index, term, false
	}
	rf.mu.Lock()
	rf.log = append(rf.log, LogEntry{Command: command, Term: term})
	index = rf.getLastLogIndex()
	term = rf.currentTerm
	rf.mu.Unlock()
	go rf.LogReplication(index)
	return index, term, true
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
	rf.pLogLock(LogAll, "Kill")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getMe() int {
	n := atomic.LoadInt64(&rf.me)
	return i64Toint(n)
}

//需要原子操作，否则可能越界
func (rf *Raft) getLastLog() LogEntry {
	lastLog := rf.log[len(rf.log)-1]
	return lastLog
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	me := rf.getMe()
	//electionTimeout之间的间隔最好是大于一个HB(这里取1.5)
	electionTimeout := rand.Int63n(ElectionMaxInterval.Nanoseconds())/Millisecond3.Nanoseconds()*Millisecond3.Nanoseconds() + MinElectionTimeout.Nanoseconds()
	rf.pLogLock(LogElec, "init electionTimeout:%d ms", time.Duration(electionTimeout).Milliseconds())
	for rf.killed() == false {
		//选举过程中发现任何leader发出的newterm都变成follower
		//发现的任何newterm变成follower
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//至少要睡这么久

		rf.mu.Lock()
		lastHeartBeatTime := rf.lastHeartBeatTime
		rf.mu.Unlock()
		sleepTime := electionTimeout - (time.Now().UnixNano() - lastHeartBeatTime)
		if sleepTime >= 0 {
			rf.pLogLock(LogElec, "start election sleep:%d ms", time.Duration(sleepTime).Milliseconds())
			time.Sleep(time.Duration(sleepTime))
			rf.pLogLock(LogElec, "finish election sleep")
		}
		//follower 超时没有心跳,开始选举,
		//这里要考虑选举中途会不会身份发生改变
		//1.follower->leader 不可能
		//2.follower->candidate 只可能是下面的代码
		//所以if不需要加锁
		rf.mu.Lock()
		lastHeartBeatTime = rf.lastHeartBeatTime
		state := rf.state
		rf.mu.Unlock()

		if time.Now().UnixNano()-lastHeartBeatTime >= electionTimeout && (state == Follower || state == Candidate) {
			//这个时候收到投票会怎么样
			//会votefor这个term的leader，假设这个时候选出了leader
			//不影响，因为后面term++了，所以根据状态转移，前面的leader收到更新的term会变成follower

			rf.mu.Lock()
			rf.pLog(LogElec, "开始选举")
			rf.currentTerm++
			rf.state = Candidate
			//vote for itself,这个时候
			//TODO:假设引入membership changes
			//如果状态属于C_o,C_o_n则可以为自己投票
			//如果C_n则需要知道自己是否属于new cluster
			rf.votedFor = me

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
			rpcChan := make(chan bool, rf.peerNumber)
			startElectTime := time.Now().UnixNano()
			for i := 0; i < rf.peerNumber; i++ {
				index := i
				if index != me {
					lastLog := rf.getLastLog()
					args := &RequestVoteArgs{-1, rf.currentTerm, me, len(rf.log) - 1, lastLog.Term}
					reply := &RequestVoteReply{}
					go func() {
						ret := rf.sendRequestVote(index, args, reply)
						rf.mu.Lock()
						if ret {
							countAllReply++
							if reply.VoteGranted {
								countVote++ //TODO:这里有没有可能加到下次大循环的countAllReply上面
							}
						}
						rf.mu.Unlock()

						rpcChan <- true //要先把票加上去
					}()
				}
			}
			rf.pLog(LogElec, "Vote 发送完毕")
			perN := rf.peerNumber
			rf.mu.Unlock()

			//接收majority
			//DONE: 如果自己一个人一直发起投票，goroutine会无限创建
			for p := 0; p < perN-1; p++ {
				<-rpcChan
				if rf.killed() {
					break
				}
				flag := false
				shouldSlp := false
				rf.mu.Lock()
				//有可能收到AE,或者AV而变成follower，可以直接退出,或者断定肯定没有没有majority
				last := (rf.peerNumber - countAllReply)
				retstr := ""
				if rf.state == Follower { //收到AE变成follower
					flag = true
					retstr = "选举过程收到AE或者RV变成Follower"
					rf.lastHeartBeatTime = time.Now().UnixNano() //你给别人投票了,防止又去参加选举,应该先等一下
				} else if time.Now().UnixNano()-startElectTime > electionTimeout {
					rf.state = Candidate
					flag = true
					retstr = "选举过程超时"
					shouldSlp = true
				} else if countVote+last < rf.majority { //肯定输保持Candidate
					flag = true
					retstr = "选举输重来"
					shouldSlp = true
				} else if countVote >= rf.majority {
					rf.state = Leader
					flag = true
					rf.mu.Unlock()
					//不可重入,没有这个会死锁
					// Once a candidate wins an election, it
					// becomes leader. It then sends heartbeat messages to all of
					// the other servers to establish its authority and prevent new
					// elections.
					rf.sendHeartBeat()
					rf.mu.Lock()
					rf.lastHeartBeatTime = time.Now().UnixNano()
					retstr = "选举成功 Term(" + strconv.Itoa(rf.currentTerm) + ")"
					// When a leader first comes to power,
					// it initializes all nextIndex values to the index just after the
					// last one in its log (11 in Figure 7).
					for j := 0; j < rf.peerNumber; j++ {
						rf.nextIndex[j] = rf.getLastLogIndex() + 1
					}

				} else if countAllReply == rf.peerNumber && countVote < rf.majority { //所有票到，一定输了
					flag = true
					retstr = "选举输重来"
					shouldSlp = true
				}
				rf.mu.Unlock()
				if flag { //dont forget releasing the lock
					rf.pLogLock(LogElec, retstr)
					if shouldSlp {
						time.Sleep(time.Duration(rand.Intn(15) * int(time.Millisecond)))
					}
					break
				}
			}
			//如果不成功就要继续选举
		}
	}
}

func (rf *Raft) pLogLock(lt LogType, format string, a ...interface{}) {
	rf.mu.Lock()
	state := rf.state
	term := rf.currentTerm
	rf.mu.Unlock()
	perfix := fmt.Sprintf(" Peer(%d) State(%v) LogType(%v) Term(%d) ", rf.getMe(), state, lt, term)
	DPrintf(lt, perfix, format, a...)
}

//需要加rf锁
func (rf *Raft) pLog(lt LogType, format string, a ...interface{}) {
	perfix := fmt.Sprintf(" Peer(%d) State(%v) LogType(%v) Term(%d) ", rf.getMe(), rf.state, lt, rf.currentTerm)
	DPrintf(lt, perfix, format, a...)
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
	initLog("raft.log")
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = intTo64(me)
	// Your initialization code here (2A, 2B, 2C).
	rf.peerNumber = len(peers)
	rf.majority = (rf.peerNumber / 2) + 1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{nil, 0}) //让第一个log下标为1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.lastHeartBeatTime = time.Now().UnixNano()
	rf.clusterState = C_o

	for i := 0; i < rf.peerNumber; i++ {
		rf.matchIndex = append(rf.matchIndex, -1)
		rf.nextIndex = append(rf.nextIndex, 1)
	}

	// initialize from state persisted before a crash

	rf.readPersist(persister.ReadRaftState())

	rf.pLogLock(LogAll, "rf's created!")
	rf.pLogLock(LogAll, "majority: %d", (rf.peerNumber/2)+1)
	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
