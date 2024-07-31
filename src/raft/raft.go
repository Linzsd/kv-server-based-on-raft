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
	"6.5840/labgob"
	"bytes"
	//	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int // 日志索引
}

const (
	ELECTION_TIMEOUT = 200
	HEARTBEAT        = 100 * time.Millisecond
)

type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	role           Role
	electionTimer  *time.Ticker
	heartBeatTimer *time.Ticker

	applyChan    chan ApplyMsg
	applyMsgCond *sync.Cond
	isSnapshot   uint32 // 是否正在保存快照 为了维持日志与快照顺序一致
	isAppendLog  uint32 // 是否正在加入日志到applychan
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.role == Leader
	return term, isleader
}

func (rf *Raft) changeState(role Role) {
	if role == Follower {
		DPrintf("{Server %v} Term [%v], %v ==> Follower\n", rf.me, rf.currentTerm, rf.role)
		rf.resetElectionTimeout()
		rf.heartBeatTimer.Stop()
	} else if role == Leader {
		DPrintf("{Server %v} Term [%v], %v ==> Leader\n", rf.me, rf.currentTerm, rf.role)
		lastLog := rf.getLastLog()
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		}
		rf.electionTimer.Stop()
		rf.heartBeatTimer.Reset(HEARTBEAT)
	}
	rf.role = role
}

// 与快照相关，用了快照后，日志索引就不是rf.log[]的索引了
func (rf *Raft) getFirstLog() LogEntry {
	return rf.log[0]
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.log[len(rf.log)-1]
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapshot []byte) {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []LogEntry
	var voteFor, currentTerm int
	if d.Decode(&logs) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&currentTerm) != nil {
		panic("decode persist state failed!")
	}
	rf.log = logs
	rf.votedFor = voteFor
	rf.currentTerm = currentTerm
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("{Server %v} Term [%v] snapshot index = %d.\n", rf.me, rf.currentTerm, index)
	lastIncludeIndex := rf.getFirstLog().Index
	if lastIncludeIndex >= index {
		// 已经做过快照 return
		DPrintf("{Server %v} Term [%v] outdate snapshot index = %d, lastIncludeIndex = %d.\n",
			rf.me, rf.currentTerm, index, lastIncludeIndex)
		return
	}
	var tmp []LogEntry
	rf.log = append(tmp, rf.log[index-lastIncludeIndex:]...)
	// lastIncludedIndex和lastIncludedTerm都保存在这里, log[0]
	rf.log[0].Command = nil
	rf.persist(snapshot)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int    // 快照中最后一个条目包含的索引
	LastIncludedTerm  int    // 快照中最后一个条目包含的任期
	Snapshot          []byte // 快照
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	// 任期过期
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm = args.Term
	rf.changeState(Follower)

	firstIndex := rf.getFirstLog().Index
	// 说明是过期的快照请求
	if rf.commitIndex >= args.LastIncludedIndex {
		DPrintf("{Server %v} Term [%v] reveive outdate snapshot. commitIndex=%d, LastIncludedIndex=%d.\n",
			rf.me, rf.currentTerm, rf.commitIndex, args.LastIncludedIndex)
		rf.mu.Unlock()
		return
	}

	// 如果前面有快照正在保存 那么直接丢弃此快照 不然可能发生以下情况
	// 第一个快照生成并解锁 但是还没有应用成功 第二个快照生成解锁 并应用
	// 第一个快照 应用 因为第一个快照内容更少 相当于日志队列发生了回退
	// 所以这里第一个快照如果在应用 第二个直接丢弃 不再应用
	if atomic.LoadUint32(&rf.isSnapshot) == 1 {
		reply.Term = -1
		rf.mu.Unlock()
		return
	}

	if rf.getLastLog().Index <= args.LastIncludedIndex {
		rf.log = make([]LogEntry, 1)
	} else {
		var tmp []LogEntry
		rf.log = append(tmp, rf.log[args.LastIncludedIndex-firstIndex:]...)
	}

	rf.log[0].Term = args.LastIncludedTerm
	rf.log[0].Index = args.LastIncludedIndex
	rf.log[0].Command = nil
	rf.persist(args.Snapshot)
	rf.lastApplied, rf.commitIndex = args.LastIncludedIndex, args.LastIncludedIndex
	atomic.StoreUint32(&rf.isSnapshot, 1)
	rf.mu.Unlock()
	for atomic.LoadUint32(&rf.isAppendLog) == 1 {
		time.Sleep(10 * time.Millisecond)
	}
	rf.mu.Lock()
	DPrintf("{Server %v} Term [%v] start append snapshot.\n", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	rf.applyChan <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	atomic.StoreUint32(&rf.isSnapshot, 0)
	rf.mu.Lock()
	DPrintf("{Server %v} Term [%v] end append snapshot.\n", rf.me, rf.currentTerm)
	rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) genRequestVoteArgs() RequestVoteArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm:  rf.getLastLog().Term,
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist(rf.persister.ReadSnapshot())
	// 有以下几种情况
	// term < currentTerm: 对端的term小于自己的term，拒绝请求
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// term > currentTerm: 对端的raft一直发送投票请求，但它无法收到本地的应答，导致一直超时->选举->term+1
	if args.Term > rf.currentTerm {
		if rf.role != Follower {
			// 看论文状态图
			// 只有给其他节点投票成功才重置选举计时器，或者当前节点是过期leader
			rf.changeState(Follower)
		}
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	// 还未投票 或者 可能重传rpc
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// 2B限制：日志需要是最新的
		if rf.isLogMatch(args.LastLogIndex, args.LastLogTerm) {
			DPrintf("{Server %v} Term [%v] vote for %d.\n", rf.me, rf.currentTerm, args.CandidateId)
			rf.changeState(Follower)
			rf.votedFor = args.CandidateId // 给候选人投票
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

// 论文原话：通过⽐较两份日志中最后⼀条日志条目的索引值和任期号定义谁的日志⽐较新。如果两份
// 日志最后的条⽬的任期号不同，那么任期号⼤的⽇志更加新。如果两份⽇志最后的条⽬任期号
// 相同，那么⽇志⽐较⻓的那个就更加新
func (rf *Raft) isLogMatch(lastLogIndex int, lastLogTerm int) bool {
	lastLog := rf.getLastLog()
	if lastLogTerm > lastLog.Term || (lastLogTerm == lastLog.Term && lastLogIndex >= lastLog.Index) {
		return true
	}
	return false
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        //  leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XIndex  int
	XTerm   int
}

func (rf *Raft) applyMsg() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyMsgCond.Wait()
		}
		applyEntries := make([]LogEntry, rf.commitIndex-rf.lastApplied)
		firstIndex := rf.getFirstLog().Index
		commitIndex := rf.commitIndex
		copy(applyEntries, rf.log[rf.lastApplied-firstIndex+1:rf.commitIndex-firstIndex+1])
		if atomic.LoadUint32(&rf.isSnapshot) == 1 {
			atomic.StoreUint32(&rf.isAppendLog, 0)
			rf.mu.Unlock()
			continue
		}
		atomic.StoreUint32(&rf.isAppendLog, 1)
		rf.mu.Unlock()
		for _, entry := range applyEntries {
			// 这是给kv层使用的，kv层从chan中取出appylyMsg，然后执行apply
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}
		atomic.StoreUint32(&rf.isAppendLog, 0)
		rf.mu.Lock()
		// 这里不能使用rf.commitIndex 因为有可能apply执行到这里 这个时候本节点又更新了一次rf.commitIndex
		// 那么第一次的更新就丢失了 会造成部分日志没有成功apply
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

// 裁剪日志 并返回最后一个匹配的索引
func (rf *Raft) cropLogs(args *AppendEntriesArgs) int {
	// 得到第一条日志的索引
	firstIndex := rf.getFirstLog().Index
	// 遍历新加入的日志
	for i, entry := range args.Entries {
		// 一旦新加入日志条目的索引超出了日志队列的范围 那么原队列不需要裁剪 把接收到的日志中不存在的日志添加到本地日志队列
		// 如果某个新加入的日志对应的任期不等于本地日志对应的任期 那么本地日志从这个位置以及以后的日志全部丢弃
		if entry.Index >= firstIndex+len(rf.log) || rf.log[entry.Index-firstIndex].Term != entry.Term {
			var tmp []LogEntry
			rf.log = append(tmp, rf.log[:entry.Index-firstIndex]...)
			return i
		}
	}
	// 遍历到这说明所有日志都匹配 可能是接收到了重传的日志队列 那么本地日志队列不需要加入新的日志
	return len(args.Entries)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist(rf.persister.ReadSnapshot())
	// 请求的term小于本节点的term，拒绝请求 论文(5.2)
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	// 请求的term大于等于本节点的term，更新本节点的term
	rf.changeState(Follower)
	rf.currentTerm = args.Term

	// 延迟的rpc请求 丢弃
	if args.PrevLogIndex < rf.getFirstLog().Index {
		DPrintf("{Server %v} Term [%v] PrevLogIndex=[%d] lastLogIndex=[%d].\n", rf.me, rf.currentTerm, args.PrevLogIndex, rf.getLastLog().Index)
		reply.Term, reply.Success = 0, false
		return
	}
	// 上一条日志索引对应的日志在本节点不存在，或者，term对不上，拒绝请求
	if args.PrevLogIndex > rf.getLastLog().Index || rf.log[args.PrevLogIndex-rf.getFirstLog().Index].Term != args.PrevLogTerm {
		DPrintf("{Server %v} Term [%v] reject append log.\n", rf.me, rf.currentTerm)

		reply.Term, reply.Success = rf.currentTerm, false
		lastIndex := rf.getLastLog().Index
		firstIndex := rf.getFirstLog().Index
		// 按照term回退索引
		//1. follower中没有args.PrevLogIndex对应的日志，nextIndex回退到刚好匹配的索引
		if args.PrevLogIndex > lastIndex {
			reply.XIndex, reply.XTerm = lastIndex+1, -1
		} else {
			//2. follower有args.PrevLogIndex对应的日志，那么是term冲突了。
			//并且返回Xindex指向当前term的第一条索引的前一条索引(快速回退)
			//因为这个term对不上，这个term期间的日志都没用了
			reply.XTerm = rf.log[args.PrevLogIndex-firstIndex].Term
			// leader收到消息后，应该回退到Xindex + 1
			index := args.PrevLogIndex - 1
			for index >= firstIndex && rf.log[index-firstIndex].Term == reply.XTerm {
				index--
			}
			reply.XIndex = index
		}
		DPrintf("{Server %v} Term [%v] reply = %v.\n", rf.me, rf.currentTerm, *reply)
		return
	}
	// 添加日志
	DPrintf("{Server %v} Term [%v] start append log.\n", rf.me, rf.currentTerm)
	firstIndex := rf.cropLogs(args)
	rf.log = append(rf.log, args.Entries[firstIndex:]...)
	DPrintf("{Server %v} Term [%v] append log sucess.\n", rf.me, rf.currentTerm)
	newCommitIndex := min(args.LeaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex {
		DPrintf("{Server %v} Term [%v] commit.\n", rf.me, rf.currentTerm)
		rf.commitIndex = newCommitIndex
		// commit
		rf.applyMsgCond.Signal()
	}
	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) genAppendEntriesArgs(peer int) *AppendEntriesArgs {
	firstIndex := rf.getFirstLog().Index
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[peer] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[peer]-1-firstIndex].Term,
		Entries:      cloneLogs(rf.log[rf.nextIndex[peer]-firstIndex:]),
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *Raft) sendEntries() {
	for i := range rf.peers {
		if rf.me != i {
			// 并发给每个peer发送心跳
			go func(peer int) {
				_, isLeader := rf.GetState()
				if !isLeader {
					return
				}
				rf.mu.Lock()
				firstLog := rf.getFirstLog()
				// follower的log远远落后，leader回退到头了，发快照。
				if rf.nextIndex[peer] <= firstLog.Index {
					DPrintf("{Server %v} Term [%v] Send snapshot to [%d], nextIndex=%d, firstIndex=%d.\n",
						rf.me, rf.currentTerm, peer, rf.nextIndex[peer], firstLog.Index)
					args := &InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: firstLog.Index,
						LastIncludedTerm:  firstLog.Term,
						Snapshot:          rf.persister.ReadSnapshot(),
					}
					rf.mu.Unlock()
					reply := &InstallSnapshotReply{}
					if rf.sendInstallSnapshot(peer, args, reply) {
						rf.mu.Lock()
						rf.handleInstallSnapshotResponse(peer, args, reply)
						rf.mu.Unlock()
					}
				} else {
					args := rf.genAppendEntriesArgs(peer)
					reply := &AppendEntriesReply{}
					DPrintf("{Server %v} Term [%v] Send AppendEntries.\n", rf.me, rf.currentTerm)
					rf.mu.Unlock()
					if rf.sendAppendEntries(peer, args, reply) {
						rf.mu.Lock()
						rf.handleAppendEntriesResponse(peer, args, reply)
						rf.mu.Unlock()
					}
				}
			}(i)
		}
		// leader回退到
	}
}

func (rf *Raft) handleInstallSnapshotResponse(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.role == Leader && rf.currentTerm == args.Term {
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.changeState(Follower)
			rf.persist(rf.persister.ReadSnapshot())
			return
		} else if rf.currentTerm == reply.Term {
			rf.matchIndex[peer] = max(rf.matchIndex[peer], args.LastIncludedIndex)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		}
	}
}

func (rf *Raft) handleAppendEntriesResponse(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 可能期间重新当选了leader，所以判断rf.currentTerm == args.Term
	if rf.role == Leader && rf.currentTerm == args.Term {
		//说明leader已经过期
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.changeState(Follower)
			rf.persist(rf.persister.ReadSnapshot())
		} else {
			if reply.Success {
				rf.matchIndex[peer] = max(rf.matchIndex[peer], args.PrevLogIndex+len(args.Entries))
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
				rf.updateLeaderCommitIndex()
			} else if reply.Term == rf.currentTerm {
				firstIndex := rf.getFirstLog().Index
				rf.nextIndex[peer] = max(reply.XIndex, rf.matchIndex[peer]+1)
				// 是term冲突
				if reply.Term != -1 {
					// reply.XIndex是有可能比firstIndex大的
					// 也就是论文figure 7的其中一种情况
					boundary := max(firstIndex, reply.XIndex)
					// 回退，在本地找到与对端节点冲突的term
					for i := args.PrevLogIndex; i >= boundary; i-- {
						if rf.log[i-firstIndex].Term == reply.XTerm {
							rf.nextIndex[peer] = i + 1
							break
						}
					}
				}
				DPrintf("{Server %v} Term [%v] update server[%d]'s nextIndex=%d.\n", rf.me, rf.currentTerm, peer, rf.nextIndex[peer])
			}
		}
	}
}

func (rf *Raft) updateLeaderCommitIndex() {
	n := len(rf.matchIndex)
	sortMatchIndex := make([]int, n)
	copy(sortMatchIndex, rf.matchIndex)
	sortMatchIndex[rf.me] = rf.getLastLog().Index
	sort.Ints(sortMatchIndex)

	// 排序后从小到大，直接找中间的节点的commitIndex，来确认是否日志被复制到大多数，即可以提交。
	newCommitIndex := sortMatchIndex[n/2]
	if newCommitIndex > rf.commitIndex && newCommitIndex <= rf.getLastLog().Index &&
		rf.log[newCommitIndex-rf.getFirstLog().Index].Term == rf.currentTerm {
		DPrintf("{Server %v} Term [%v] update commitIndex[%d] to [%d].\n", rf.me, rf.currentTerm, rf.commitIndex, newCommitIndex)
		rf.commitIndex = newCommitIndex
		rf.applyMsgCond.Signal()
	}
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

	// Your code here (3B).
	term, isLeader := rf.GetState()
	if !isLeader {
		return -1, -1, isLeader
	}
	rf.mu.Lock()
	DPrintf("{Server %v} Term [%v] receive a command [%v].\n", rf.me, rf.currentTerm, command)
	defer rf.mu.Unlock()
	entry := LogEntry{
		Term:    term,
		Command: command,
		Index:   rf.getLastLog().Index + 1,
	}
	rf.log = append(rf.log, entry)
	rf.persist(rf.persister.ReadSnapshot())
	index := entry.Index
	rf.sendEntries()
	return index, term, isLeader
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

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.mu.Unlock()
	rf.persist(rf.persister.ReadSnapshot())
	args := rf.genRequestVoteArgs()
	voteCount := 1
	rf.mu.Lock()
	DPrintf("{Server %v} Term [%v] ready to send vote request.\n", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				reply := RequestVoteReply{}
				if rf.sendRequestVote(server, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// 为什么要确认是否是候选人？因为在等待投票应答时，可能会收到来自更高或相等term的leader的心跳，导致自己变成follower
					if rf.role == Candidate && reply.VoteGranted {
						voteCount++
						// 获得大多数的投票
						if voteCount > len(rf.peers)/2 {
							rf.changeState(Leader)
							DPrintf("{Server %v} Term [%v] Success become leader.\n",
								rf.me, rf.currentTerm)
							rf.sendEntries()
						}
					} else if reply.Term > rf.currentTerm {
						// 有节点更快的成为了leader，并发送了心跳，导致本节点成为follower
						DPrintf("{Server %v} Old Term [%v] discovers a new term %v, convert to follower\n",
							rf.me, rf.currentTerm, reply.Term)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.changeState(Follower)
						rf.persist(rf.persister.ReadSnapshot())
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimer.Reset(randomElectionTime())
}

func randomElectionTime() time.Duration {
	// 超时时间偏差 0 ~ 200 ms
	ms := rand.Int63() % 200
	// 超时计时器 200 ~ 400 ms
	eleTime := time.Duration(ELECTION_TIMEOUT+ms) * time.Millisecond
	return eleTime
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.changeState(Candidate)
			rf.resetElectionTimeout()
			DPrintf("{Server %v} Term [%v] start election\n", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			rf.startElection()
		case <-rf.heartBeatTimer.C:
			rf.mu.Lock()
			if rf.role == Leader {
				DPrintf("{Server %v} Term [%v] send heartbeat\n", rf.me, rf.currentTerm)
				rf.sendEntries()
			}
			rf.mu.Unlock()
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.role = Follower
	rf.electionTimer = time.NewTicker(randomElectionTime())
	rf.heartBeatTimer = time.NewTicker(HEARTBEAT)
	rf.heartBeatTimer.Stop()

	rf.applyMsgCond = sync.NewCond(&rf.mu)
	rf.applyChan = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastApplied = rf.getFirstLog().Index
	rf.commitIndex = rf.getFirstLog().Index
	lastLog := rf.getLastLog()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLog.Index + 1
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyMsg()

	return rf
}
