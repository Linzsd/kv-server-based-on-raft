package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64  // 客户端Id
	RequestId int64  // 请求Id
	OpType    string // 操作类型PutAppend/GET
	Key       string // 添加的Key
	Value     string // 添加的Value
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	stateMachine   *MemoryKV                 // 状态机，存储kv
	notifyChanMap  map[int]chan *CommonReply // 用于通知的chan, key = index val = chan
	lastRequestMap map[int64]ReplyContext    // 缓存每个客户端对应的最近请求和reply key = clientId
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    GET,
		Key:       args.Key,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("server{%d}: raft's term = %d, index = %d, get args = %v", kv.me, term, index, args)
	kv.mu.Lock()
	kv.notifyChanMap[index] = make(chan *CommonReply, 1)
	notifyChan := kv.notifyChanMap[index]
	kv.mu.Unlock()

	DPrintf("server{%d}: wait notify or timeout", kv.me)
	select {
	case result := <-notifyChan:
		DPrintf("server{%d}: get result = %v", kv.me, result)
		reply.Value, reply.Err = result.Value, result.Err
		currentTerm, isLeader := kv.rf.GetState()
		// 通过start传入日志后，可能发生了当前leader掉线且快速重启，重新选举了leader。
		if !isLeader || currentTerm != term {
			DPrintf("server{%d}: leader changed, currentTerm=%d, log term=%d", kv.me, currentTerm, term)
			reply.Err = ErrWrongLeader
		}
	case <-time.After(ExecuteTimeout):
		DPrintf("server{%d}: time out!", kv.me)
		reply.Err = TimeOut
	}
	kv.mu.Lock()
	delete(kv.notifyChanMap, index)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.isOldRequest(args.ClientId, args.RequestId) {
		DPrintf("server{%d}: get old request", kv.me)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
	}
	// index=最新的日志索引
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("server{%d}: raft's term = %d, index = %d, get PutAppend args = %v", kv.me, term, index, args)
	kv.mu.Lock()
	kv.notifyChanMap[index] = make(chan *CommonReply, 1)
	notifyChan := kv.notifyChanMap[index]
	kv.mu.Unlock()
	DPrintf("server{%d}: wait notify or timeout", kv.me)
	select {
	case result := <-notifyChan:
		reply.Err = result.Err
		currentTerm, isLeader := kv.rf.GetState()
		// 通过start传入日志后，可能发生了当前leader掉线且快速重启，重新选举了leader。
		if !isLeader || currentTerm != term {
			DPrintf("server{%d}: leader changed, currentTerm=%d, log term=%d", kv.me, currentTerm, term)
			reply.Err = ErrWrongLeader
		}
	case <-time.After(ExecuteTimeout):
		DPrintf("server{%d}: time out!", kv.me)
		reply.Err = TimeOut
	}
	kv.mu.Lock()
	delete(kv.notifyChanMap, index)
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.stateMachine = &MemoryKV{make(map[string]string)}
	kv.notifyChanMap = make(map[int]chan *CommonReply)
	kv.lastRequestMap = make(map[int64]ReplyContext)
	go kv.applier()
	DPrintf("server{%d}: start", kv.me)
	return kv
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case applyMsg := <-kv.applyCh:
			DPrintf("server{%d}: receive applyMsg = %v", kv.me, applyMsg)
			kv.mu.Lock()
			if applyMsg.CommandValid {
				reply := kv.apply(applyMsg.Command)

				currentTerm, isLeader := kv.rf.GetState()
				// 如果不是leader，或者是分区的旧leader，就不用给client应答
				if isLeader && applyMsg.CommandTerm == currentTerm {
					kv.notify(applyMsg.CommandIndex, reply)
				}

			} else {
				panic("wrong applyMsg")
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) apply(cmd interface{}) *CommonReply {
	reply := &CommonReply{}
	op := cmd.(Op)
	DPrintf("server{%d}: apply command = %v\n", kv.me, op)
	// 有可能出现这边刚执行到这里 然后另一边重试 进来了重复命令 这边还没来得及更新 那边判断重复指令不重复
	// 因此需要在应用日志之前再过滤一遍日志 如果发现有重复日志的话 那么就直接返回OK
	if op.OpType != GET && kv.isOldRequest(op.ClientId, op.RequestId) {
		reply.Err = OK
	} else {
		reply = kv.applyLogToStateMachine(&op)
		if op.OpType != GET {
			kv.updateLastRequest(&op, reply)
		}
	}

	reply.Err = OK
	return reply
}

func (kv *KVServer) isOldRequest(clientId int64, requestId int64) bool {
	if cxt, ok := kv.lastRequestMap[clientId]; ok {
		if requestId <= cxt.LastRequestId {
			return true
		}
	}
	return false
}

func (kv *KVServer) updateLastRequest(op *Op, reply *CommonReply) {
	ctx := ReplyContext{
		LastRequestId: op.RequestId,
		Reply:         *reply,
	}

	lastCtx, ok := kv.lastRequestMap[op.ClientId]
	if (ok && lastCtx.LastRequestId < op.RequestId) || !ok {
		kv.lastRequestMap[op.ClientId] = ctx
	}
}

func (kv *KVServer) applyLogToStateMachine(op *Op) *CommonReply {
	var reply = &CommonReply{}

	switch op.OpType {
	case GET:
		reply.Value = kv.stateMachine.get(op.Key)
	case PUT:
		kv.stateMachine.put(op.Key, op.Value)
	case APPEND:
		reply.Value = kv.stateMachine.append(op.Key, op.Value)
	}

	reply.Err = OK

	return reply
}

func (kv *KVServer) notify(index int, reply *CommonReply) {
	DPrintf("server{%d}: notify index = %d\n", kv.me, index)
	if notifyCh, ok := kv.notifyChanMap[index]; ok {
		notifyCh <- reply
	}
}
