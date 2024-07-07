package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type dupTable struct {
	seq   int
	value string
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	kvDb        map[string]string
	clientTable map[int64]*dupTable
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.clientTable[args.ClientId] == nil {
		reply.Value = kv.kvDb[args.Key]
		return
	}
	dt := kv.clientTable[args.ClientId]
	if dt.seq == args.Seq {
		reply.Value = dt.value
		return
	}
	dt.seq = args.Seq
	dt.value = kv.kvDb[args.Key]
	reply.Value = dt.value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.clientTable[args.ClientId] == nil {
		kv.kvDb[args.Key] = args.Value
		//kv.clientTable[args.ClientId] = &dupTable{-1, ""}
		return
	}
	dt := kv.clientTable[args.ClientId]
	if dt.seq == args.Seq {
		DPrintf("Server: ClientId [%v] Put req [%v] repeat!!\n",
			args.ClientId, args.Seq)
		return
	}
	DPrintf("Server: ClientId [%v] Put req [%v], the value is [%v]\n",
		args.ClientId, args.Seq, args.Value)
	dt.seq = args.Seq
	dt.value = kv.kvDb[args.Key]
	kv.kvDb[args.Key] = args.Value
}

// append new value to value and return old value
func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.clientTable[args.ClientId] == nil {
		kv.clientTable[args.ClientId] = &dupTable{-1, ""}
	}
	dt := kv.clientTable[args.ClientId]

	if dt.seq == args.Seq {
		// 重复的请求
		DPrintf("Server: ClientId [%v] Append req [%v] repeat!! the value is [%v]\n",
			args.ClientId, args.Seq, dt.value)
		reply.Value = dt.value
		return
	}
	dt.seq = args.Seq
	dt.value = kv.kvDb[args.Key]
	reply.Value = dt.value

	kv.kvDb[args.Key] = kv.kvDb[args.Key] + args.Value
	DPrintf("Server: ClientId [%v] Append req [%v], return value is [%v]\n",
		args.ClientId, args.Seq, dt.value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvDb = map[string]string{}
	kv.clientTable = map[int64]*dupTable{}
	return kv
}
