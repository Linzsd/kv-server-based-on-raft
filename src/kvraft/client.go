package kvraft

import (
	"6.5840/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64
	requestId int64
	leaderId  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.requestId = 0
	ck.leaderId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	DPrintf("client{%d}: requestId = %d, Get key = %s", ck.clientId, ck.requestId, key)
	args := GetArgs{Key: key, RequestId: ck.requestId, ClientId: ck.clientId}
	reply := GetReply{}
	result := ""
	for {
		if ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply) {
			DPrintf("client{%d}: get from leader = %d reply = %v", ck.clientId, ck.leaderId, reply)
			switch reply.Err {
			case ErrWrongLeader, TimeOut:
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
				break
			case ErrNoKey, OK:
				result = reply.Value
				ck.requestId += 1
				return result
			}
		} else {
			// rpc失败，leader可能掉线了，换一个节点rpc
			DPrintf("client{%d}: GET RPC error", ck.clientId)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf("client{%d}: requestId = %d PutAppend key = %s value = %s type = %s", ck.clientId, ck.requestId, key, value, op)
	args := PutAppendArgs{Key: key, Value: value, Op: op, RequestId: ck.requestId, ClientId: ck.clientId}
	reply := PutAppendReply{}
	for {
		if ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply) {
			DPrintf("client{%d}: get from leader = %d reply = %v", ck.clientId, ck.leaderId, reply)
			switch reply.Err {
			case ErrWrongLeader, TimeOut:
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
				break
			case ErrNoKey, OK:
				ck.requestId += 1
				return
			}
		} else {
			DPrintf("client{%d}: PutAppend RPC error", ck.clientId)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "PUT")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "APPEND")
}
