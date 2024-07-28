package kvraft

import "time"

const ExecuteTimeout = 500 * time.Millisecond

const (
	PUT    = "PUT"
	GET    = "GET"
	APPEND = "APPEND"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	TimeOut        = "TimeOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op        string
	RequestId int64
	ClientId  int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	RequestId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type CommonArgs struct {
	RequestId int64
	ClientId  int64
}

type CommonReply struct {
	Err   Err
	Value string
}

type ReplyContext struct {
	LastRequestId int64
	Reply         CommonReply
}
