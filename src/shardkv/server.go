package shardkv

import (
  "net"
  "strconv"
)
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
       if Debug > 0 {
               log.Printf(format, a...)
       }
       return
}

const (
  PUT = "Put"
  GET = "Get"
  HASH = "Hash"
  RECONFIG = "Reconfig"
)

type Op struct {
  // Your definitions here.
  CID int64
  Type string
  Key string
  Value string
  PutHash bool
  Seq int

  Num int // config number
  Shards [shardmaster.NShards]int64 // gid
  Groups map[int64][]string // gid -> servers[]
}


type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
  logTail int
  replyDic map[int64]int  //cid : seq
  replyCache map[int64]PutReply //will use putreply for now, however, it can also be getreply
  kvDB map[string]string

  curConfig shardmaster.Config//since required sequential consistency, we can have one copy locally used to check ErrWrongGroup, and update by tick()
}

func (kv *ShardKV) SendShardInfo(args *FetchArgs, reply *FetchReply) error {
	// wait till the config number is the same as reconfiguration number
	if args.ConfigNum > kv.curConfig.Num {
		reply.Err = ErrReconfigHolding
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// copy all info from the old owner to the new one
	cache := make(map[int64]PutReply)
	kvDB := make(map[string]string)
	replyDic := make(map[int64]int)

	for k, v := range kv.replyCache {
		cache[k] = v
	}

	for k, v := range kv.kvDB {
		if key2shard(k) == args.ShardNum {
			kvDB[k] = v
		}
	}

	for k, v := range kv.replyDic {
		replyDic[k] = v
	}

	reply.ReplyDic = replyDic
	reply.KVDB = kvDB
	reply.ReplyCache = cache
	reply.Err = OK

	return nil

}

func concatHash(prevStr string, newStr string) string {
  concatStr := prevStr + newStr
  res := hash(concatStr)

  return strconv.Itoa(int(res))
}

func (kv *ShardKV) catchUp(v Op) (reply PutReply) {

  switch optype := v.Type; optype {

  case PUT:

    kv.logTail = kv.logTail + 1
    kv.px.Done(kv.logTail)
    reply := &PutReply{}

    if kv.gid != kv.curConfig.Shards[key2shard(v.Key)] {
	  reply.Err = ErrWrongGroup
      return *reply
    }

    kv.kvDB[v.Key] = v.Value
    reply.Err = OK
    kv.replyCache[v.CID] = *reply
    kv.replyDic[v.CID] = v.Seq
    return *reply

  case GET:

    kv.logTail = kv.logTail + 1
    kv.px.Done(kv.logTail)
    reply := &PutReply{}

    if kv.gid != kv.curConfig.Shards[key2shard(v.Key)] {
      reply.Err = ErrWrongGroup
      return *reply
    }

    if val, ok := kv.kvDB[v.Key]; ok {
      reply.Err = OK
      reply.PreviousValue = val
    } else {
      reply.Err = ErrNoKey
      reply.PreviousValue = ""
    }
    kv.replyDic[v.CID] = v.Seq
    kv.replyCache[v.CID] = *reply
    return *reply

  case HASH:

    kv.logTail = kv.logTail + 1
    kv.px.Done(kv.logTail)
    reply := &PutReply{}

    if kv.gid != kv.curConfig.Shards[key2shard(v.Key)] {
      reply.Err = ErrWrongGroup
      return *reply
    }

    if prevStr, ok := kv.kvDB[v.Key]; ok {
      kv.kvDB[v.Key] = concatHash(prevStr, v.Value)
      reply.PreviousValue = prevStr
    } else {
      kv.kvDB[v.Key] = concatHash("", v.Value)
      reply.PreviousValue = ""
    }
    reply.Err = OK
    kv.replyDic[v.CID] = v.Seq
    kv.replyCache[v.CID] = *reply
    return *reply

  case RECONFIG:
	// After a server has moved to a new view, it can leave the shards that it is not owning in the new view undeleted.
	// If a kvserver severs a new shard, it should fetch shardInfo from the server used to own it.
    preply := &PutReply{}

    for i:= 0; i < shardmaster.NShards; i++ {
      if kv.gid != kv.curConfig.Shards[i] && kv.gid == v.Shards[i] {

        if kv.curConfig.Shards[i] == 0 {
          continue
        }

        // try each kvserver replica in the group which used to own the shard[i]
        args := &FetchArgs{ShardNum: i, ConfigNum: v.Num}
        var reply FetchReply
        count := 0
        to := 10 * time.Millisecond
        for {
          ok := call(kv.curConfig.Groups[kv.curConfig.Shards[i]][count % len(kv.curConfig.Groups)], "ShardKV.SendShardInfo", args, &reply)
          count ++
          if ok && reply.Err == OK {

            // send database to kv and update replyDic and replyCache
		    for k, v := range reply.KVDB {
			  kv.kvDB[k] = v
		    }

		    for k, v := range reply.ReplyDic {
			  if kv.replyDic[k] < v {
				  kv.replyDic[k] = v
				  kv.replyCache[k] = reply.ReplyCache[k]
			  }
		    }

            break

          } else if reply.Err == ErrReconfigHolding {
            preply.Err = ErrReconfigHolding
            return *preply
          }

          time.Sleep(to)
          if to < 10 * time.Second {
            to *= 2
          }
        }

      }
    }

    kv.logTail = kv.logTail + 1
    kv.px.Done(kv.logTail)
    kv.curConfig = shardmaster.Config{Groups: v.Groups, Shards: v.Shards, Num: v.Num}
    return *preply

  default:
    return
  }

  return
}

func (kv *ShardKV) Reconfig(newConfig shardmaster.Config) bool {

  //use paxos to start newReconfig
  newReconfig := Op{Num: newConfig.Num, Shards: newConfig.Shards, Groups: newConfig.Groups, Type: RECONFIG}

  for {
    to := 10 * time.Millisecond
    tentativeSeq := kv.logTail + 1

    for {
      time.Sleep(to)
      kv.px.Start(tentativeSeq, newReconfig)
      time.Sleep(to)

      if to < 10 * time.Second {
        to *= 2
      }

      decided, op := kv.px.Status(tentativeSeq)
      if decided {
        newOp := op.(Op)
        if newOp.Type == RECONFIG && newOp.Num == newConfig.Num {
          temp := kv.catchUp(newOp)
          if temp.Err == ErrReconfigHolding {
            return false
          }
           return true
        } else {
          kv.catchUp(newOp)
          break
        }
      }
    }
  }
  return false
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  newGet := Op{Type: GET, Key: args.Key, CID: args.CID, Seq: args.Seq}

  // catchup new operations if possible
  var tentativeSeq int
  for {
    tentativeSeq = kv.logTail + 1
    decided, op := kv.px.Status(tentativeSeq)
    if decided {
      op1 := op.(Op)
      temp := kv.catchUp(op1)
      if op1.Type == RECONFIG && temp.Err == ErrReconfigHolding {
        reply.Err = ErrReconfigHolding
        return nil
      }
    } else {
      break
    }
  }

  // check duplicate
  isDup, dupV := kv.isDuplicate(newGet.CID, newGet.Seq)
  if isDup {
    reply.Err = dupV.Err
    reply.Value = dupV.PreviousValue
    return nil
  }

  // use paxos to propose new op
  for {
    to := 10 * time.Millisecond
    tentativeSeq = kv.logTail + 1

    for {
      time.Sleep(to)
      kv.px.Start(tentativeSeq, newGet)
      time.Sleep(to)
      if to < 10 * time.Second {
        to *= 2
      }

      decided, op := kv.px.Status(tentativeSeq)
      if decided {
        newOp := op.(Op)
        if newOp.CID == newGet.CID && newOp.Seq == newGet.Seq {
          temp := kv.catchUp(newOp)
          reply.Err = temp.Err
          reply.Value = temp.PreviousValue
          return nil
        } else {
          temp := kv.catchUp(newOp)
          if newOp.Type == RECONFIG && temp.Err == ErrReconfigHolding {
            reply.Err = ErrReconfigHolding
            return nil
          }
          break
        }
      }
    }
  }
  return nil
}

func (kv *ShardKV) isDuplicate(cid int64, seq int) (bool, PutReply) {
  v, ok := kv.replyDic[cid]
  var reply PutReply
  if ok && v == seq {
    return true, kv.replyCache[cid]
  }
  return false, reply
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  newPut := Op{Key: args.Key, CID: args.CID, Seq: args.Seq, Value: args.Value}
  if args.DoHash {
    newPut.Type = HASH
  } else {
    newPut.Type = PUT
  }

  // catchup new operations if possible
  var tentativeSeq int
  for {
    tentativeSeq = kv.logTail + 1
    decided, op := kv.px.Status(tentativeSeq)
    if decided {
      newOp := op.(Op)
      temp := kv.catchUp(newOp)
      if newOp.Type == RECONFIG && temp.Err == ErrReconfigHolding {
        reply.Err = ErrReconfigHolding
        return nil
      }
    } else {
      break
    }
  }

  // check duplicate
  isDup, dupV := kv.isDuplicate(newPut.CID, newPut.Seq)
  if isDup {
    reply.Err = dupV.Err
    reply.PreviousValue = dupV.PreviousValue
    return nil
  }

  // use paxos to propose new op
  for {
    to := 10 * time.Millisecond
    tentativeSeq = kv.logTail + 1

    for {
      time.Sleep(to)
      kv.px.Start(tentativeSeq, newPut)
      time.Sleep(to)

      if to < 10 * time.Second {
        to *= 2
      }

      decided, op := kv.px.Status(tentativeSeq)

      if decided {
        newOp := op.(Op)
        if newOp.CID == newPut.CID && newOp.Seq == newPut.Seq {
          temp := kv.catchUp(newOp)
          reply.Err = temp.Err
          reply.PreviousValue = temp.PreviousValue
          return nil
        } else {
          temp := kv.catchUp(newOp)
          if newOp.Type == RECONFIG && temp.Err == ErrReconfigHolding {
            reply.Err = ErrReconfigHolding
            return nil
          }
          break
        }
      }
    }
  }
  return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  // kv should catchUp operations,
  // at the same time, it should check if newConfig.Num is not the same as the curConfig.Num
  var tentativeSeq int
  for {
    tentativeSeq = kv.logTail + 1
    decided, op := kv.px.Status(tentativeSeq)
    if decided {
      newOp, ok := op.(Op)
      if ok {
        if newOp.Type == RECONFIG {
          break
        }
        kv.catchUp(newOp)
      }
    } else {
      break
    }
  }

  newConfig := kv.sm.Query(-1)

  diff := newConfig.Num - kv.curConfig.Num
  for diff > 0 {
    config := kv.sm.Query(kv.curConfig.Num + 1)
    ok := kv.Reconfig(config)
    if !ok {
      return
    }
    diff --
  }
  return
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  // Your initialization code here.
  // Don't call Join().
  kv.logTail = -1
  kv.kvDB = make(map[string]string)
  kv.replyCache = make(map[int64]PutReply)
  kv.replyDic = make(map[int64]int)
  kv.curConfig = kv.sm.Query(-1)

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)


  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
