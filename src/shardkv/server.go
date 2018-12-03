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

func (kv *ShardKV) correctGroup(key string) bool {
  if kv.gid == kv.curConfig.Shards[key2shard(key)]{
    return true
  }
  return false
}

func concatHash(prevStr string, newStr string) string {
  concatStr := prevStr + newStr
  res := hash(concatStr)

  return strconv.Itoa(int(res))
}

func (kv *ShardKV) mergeDB(cache map[int64]PutReply, DB map[string]string, reply map[int64]int){
  //updating kvDB
  for k, v := range DB{
    kv.kvDB[k] = v
  }

  //updating reply if needed
  for k, v := range reply{
    if kv.replyDic[k] < v{
      kv.replyDic[k] = v
      kv.replyCache[k] = cache[k]
    }
  }

}

func (kv *ShardKV) isDuplicate(cid int64, seq int) (bool, PutReply) {
  //v,ok := kv.dupDic[uid]
  v, ok := kv.replyDic[cid]
  var pr PutReply
  if ok {
    // if the seq to be checked < seq, we have nothing to return,
    // and since there should be only one outstanding request from the client
    // we expect this never happen
    if seq < v{
      DPrintf("The seq to be checked is smaller than the last seq of that client, which should not happen\n")
      return true, pr
    }else if v == seq{
      return true, kv.replyCache[cid]
    }


  }
  return false, pr

}

func (kv *ShardKV) SendShardInfo(args *FetchArgs, reply *FetchReply) error{



	DPrintf("Group %d: me %d locked\n", kv.gid, kv.me)
	if args.ConfigNum > kv.curConfig.Num{
		DPrintf("Not ready yet, my current num is: %d, wanted: %d\n", kv.curConfig.Num, args.ConfigNum)
		reply.Err = ErrReconfigHolding
		return nil
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()


	var cache = make(map[int64]PutReply)
	var kvDB = make(map[string]string)
	var replyDic = make(map[int64]int)


	for k,v := range kv.replyCache{
		cache[k] = v
	}

	for k, v := range kv.kvDB{
		if key2shard(k) == args.ShardNum{
			kvDB[k] = v
		}
	}

	for k, v := range kv.replyDic{
		replyDic[k] = v
	}
	reply.ReplyDic = replyDic
	reply.KVDB = kvDB
	reply.ReplyCache = cache
	reply.Err = OK

	DPrintf("Group %d: me %d Unlocked\n", kv.gid, kv.me)

	return nil

}

func (kv *ShardKV) catchUp(v Op) (reply PutReply) {

  DPrintf("Server: %d : %d | Doing catchup for op: %s, seq num: %d",kv.gid, kv.me, v.Type, kv.logTail +1)

  switch optype := v.Type; optype {
  case PUT:

    kv.logTail = kv.logTail+1
    kv.px.Done(kv.logTail)


    reply := &PutReply{}

    if kv.correctGroup(v.Key){
      //reply.PreviousValue = v.Value
      kv.kvDB[v.Key] = v.Value
      reply.Err = OK
      kv.replyCache[v.CID] = *reply
      kv.replyDic[v.CID] = v.Seq
    }else{
      reply.Err = ErrWrongGroup
    }


    return *reply
  case GET:
    //do nothing
    kv.logTail = kv.logTail+1
    kv.px.Done(kv.logTail)

    reply := &PutReply{}

    if kv.correctGroup(v.Key){
      //reply.PreviousValue = v.Value
      val, ok := kv.kvDB[v.Key]
      if ok{
        reply.Err = OK
        reply.PreviousValue = val
      }else{
        reply.Err = ErrNoKey
        reply.PreviousValue = ""
      }
      kv.replyDic[v.CID] = v.Seq
      kv.replyCache[v.CID] = *reply
    }else{
      reply.Err = ErrWrongGroup
    }


    return *reply
  case HASH:

    kv.logTail = kv.logTail+1
    kv.px.Done(kv.logTail)

    reply := &PutReply{}

    if kv.correctGroup(v.Key){
      prevStr, ok := kv.kvDB[v.Key]
      if ok {
        kv.kvDB[v.Key] = concatHash(prevStr, v.Value)
        reply.PreviousValue = prevStr
      }else{
        kv.kvDB[v.Key] = concatHash("", v.Value)
        reply.PreviousValue = ""
      }
      kv.replyDic[v.CID] = v.Seq
      reply.Err = OK
      kv.replyCache[v.CID] = *reply
    }else{
      reply.Err = ErrWrongGroup
    }

    return *reply
  case RECONFIG:

    preply := &PutReply{}

    for i:=0; i < shardmaster.NShards; i++{
      //only fetch shard that does not belong to itself before
      if kv.curConfig.Shards[i] != kv.gid && v.Shards[i] == kv.gid{
        if kv.curConfig.Shards[i] == 0{
          continue
        }
        args := &FetchArgs{}
        args.ShardNum = i
        args.ConfigNum = v.Num
        var reply FetchReply
        count := 0
        to := 10 * time.Millisecond
        for true {

          ok := call(kv.curConfig.Groups[kv.curConfig.Shards[i]][count % len(kv.curConfig.Groups)], "ShardKV.SendShardInfo", args, &reply)
          count ++
          if ok && reply.Err == OK{
            //need to update own db
            kv.mergeDB(reply.ReplyCache, reply.KVDB, reply.ReplyDic)
            break
          }else if reply.Err == ErrReconfigHolding{
            DPrintf("target not ready! Abort!\n")
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
    var c shardmaster.Config
    kv.logTail = kv.logTail+1
    kv.px.Done(kv.logTail)
    c.Groups = v.Groups
    c.Shards = v.Shards
    c.Num = v.Num
    kv.curConfig = c

    //DPrintf("Server; %d %d, config successfully update to %s\n", kv.gid, kv.me, v.Num)

    return *preply
  default:
    return
    DPrintf("SHOULD NOT PRINT DEFAULT\n")
  }
  DPrintf("SHOULD NOT PRINT DEFAULT\n")
  return
}

func (kv *ShardKV) Reconfig(newConfig shardmaster.Config) bool{

  newReconfig := &Op{}
  DPrintf("Server : %d : %d doing reconfig, New Config Num is: %d\n", kv.gid, kv.me, newConfig.Num)
  newReconfig.Num = newConfig.Num
  newReconfig.Shards = newConfig.Shards
  newReconfig.Groups = newConfig.Groups

  newReconfig.Type = RECONFIG


  for true {
    to := 10 * time.Millisecond
    //kv.mu.Lock()
    tentativeSeq := kv.logTail + 1
    //kv.mu.Unlock()
    //DPrintf("Server : %s Reconfig \n", kv.me)
    //tentativeSeq = kv.logTail + 1
    //time.Sleep(50*time.Millisecond)

    //check periodically

    for {
      time.Sleep(to)
      kv.px.Start(tentativeSeq, newReconfig)
      time.Sleep(to)

      if to < 10 * time.Second {
        to *= 2
      }

      decided, op := kv.px.Status(tentativeSeq)

      if decided {

        op2, ok := op.(*Op)
        //DPrintf("is ok to convert: %t", ok)
        //DPrintf("Server.Put invoke status | return uid: %d , request uid: %d \n", op1.Uid, newPut.Uid)

        DPrintf("Decided? %t", decided)
        //if ok2 {DPrintf("Server.Put invoke status | return uid: %d , request uid: %d \n", newOp.Uid, newPut.Uid)}
        if ok{
          if op2.Type == RECONFIG{

            if op2.Num == newConfig.Num{

              temp := kv.catchUp(*op2)
              if temp.Err == ErrReconfigHolding{
                return false
              }

              return true
            }
            DPrintf("Weird thing happens 2... agreed RECONFIG Op with config num: %d\n", op2.Num)
          }else{
            //case has already been decided, therefore the kvpaxos replica need to catch up
            kv.catchUp(*op2)
            DPrintf("Test1\n")
            //need to start next instance immediately, thus no sleep
            //tentativeSeq += 1
            //time.Sleep(to)
            break
          }
        }else{
          op3, ok := op.(Op)
          if ok{
            if op3.Type == RECONFIG{
              if op3.Num == newConfig.Num {

                temp := kv.catchUp(op3)
                if temp.Err == ErrReconfigHolding{
                  return false
                }
                return true
              }
              DPrintf("Weird thing happens 1...agreed RECONFIG Op with config num: %d\n", op3.Num)
            }else{
              //case has already been decided, therefore the kvpaxos replica need to catch up
              kv.catchUp(op3)
              //need to start next instance immediately, thus no sleep
              //tentativeSeq += 1
              DPrintf("Test2\n")
              break
            }
          }else{
            DPrintf("Fatal Error: decided = true, but value = nil")
          }

        }

      }

      DPrintf("Do Reconfig again... \n")
    }

  }


  return false

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  // Your code here.
  newGet := &Op{}
  newGet.Type = GET
  newGet.Key = args.Key
  newGet.CID = args.CID
  newGet.Seq = args.Seq
  //seq := kv.newSeq()

  var tentativeSeq int
  for true{

    tentativeSeq = kv.logTail + 1


    decided, op := kv.px.Status(tentativeSeq)
    if decided {
      op1, ok := op.(Op)
      if ok{
        if op1.CID == 0{DPrintf("1fatal error, uid == 0")}

        temp := kv.catchUp(op1)
        if op1.Type == RECONFIG && temp.Err == ErrReconfigHolding{
          reply.Err = ErrReconfigHolding
          return nil
        }
      }else{
        op2, ok2 := op.(*Op)
        //DPrintf("uid: %d\n", op2.Uid)
        if !ok2 {DPrintf("Fatal Error: not match to *op\n")}
        if op2.CID == 0{DPrintf("fatal error, uid == 0")}
        temp := kv.catchUp(*op2)
        if op2.Type == RECONFIG && temp.Err == ErrReconfigHolding{
          reply.Err = ErrReconfigHolding
          return nil
        }
      }

    }else{break}

  }

  isDup, dupV := kv.isDuplicate(newGet.CID,newGet.Seq)
  if isDup{
    reply.Err = dupV.Err
    reply.Value = dupV.PreviousValue
    return nil
  }
  //asking for the status until

  for true {
    to := 10 * time.Millisecond
    //kv.mu.Lock()
    tentativeSeq = kv.logTail + 1
    //kv.mu.Unlock()

    //time.Sleep(20*time.Millisecond)
    for {
      time.Sleep(to)
      kv.px.Start(tentativeSeq, newGet)
      time.Sleep(to)
      if to < 10 * time.Second {
        to *= 2
      }

      decided, op := kv.px.Status(tentativeSeq)

      if decided {
        op2, ok := op.(*Op)
        if ok{
          //if op2.CID == 0{DPrintf("fatal error, uid == 0")}
          if op2.CID == newGet.CID && op2.Seq == newGet.Seq{
            //no need to catch up, simply return value
            temp := kv.catchUp(*op2)
            reply.Err = temp.Err
            reply.Value = temp.PreviousValue
            return nil
          }else {
            //case has already been decided, therefore the kvpaxos replica need to catch up
            temp := kv.catchUp(*op2)
            if op2.Type == RECONFIG && temp.Err == ErrReconfigHolding{
              reply.Err = ErrReconfigHolding
              return nil
            }

            break
          }
        }else{
          op3, ok2 := op.(Op)
          if !ok2 {DPrintf("Fatal Error: not match to *op\n")}
          //if op3.Uid == 0{DPrintf("fatal error, uid == 0")}
          if op3.CID == newGet.CID && op3.Seq == newGet.Seq{
            //no need to catch up, simply return value
            temp := kv.catchUp(op3)
            reply.Err = temp.Err
            reply.Value = temp.PreviousValue
            return nil
          }else {
            //case has already been decided, therefore the kvpaxos replica need to catch up
            temp := kv.catchUp(op3)
            if op3.Type == RECONFIG && temp.Err == ErrReconfigHolding{
              reply.Err = ErrReconfigHolding
              return nil
            }

            break
          }
        }
      }

    }

  }

  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  DPrintf("Received new put command, puthash: %t, key : %s, value: %s\n, cid: %d, seq: %d", args.DoHash,args.Key, args.Value, args.CID, args.Seq )

  newPut := &Op{}
  if args.DoHash{
    newPut.Type = HASH
  }else{newPut.Type = PUT}


  newPut.Key = args.Key
  newPut.CID = args.CID
  newPut.Seq = args.Seq
  newPut.Value = args.Value
  //newPut.DoHash = args.DoHash


  var tentativeSeq int
  //tentativeSeq = kv.logTail + 1
  for true{
    //kv.mu.Lock()
    tentativeSeq = kv.logTail + 1

    //kv.mu.Unlock()
    decided, op := kv.px.Status(tentativeSeq)

    if decided {
      DPrintf("apply log into db...\n")
      op1, ok := op.(Op)
      if ok{
        if op1.CID == 0{DPrintf("2fatal error, uid == 0")}
        temp := kv.catchUp(op1)
        if op1.Type == RECONFIG && temp.Err == ErrReconfigHolding{
          reply.Err = ErrReconfigHolding
          return nil
        }
      }else{
        op2, ok2 := op.(*Op)
        DPrintf("uid: %d\n", op2.CID)
        if !ok2 {DPrintf("Fatal Error: not match to *op\n")}
        if op2.CID == 0{DPrintf("3fatal error, uid == 0")}
        temp:=kv.catchUp(*op2)
        if op2.Type == RECONFIG && temp.Err == ErrReconfigHolding{
          reply.Err = ErrReconfigHolding
          return nil
        }
      }

    }else{break}

  }
  DPrintf("Put | done catchup\n")

  isDup, dupV := kv.isDuplicate(newPut.CID, newPut.Seq)
  if isDup{
    reply.Err = dupV.Err
    reply.PreviousValue = dupV.PreviousValue
    DPrintf("Put | is duplicate\n")
    return nil
  }

  DPrintf("Put | done check duplicate\n")


  for true {
    to := 10 * time.Millisecond
    //kv.mu.Lock()
    tentativeSeq = kv.logTail + 1
    //kv.mu.Unlock()
    DPrintf("Server.Put | seq: %d, Key: %s, value: %s, dohash: %s\n", tentativeSeq, newPut.Key, newPut.Value, newPut.Type)
    //tentativeSeq = kv.logTail + 1
    //time.Sleep(50*time.Millisecond)

    //check periodically

    for {
      time.Sleep(to)
      kv.px.Start(tentativeSeq, newPut)
      time.Sleep(to)

      if to < 10 * time.Second {
        to *= 2
      }

      decided, op := kv.px.Status(tentativeSeq)

      if decided {

        op2, ok := op.(*Op)
        //DPrintf("is ok to convert: %t", ok)
        //DPrintf("Server.Put invoke status | return uid: %d , request uid: %d \n", op1.Uid, newPut.Uid)

        DPrintf("Decided? %t", decided)
        //if ok2 {DPrintf("Server.Put invoke status | return uid: %d , request uid: %d \n", newOp.Uid, newPut.Uid)}
        if ok{
          if op2.CID == newPut.CID && op2.Seq == newPut.Seq{

            temp := kv.catchUp(*op2)
            reply.Err = temp.Err
            reply.PreviousValue  = temp.PreviousValue

            return nil
          }else{
            //case has already been decided, therefore the kvpaxos replica need to catch up
            temp := kv.catchUp(*op2)
            if op2.Type == RECONFIG && temp.Err == ErrReconfigHolding{
              reply.Err = ErrReconfigHolding
              return nil
            }
            //need to start next instance immediately, thus no sleep
            //tentativeSeq += 1
            //time.Sleep(to)
            break
          }
        }else{
          op3, ok := op.(Op)
          if ok{
            if op3.CID == newPut.CID && op3.Seq == newPut.Seq{


              temp := kv.catchUp(op3)
              reply.Err = temp.Err
              reply.PreviousValue = temp.PreviousValue

              return nil
            }else{
              //case has already been decided, therefore the kvpaxos replica need to catch up
              temp := kv.catchUp(op3)
              if op3.Type == RECONFIG && temp.Err == ErrReconfigHolding{
                reply.Err = ErrReconfigHolding
                return nil
              }
              //need to start next instance immediately, thus no sleep
              //tentativeSeq += 1
              break
            }
          }else{
            DPrintf("Fatal Error: decided = true, but value = nil")
          }

        }

      }

      DPrintf("Do put again... \n")
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

  var tentativeSeq int
  //DPrintf("Group %d: me %d tick locked\n", kv.gid, kv.me)
  for {
    tentativeSeq = kv.logTail + 1
    decided, op := kv.px.Status(tentativeSeq)
    if decided {
      op1, ok := op.(Op)
      if ok {
        //if op1.CID == 0 {DPrintf("4fatal error, uid == 0")}
        if op1.Type == RECONFIG {
          break
        }

        kv.catchUp(op1)
      }else{
        op2 := op.(*Op)
        //DPrintf("uid: %d\n", op2.Uid)
        //if !ok2 {DPrintf("Fatal Error: not match to *op\n")}
        //if op2.CID == 0 {DPrintf("fatal error, uid == 0")}
        if op2.Type == RECONFIG {
          break
        }
        kv.catchUp(*op2)
      }

    } else {
      break
    }
  }

  //DPrintf("Server: %d : %d, Tick\n", kv.gid, kv.me)
  newConfig := kv.sm.Query(-1)

  diff := newConfig.Num - kv.curConfig.Num
  for diff > 0 {

    config := kv.sm.Query(kv.curConfig.Num + 1)
    //DPrintf("Server: %d : %d, new Config Num is : %s, and curConfig Num is: %s", kv.gid, kv.me, config.Num, kv.curConfig.Num)

    temp := kv.Reconfig(config)
    if !temp{
      DPrintf("Tick() return first, bkz not ready\n")
      return
    }
    diff --
  }
  DPrintf("Group %d: me %d tick() Unlocked\n", kv.gid, kv.me)
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
