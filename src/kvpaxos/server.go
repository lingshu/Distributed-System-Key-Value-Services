package kvpaxos

import (
  "net"
  "strconv"
  "time"
)
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

const Debug=0

const (
  PUT = "Put"
  GET = "Get"
  HASH = "Hash"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}


type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Type string
  Key    string
  Value  string
  Uid     int64
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  kvDB map[string]string//key value db
  logTail int //the tail of the log, before which all have been performed in order onto state machine kvDB, initially -1
  replyDic map[int64]string
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  // Your code here.
  newGet := &Op{}
  newGet.Type = GET
  newGet.Key = args.Key
  newGet.Uid = args.Uid
  //seq := kv.newSeq()

  var tentativeSeq int
  for true{

    tentativeSeq = kv.logTail + 1


    decided, op := kv.px.Status(tentativeSeq)
    if decided {
      op1, ok := op.(Op)
      if ok{
        if op1.Uid == 0{DPrintf("fatal error, uid == 0")}

        kv.catchUp(op1)
      }else{
        op2, ok2 := op.(*Op)
        //DPrintf("uid: %d\n", op2.Uid)
        if !ok2 {DPrintf("Fatal Error: not match to *op\n")}
        if op2.Uid == 0{DPrintf("fatal error, uid == 0")}
        kv.catchUp(*op2)
      }

    }else{break}

  }

  isDup, dupV := kv.isDuplicate(newGet.Uid)
  if isDup{
    reply.Err = DuplicateOp
    reply.Value = dupV
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
          if op2.Uid == 0{DPrintf("fatal error, uid == 0")}
          if op2.Uid == newGet.Uid{
            //no need to catch up, simply return value
            //since the op is a get, no reason to log it
            //kv.mu.Lock()
            v, ok := kv.kvDB[args.Key]
            //kv.mu.Unlock()

            if ok{
              reply.Err = OK
              reply.Value = v
            }else{
              reply.Err = ErrNoKey
              reply.Value = ""
            }
            kv.catchUp(*op2)
            return nil
          }else if op2.Uid != newGet.Uid{
            //case has already been decided, therefore the kvpaxos replica need to catch up
            kv.catchUp(*op2)
            //tentativeSeq += 1
            //need to start next instance immediately, thus no sleep
            //time.Sleep(to)
            break
          }
        }else{
          op3, ok2 := op.(Op)
          if !ok2 {DPrintf("Fatal Error: not match to *op\n")}
          if op3.Uid == 0{DPrintf("fatal error, uid == 0")}
          if op3.Uid == newGet.Uid{
            //no need to catch up, simply return value
            //since the op is a get, no reason to log it
            //kv.mu.Lock()
            v, ok := kv.kvDB[args.Key]
            //kv.mu.Unlock()

            if ok{
              reply.Err = OK
              reply.Value = v
            }else{
              reply.Err = ErrNoKey
              reply.Value = ""
            }
            kv.catchUp(op3)
            return nil
          }else if op3.Uid != newGet.Uid{
            //case has already been decided, therefore the kvpaxos replica need to catch up
            kv.catchUp(op3)
            //tentativeSeq += 1
            //need to start next instance immediately, thus no sleep
            break
          }
        }
      }


    }

  }
  return nil
}

func concatHash(prevStr string, newStr string) string {
  concatStr := prevStr + newStr
  res := hash(concatStr)

  return strconv.Itoa(int(res))
}

func (kv *KVPaxos) catchUp(v Op) bool {

  //DPrintf("ME: %d | Doing catchup for op: %s, seq num: %d", kv.me, v, kv.logTail +1)

  switch optype := v.Type; optype {
  case PUT:
    kv.kvDB[v.Key] = v.Value
    kv.logTail = kv.logTail+1
    kv.px.Done(kv.logTail)
    //mem shrink
    //kv.dupDic[v.Uid] = true

    kv.replyDic[v.Uid] = "1"
    return true
  case GET:
    //do nothing
    kv.logTail = kv.logTail+1
    kv.px.Done(kv.logTail)
    //kv.dupDic[v.Uid] = true
    kv.replyDic[v.Uid] = kv.kvDB[v.Key]
    return true
  case HASH:
    prevStr, ok := kv.kvDB[v.Key]
    if ok {
      kv.kvDB[v.Key] = concatHash(prevStr, v.Value)
    }else{
      kv.kvDB[v.Key] = concatHash("", v.Value)
    }
    kv.logTail = kv.logTail+1
    kv.px.Done(kv.logTail)
    //kv.dupDic[v.Uid] = true
    kv.replyDic[v.Uid] = prevStr
    return true
  default:
    return false
    DPrintf("SHOULD NOT PRINT DEFAULT\n")
  }
  DPrintf("SHOULD NOT PRINT DEFAULT\n")
  return true
}

func (kv *KVPaxos) isDuplicate(uid int64) (bool, string) {
  //v,ok := kv.dupDic[uid]
  v, ok := kv.replyDic[uid]
  if ok {

    return true, v

  }
  return false, ""

}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  newPut := &Op{}
  if args.DoHash{
    newPut.Type = HASH
  }else{newPut.Type = PUT}


  newPut.Key = args.Key
  newPut.Uid = args.Uid
  newPut.Value = args.Value
  //newPut.DoHash = args.DoHash


  //check duplicate
  //TO DO
  //seq := kv.newSeq()

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
        if op1.Uid == 0{DPrintf("fatal error, uid == 0")}
        kv.catchUp(op1)
      }else{
        op2, ok2 := op.(*Op)
        DPrintf("uid: %d\n", op2.Uid)
        if !ok2 {DPrintf("Fatal Error: not match to *op\n")}
        if op2.Uid == 0{DPrintf("fatal error, uid == 0")}
        kv.catchUp(*op2)
      }

    }else{break}

  }

  isDup, dupV := kv.isDuplicate(newPut.Uid)
  if isDup{
    reply.Err = DuplicateOp
    reply.PreviousValue = dupV
    return nil
  }


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
          if op2.Uid == newPut.Uid{

            if args.DoHash {
              //kv.mu.Lock()
              v, ok := kv.kvDB[args.Key]
              //kv.mu.Unlock()
              if ok{
                reply.PreviousValue = v
              }else{
                reply.PreviousValue = ""
              }
            }
            kv.catchUp(*op2)
            reply.Err = OK

            return nil
          }else if op2.Uid != newPut.Uid && op2.Uid != 0{
            //case has already been decided, therefore the kvpaxos replica need to catch up
            kv.catchUp(*op2)
            //need to start next instance immediately, thus no sleep
            //tentativeSeq += 1
            //time.Sleep(to)
            break
          }
        }else{
          op3, ok := op.(Op)
          if ok{
            if op3.Uid == newPut.Uid{

              if args.DoHash {
                //kv.mu.Lock()
                v, ok := kv.kvDB[args.Key]
                //kv.mu.Unlock()
                if ok{
                  reply.PreviousValue = v
                }else{
                  reply.PreviousValue = ""
                }
              }
              kv.catchUp(op3)
              reply.Err = OK

              return nil
            }else if op3.Uid != newPut.Uid && op3.Uid != 0{
              //case has already been decided, therefore the kvpaxos replica need to catch up
              kv.catchUp(op3)
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

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.
  kv.kvDB = make(map[string]string)
  kv.replyDic = make(map[int64]string)
  //kv.dupDic = make(map[int64]bool)
  kv.logTail = -1

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
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

