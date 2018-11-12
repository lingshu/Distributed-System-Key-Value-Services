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
  OpName string
  Key    string
  Value  string
  Id     int64
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  content map[string]string
  seq     int // seq for next req
  history map[int64]string
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  op:= Op{OpName : GET, Key: args.Key, Value: "", Id:args.Id}
  reply.Err, reply.Value = kv.Operate(op)
  return nil
}

func (kv *KVPaxos) apply(op *Op) string {
  previousValue := ""
  prev := ""
  switch op.OpName {
  case PUT:
    kv.content[op.Key] = op.Value
  case HASH:
    if oldValue, exist := kv.content[op.Key]; exist {
      previousValue = oldValue
      prev = oldValue
    }
    newValue := strconv.Itoa(int(hash(prev + op.Value)))
    kv.content[op.Key] = newValue
  default:
  }

  kv.history[op.Id] = previousValue
  return previousValue
}

func (kv *KVPaxos) Operate(op Op) (Err, string) {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  if v, ok := kv.history[op.Id]; ok {
    switch op.OpName {
    case PUT:
      return OK, ""
    case HASH:
      return OK, v
    case GET:
      return OK, kv.content[op.Key]
    }
  }

  previousValue := ""
  chosen := false
  for !chosen {
    timeout := 0 * time.Millisecond
    sleepInterval := 10 * time.Millisecond
    kv.px.Start(kv.seq, op)
  INNER:
    for {
      decide, v := kv.px.Status(kv.seq)
      if decide {
        previousValue = ""
        opNew := v.(Op)
        kv.px.Done(kv.seq)
        previousValue = kv.apply(&opNew)
        kv.seq++
        if opNew.Id == op.Id {
          switch opNew.OpName {
          case PUT:
            return OK, ""
          case HASH:
            return OK, previousValue
          case GET:
            if value, exist := kv.content[op.Key]; exist {
              return OK, value
            } else {
              return ErrNoKey, ""
            }
          }
          chosen = true
        }
        break INNER
      } else {
        if timeout > 10 * time.Second {
          return ErrPending, ""
        } else {
          time.Sleep(sleepInterval)
          timeout += sleepInterval
          sleepInterval *= 2
        }
      }
    }

  }
  return OK, ""
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  op := Op{}
  if args.DoHash {
    op = Op{OpName: HASH, Key: args.Key, Value: args.Value, Id: args.Id}
  } else {
    op = Op{OpName: PUT, Key: args.Key, Value: args.Value, Id: args.Id}
  }

  reply.Err, reply.PreviousValue = kv.Operate(op)
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
  kv.content = make(map[string]string)
  kv.history = make(map[int64]string)
  kv.seq = 0

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

