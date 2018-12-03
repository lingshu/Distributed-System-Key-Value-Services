package shardmaster

import (
  "net"
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

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  configNum int
  logTail int
}


type Op struct {
  // Your data here.
  Type string
  GID int64
  Shard int
  Num int
  Servers []string
}

const (
  JOIN = "Join"
  LEAVE = "Leave"
  MOVE = "Move"
  QUERY = "Query"
)

func contains(s [NShards]int64, e int64) bool {
  for _, a := range s {
    if a == e {
      return true
    }
  }
  return false
}

func (sm *ShardMaster) Update(newShards [NShards]int64, newGroups map[int64][]string) bool {
  newConfig := make([]Config, 1)
  newConfig[0].Num = sm.configNum + 1
  newConfig[0].Groups = newGroups
  newConfig[0].Shards = newShards
  sm.configs = append(sm.configs, newConfig[0])
  sm.configNum = sm.configNum + 1
  sm.logTail = sm.logTail + 1
  sm.px.Done(sm.logTail)
  return true
}

func (sm *ShardMaster) CatchUp(v Op) bool {
  switch optype := v.Type; optype {

  case JOIN:

    // the old config's shards already own the GID
    if contains(sm.configs[sm.configNum].Shards, v.GID){
      sm.logTail = sm.logTail + 1
      sm.px.Done(sm.logTail)
      return true
    }

    oldConfig := sm.configs[sm.configNum]
    NumGroup := len(oldConfig.Groups) + 1
    shardsPerGroup := NShards / NumGroup
    newShards := [NShards]int64{}
    newGroups := make(map[int64][]string)

    // extra group exists, keep the config as it is
    if NumGroup > NShards {
     for k, v := range oldConfig.Groups {
       newGroups[k] = v
     }
     newGroups[v.GID] = v.Servers

     for i, e := range oldConfig.Shards {
       newShards[i] = e
     }

     return sm.Update(newShards, newGroups)
    }

    count := 0
    mapTemp := make(map[int64]int)
    for i, e := range oldConfig.Shards {
      if e != 0 && mapTemp[e] < shardsPerGroup {
        mapTemp[e] += 1
        newShards[i] = e
      } else if count < shardsPerGroup {
        mapTemp[v.GID] += 1
        newShards[i] = v.GID
        count++
      }
    }

    // put groups into new group
    for k, v := range oldConfig.Groups {
      newGroups[k] = v
    }
    newGroups[v.GID] = v.Servers

    return sm.Update(newShards, newGroups)

  case LEAVE:

    // the GID to leave does not exist
    _, ok := sm.configs[sm.configNum].Groups[v.GID]
    if !ok {
      sm.logTail = sm.logTail+1
      sm.px.Done(sm.logTail)
      return true
    }

    newGroups := make(map[int64][]string)
    newShards := [NShards]int64{}
    outstanding := make(map[int]bool)
    mapTemp := make(map[int64]int)

    oldConfig := sm.configs[sm.configNum]
    NumGroup := len(oldConfig.Groups) - 1
    var shardsPerGroup int
    if NumGroup <= 0 {
      shardsPerGroup = 0
    } else if NumGroup > NShards {
      shardsPerGroup = 1
    } else {
      shardsPerGroup = NShards / NumGroup
    }

    // the GID to leave is not in use
    if !contains(sm.configs[sm.configNum].Shards, v.GID) {
      for i, e := range oldConfig.Shards {
        newShards[i] = e
      }

      for k, val := range oldConfig.Groups {
        if k != v.GID {
          newGroups[k] = val
        }
      }

      return sm.Update(newShards, newGroups)
    }

    // corner case: null newShards and newGroups
    if NumGroup == 0 {
      return sm.Update([NShards]int64{}, make(map[int64][]string))
    }

    // get mapTemp
    for i, e := range oldConfig.Shards {
      if e != v.GID {
        newShards[i] = e
        mapTemp[e] += 1
      }
    }
    if NumGroup <= NShards {
      for k, _ := range oldConfig.Groups {
        _, ok := mapTemp[k]
        if !ok && k != v.GID {
          mapTemp[k] = 0
        }
      }
    }

    for i, e := range oldConfig.Shards {
      found := false
      if e == v.GID {
        for k, v := range mapTemp {
          if v < shardsPerGroup {
            newShards[i] = k
            mapTemp[k] = v + 1
            found = true
            break
          }
        }

        if !found {
          outstanding[i] = true
        }
      }
    }

    for i, e := range outstanding {
      if e {
        if len(oldConfig.Groups) < NShards {
          for k, v := range mapTemp {
            if v <= shardsPerGroup {
              newShards[i] = k
              mapTemp[k] += 1
              break
            }
          }
        } else {
          for k, _ := range oldConfig.Groups {
            freq, ok := mapTemp[k]
            if k != v.GID && (!ok || freq == 0) {
              newShards[i] = k
              mapTemp[k] = 1
              break
            }
          }
        }
      }
    }

    for k, value := range oldConfig.Groups {
      if k != v.GID{
        newGroups[k] = value
      }
    }

    return sm.Update(newShards, newGroups)

  case MOVE:

    oldConfig := sm.configs[sm.configNum]
    newGroups := make(map[int64][]string)
    newShards := [NShards]int64{}

    for i, e := range oldConfig.Shards {
      if i != v.Shard {
        newShards[i] = e
      } else {
        newShards[i] = v.GID
      }
    }

    for k, value := range oldConfig.Groups {
      newGroups[k] = value

    }
    return sm.Update(newShards, newGroups)

  case QUERY:

    sm.logTail = sm.logTail + 1
    sm.px.Done(sm.logTail)
    return true

  default:

    return false
  }

  return true
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // Your code here.
  sm.mu.Lock()

  newJoin := Op{Type: JOIN, GID: args.GID, Servers: args.Servers}

  var tentativeSeq int
  for true {
    tentativeSeq = sm.logTail + 1
    decided, op := sm.px.Status(tentativeSeq)
    if decided {
      newOp := op.(Op)
      sm.CatchUp(newOp)

    } else {
      break
    }
  }
  for true {
    to := 10 * time.Millisecond
    tentativeSeq = sm.logTail + 1

    for {
      time.Sleep(to)
      sm.px.Start(tentativeSeq, newJoin)
      time.Sleep(to)
      if to < 10 * time.Second {
        to *= 2
      }

      decided, op := sm.px.Status(tentativeSeq)
      if decided {
         newOp, _ := op.(Op)
         if newOp.GID == newJoin.GID && newOp.Type == JOIN {
           sm.CatchUp(newOp)
           sm.mu.Unlock()
           return nil
         }else {
           sm.CatchUp(newOp)
           break
         }
      }
    }
  }
  sm.mu.Unlock()
  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // Your code here.
  sm.mu.Lock()

  newLeave := Op{Type: LEAVE, GID: args.GID}
  var tentativeSeq int

  for {
    tentativeSeq = sm.logTail + 1
    decided, op := sm.px.Status(tentativeSeq)
    if decided {
      newOp := op.(Op)
      sm.CatchUp(newOp)
    } else {
      break
    }
  }

  for {
    to := 10 * time.Millisecond
    tentativeSeq = sm.logTail + 1

    for {
      time.Sleep(to)
      sm.px.Start(tentativeSeq, newLeave)
      time.Sleep(to)
      if to < 10 * time.Second {
        to *= 2
      }

      decided, op := sm.px.Status(tentativeSeq)
      if decided {
        newOp := op.(Op)
        if newOp.GID == newLeave.GID && newOp.Type == LEAVE {
          sm.CatchUp(newOp)
          sm.mu.Unlock()
          return nil
        } else {
          sm.CatchUp(newOp)
          break
       }
      }
    }
  }
  sm.mu.Unlock()
  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  // Your code here.
  sm.mu.Lock()

  newMove := Op{Type: MOVE, GID: args.GID, Shard: args.Shard}

  var tentativeSeq int
  for true{
    tentativeSeq = sm.logTail + 1

    decided, op := sm.px.Status(tentativeSeq)
    if decided {
      newOp := op.(Op)
      sm.CatchUp(newOp)
      } else {
        break
    }
  }
  for {
    to := 10 * time.Millisecond
    tentativeSeq = sm.logTail + 1

    for {
      time.Sleep(to)
      sm.px.Start(tentativeSeq, newMove)
      time.Sleep(to)
      if to < 10 * time.Second {
        to *= 2
      }

      decided, op := sm.px.Status(tentativeSeq)
      if decided {
        if newOp, ok := op.(Op); ok {
          if newOp.GID == newMove.GID && newOp.Type == MOVE && newOp.Shard == newMove.Shard {
            sm.CatchUp(newOp)
            sm.mu.Unlock()
            return nil
          }
        } else {
          sm.CatchUp(newOp)
          break
        }
      }
    }
  }
  sm.mu.Unlock()
  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.
  sm.mu.Lock()

  newQuery := Op{Type: QUERY, Num: args.Num}
  var tentativeSeq int
  for {
    tentativeSeq = sm.logTail + 1
    decided, op := sm.px.Status(tentativeSeq)
    if decided {
      newOp := op.(Op)
      sm.CatchUp(newOp)
    } else {
      break
    }
  }

  for {
    to := 10 * time.Millisecond
    tentativeSeq = sm.logTail + 1

    for {
      time.Sleep(to)
      sm.px.Start(tentativeSeq, newQuery)
      time.Sleep(to)
      if to < 10 * time.Second {
        to *= 2
      }

      decided, op := sm.px.Status(tentativeSeq)
      if decided {
       newOp := op.(Op)
       if newOp.Type == QUERY && newOp.Num == newQuery.Num {
         sm.CatchUp(newOp)
         if newOp.Num == -1 || newOp.Num > sm.configNum {
           reply.Config = sm.configs[sm.configNum]
         } else {
           reply.Config = sm.configs[newOp.Num]
         }
         sm.mu.Unlock()
         return nil
       } else {
         sm.CatchUp(newOp)
         break
       }
      }
    }
  }
  sm.mu.Unlock()
  return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}
  sm.logTail = -1
  sm.configNum = 0

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
