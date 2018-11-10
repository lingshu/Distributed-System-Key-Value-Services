package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
  "math"
  "net"
  "strconv"
  "time"
)
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"


type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]


  // Your data here.
  minSeq int
  majorityNum int
  instances map[int]*State
  doneSeqs []int
  maxSeq int
}

type ProposeArgs struct {
  Seq int
  ProposalNum string
}

type ProposeReply struct {
  ProposeOk bool
  Na string
  Va interface{}
}

type AcceptArgs struct {
  Seq int
  ProposalNum string
  MaxV interface{}
}

type AcceptReply struct {
  AcceptOk bool
}

type State struct {
  np string
  na string
  va interface{}
  status bool
}

type DecideArgs struct {
  Seq int
  ProposalNum string
  MaxV interface{}
}

type DecideReply struct {
  Done int
}

const InitialValue = -1

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()
    
  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

func (px *Paxos) createMajorityServers() map[int]string {
  servers := make(map[int]string)
  count := 0
  for count < px.majorityNum {
    index := rand.Intn(len(px.peers))
    if _, exist := servers[index]; exist {
      continue
    }
    servers[index] = px.peers[index]
    count++
  }
  return servers
}

func (px *Paxos) Propose(args *ProposeArgs, reply *ProposeReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  reply.ProposeOk = false
  if _, exist := px.instances[args.Seq]; !exist {
    px.instances[args.Seq] = &State{"", "", nil, false}
  }

  if args.ProposalNum >= px.instances[args.Seq].np {
    px.instances[args.Seq].np = args.ProposalNum
    reply.ProposeOk = true
    reply.Na = px.instances[args.Seq].na
    reply.Va = px.instances[args.Seq].va
  }
  return nil
}

func (px *Paxos) sendPropose(seq int, proposalNum string, servers map[int]string, v interface{}) (interface{}, bool) {
  args := &ProposeArgs{seq, proposalNum}
  var reply ProposeReply
  maxSeq := ""
  maxV := v
  count := 0
  for index, server := range servers {
    ret := false
    if index == px.me {
      err := px.Propose(args, &reply)
      if err == nil {
        ret = true
      }
    } else {
      ret = call(server, "Paxos.Propose", args, &reply)
    }
    if ret && reply.ProposeOk {
      if reply.Na > maxSeq {
        maxSeq = reply.Na
        maxV = reply.Va
      }
      count++
    }
  }
  return maxV, count == px.majorityNum
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  reply.AcceptOk = false
  if _, exist := px.instances[args.Seq]; !exist {
    px.instances[args.Seq] = &State{"", "", nil, false}
  }

  if args.ProposalNum >= px.instances[args.Seq].np {
    px.instances[args.Seq].np = args.ProposalNum
    px.instances[args.Seq].na = args.ProposalNum
    px.instances[args.Seq].va = args.MaxV
    reply.AcceptOk = true
  }
  return nil
}

func (px *Paxos) sendAccept(seq int, proposalNum string, maxV interface{}, servers map[int]string) bool {
  args := &AcceptArgs{seq, proposalNum, maxV}
  var reply AcceptReply
  count := 0
  for index, server := range servers {
    ret := false
    if index == px.me {
      err := px.Accept(args, &reply)
      if err == nil {
        ret = true
      }
    } else {
      ret = call(server, "Paxos.Accept", args, &reply)
    }

    if ret && reply.AcceptOk {
      count++
    }
  }
  return count == px.majorityNum
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  if _, exist := px.instances[args.Seq]; !exist {
    px.instances[args.Seq] = &State{"", args.ProposalNum, args.MaxV, true}
  } else {
    px.instances[args.Seq].na = args.ProposalNum
    px.instances[args.Seq].va = args.MaxV
    px.instances[args.Seq].status = true
  }

  if args.Seq > px.maxSeq {
    px.maxSeq = args.Seq
  }
  reply.Done = px.doneSeqs[px.me]
  return nil
}

func (px *Paxos) sendDecide(seq int, proposalNum string, maxV interface{}) {
  args := &DecideArgs{seq, proposalNum, maxV}
  var reply DecideReply
  ok := false
  min := math.MaxInt32
  dones := make([]int, len(px.peers))
  for ok == false {
    ok = true
    for index, server := range px.peers {
      ret := false
      if index == px.me {
        err := px.Decide(args, &reply)
        if err == nil {
          ret = true
        }
      } else {
        ret = call(server, "Paxos.Decide", args, &reply)
      }
      if ret == true {
        if reply.Done < min {
          min = reply.Done
        }
        dones[index] = reply.Done
      } else {
        ok = false
      }
    }
    if ok == false {
      time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
    }
  }

  if min != InitialValue {
    px.mu.Lock()
    px.doneSeqs = dones
    for index, _ := range px.instances {
      if index <= min {
        delete(px.instances, index)
      }
    }
    px.minSeq = min + 1
    px.mu.Unlock()
  }
}
//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
  go func() {
    if seq < px.minSeq {
      return
    }

    for {
      proposalNum := strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + strconv.FormatInt(int64(px.me), 10)
      majorityServers := px.createMajorityServers()
      maxV, ok := px.sendPropose(seq, proposalNum, majorityServers, v)
      if ok {
        ok := px.sendAccept(seq, proposalNum, maxV, majorityServers)
        if ok {
          px.sendDecide(seq, proposalNum, maxV)
          break
        } else {
          time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
        }
      } else {
        time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
      }
    }
  }()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  px.doneSeqs[px.me] = seq
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  return px.maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  // You code here.
  px.mu.Lock()
  defer px.mu.Unlock()

  min := math.MaxInt32
  for _, seq := range px.doneSeqs {
    if seq < min {
      min = seq
    }
  }

  if px.minSeq <= min {
    for seq, _ := range px.instances {
      if seq <= min {
        delete(px.instances, seq)
      }
    }
    px.minSeq = min + 1
  }
  return px.minSeq
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  minSeq := px.Min()
  if seq < minSeq {
    return false, nil
  }

  px.mu.Lock()
  defer px.mu.Unlock()
  if state, ok := px.instances[seq]; ok {
    return state.status, state.va
  } else {
    return false, nil
  }
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me


  // Your initialization code here.
  px.majorityNum = len(px.peers) / 2 + 1
  px.minSeq = 0
  px.maxSeq = InitialValue
  px.doneSeqs = make([]int, len(px.peers))
  for i := 0; i < len(px.peers); i++ {
    px.doneSeqs[i] = InitialValue
  }
  px.instances = make(map[int]*State)


  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
