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
  "net"
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
  proposerMgr *ProposerManager
  acceptorMgr *AcceptorManager

  instanceState map[int]interface{}

  doneSeqs     []int // non-local, except doneSeqs[me]
  minDoneSeq   int   // the minimal in doneSeqs
  minDoneIndex int   // the index of the minimal in doneSeqs
}

type ProposerManager struct {
  px             *Paxos
  mu             sync.Mutex
  peers          []string
  me             int // index of the proposers
  proposers      map[int]*Proposer
  seqMax        int
  seq_chosen_max int
}

type AcceptorManager struct {
  mu        sync.Mutex
  acceptors map[int]*Acceptor
  seqMax   int
}

type Proposer struct {
  mgr           *ProposerManager
  seq           int
  propose_value interface{}
  isDead        bool
}

type Acceptor struct {
  mu sync.Mutex
  // init: -1, -1, ""
  n_p int
  n_a int
  v_a interface{}
}

type PrepareReply struct {
  N    int // for choosing next proposing number
  N_a  int
  V_a  interface{}
  Succ bool
}

type PrepareArgs struct {
  Seq int
  N   int
}

type AcceptReply struct {
  N    int // for choosing next proposing number
  Succ bool
}

type AcceptArgs struct {
  Seq int
  N   int
  V   interface{}
}

type DecideArgs struct {
  Seq int
  V   interface{}
}

type DecideReply bool

type SeqArgs struct {
  Sender int
  Seq    int
}

type SeqReply bool

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

func (acceptorMgr *AcceptorManager) GetInstance(seq int) *Acceptor {
  acceptorMgr.mu.Lock()
  defer acceptorMgr.mu.Unlock()

  // only the seq that has not been accepted can be considered to be accepted
  acceptor, ok := acceptorMgr.acceptors[seq]
  if !ok {
    if seq > acceptorMgr.seqMax {
      acceptorMgr.seqMax = seq
    }
    acceptor = &Acceptor{n_p: -1, n_a: -1, v_a: nil}
    acceptorMgr.acceptors[seq] = acceptor
  }
  return acceptor
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  acceptor := px.acceptorMgr.GetInstance(args.Seq)
  acceptor.mu.Lock()
  defer acceptor.mu.Unlock()

  if args.N > acceptor.n_p {
    reply.Succ = true
    acceptor.n_p = args.N
    reply.N = args.N
    reply.N_a = acceptor.n_a
    reply.V_a = acceptor.v_a
  } else {
    reply.Succ = false
    reply.N = acceptor.n_p
  }
  return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  acceptor := px.acceptorMgr.GetInstance(args.Seq)
  acceptor.mu.Lock()
  defer acceptor.mu.Unlock()
  if args.N >= acceptor.n_p {
    reply.Succ = true
    acceptor.n_p = args.N
    acceptor.n_a = args.N
    acceptor.v_a = args.V
    reply.N = args.N
  } else {
    reply.Succ = false
    reply.N = acceptor.n_p
  }
  return nil
}

func (proposerMgr *ProposerManager) RunProposer(seq int, v interface{}) {
  proposerMgr.mu.Lock()
  defer proposerMgr.mu.Unlock()

  // only propose the seq that has not been proposed
  if _, ok := proposerMgr.proposers[seq]; !ok {
  	// change seqMax
    if seq > proposerMgr.seqMax {
      proposerMgr.seqMax = seq
    }
    prop := &Proposer{mgr: proposerMgr, seq: seq, propose_value: v, isDead: false}
    proposerMgr.proposers[seq] = prop
    go func() {
      prop.Propose()
    }()
  }
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  // assertion: it must be an idempotent operation
  px.instanceState[args.Seq] = args.V
  *reply = DecideReply(true)
  return nil
}

func (proposer *Proposer) sendPropose(proposeNum int, nextProposeNum int) (int, interface{}, bool) {
	majoritySize := len(proposer.mgr.peers)/2 + 1
	prepareReplies := make(chan PrepareReply, len(proposer.mgr.peers))
	prepareBarrier := make(chan bool)

	for me, peer := range proposer.mgr.peers {
		go func(me int, peer string) {
			args := &PrepareArgs{Seq: proposer.seq, N: proposeNum}
			var reply PrepareReply

			ret := false
			if me == proposer.mgr.me {
				err := proposer.mgr.px.Prepare(args, &reply)
				if err == nil {
					ret = true
				}
			} else {
				ret = call(peer, "Paxos.Prepare", args, &reply)
			}
			prepareBarrier <- true

			if ret {
				if reply.Succ {
					prepareReplies <- reply
				} else if reply.N > nextProposeNum {
					nextProposeNum = reply.N
				}
			}
		}(me, peer)
	}

	for i := 0; i < len(proposer.mgr.peers); i++ {
		<-prepareBarrier
	}

	var acceptedValue interface{} = nil
	ok := false
	if len(prepareReplies) >= majoritySize {
		ok = true
		// choose the V_a corresponding to the max N_a to be the acceptedValue
		acceptedProposeNum := -1
		repliesNum := len(prepareReplies)
		for i := 0; i < repliesNum; i++ {
			r := <-prepareReplies
			if r.N_a > acceptedProposeNum {
				acceptedProposeNum = r.N_a
				acceptedValue = r.V_a
			}
		}
	}
	return nextProposeNum, acceptedValue, ok
}

func (proposer *Proposer) sendAccept(proposeNum int, acceptedValue interface{}, nextProposeNum int) (interface{}, int, bool) {
	majoritySize := len(proposer.mgr.peers)/2 + 1
	acceptReplies := make(chan AcceptReply, len(proposer.mgr.peers))
	acceptBarrier := make(chan bool)
	var acceptReqValue interface{}

	if acceptedValue != nil {
		acceptReqValue = acceptedValue
	} else {
		acceptReqValue = proposer.propose_value
	}

	for me, peer := range proposer.mgr.peers {
		go func(me int, peer string) {
			args := &AcceptArgs{Seq: proposer.seq, N: proposeNum, V: acceptReqValue}
			var reply AcceptReply

			ret := false
			if me == proposer.mgr.me {
				err := proposer.mgr.px.Accept(args, &reply)
				if err == nil {
					ret = true
				}
			} else {
				ret = call(peer, "Paxos.Accept", args, &reply)
			}
			acceptBarrier <- true
			if ret {
				if reply.Succ {
					acceptReplies <- reply
				} else if reply.N > nextProposeNum {
					nextProposeNum = reply.N
				}
			}
		}(me, peer)
	}

	for i := 0; i < len(proposer.mgr.peers); i++ {
		<-acceptBarrier
	}

	ok := false
	if len(acceptReplies) >= majoritySize {
		ok = true
	}
	return acceptReqValue, nextProposeNum, ok
}

func (proposer *Proposer) sendDecision(acceptReqValue interface{}) {
	for me, peer := range proposer.mgr.peers {
		go func(me int, peer string) {
			args := &DecideArgs{Seq: proposer.seq, V: acceptReqValue}
			var reply DecideReply

			ok := false
			for !ok && !proposer.isDead {
				if me == proposer.mgr.me {
					err := proposer.mgr.px.Decide(args, &reply)
					if err == nil {
						ok = true
					}
				} else {
					ok = call(peer, "Paxos.Decide", args, &reply)
				}
				time.Sleep(time.Second)
			}
		}(me, peer)
	}
}

func (proposer *Proposer) chooseNextProposeNum(nextProposeNum int) (int) {
	try_num := nextProposeNum/len(proposer.mgr.peers)*len(proposer.mgr.peers) + proposer.mgr.me
	if try_num > nextProposeNum {
		nextProposeNum = try_num
	} else {
		nextProposeNum = try_num + len(proposer.mgr.peers)
	}
	return nextProposeNum
}

func (proposer *Proposer) Propose() {
  proposeNum := proposer.mgr.me
  for !proposer.isDead {
  	nextProposeNum := proposeNum
  	nextProposeNum1, acceptedValue, ok:= proposer.sendPropose(proposeNum, nextProposeNum)
    if ok {
	  acceptReqValue, nextProposeNum2, ok := proposer.sendAccept(proposeNum, acceptedValue, nextProposeNum1)
	  nextProposeNum = nextProposeNum2
      if ok {
      	proposer.sendDecision(acceptReqValue)
        break
      }
    }
  	nextProposeNum3 := proposer.chooseNextProposeNum(nextProposeNum)
    proposeNum = nextProposeNum3

    time.Sleep(50 * time.Millisecond)
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
  if seq <= px.minDoneSeq {
    return
  }

  px.proposerMgr.RunProposer(seq, v)
}

func (px *Paxos) UpdateDoneSeqs(args *SeqArgs, reply *SeqReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  if args.Seq > px.doneSeqs[args.Sender] {
    px.doneSeqs[args.Sender] = args.Seq
    // the peer with the minimum done seq
    if args.Sender == px.minDoneIndex {
      px.minDoneSeq = args.Seq
      for index, seq := range px.doneSeqs {
        if seq < px.minDoneSeq {
          px.minDoneSeq = seq
          px.minDoneIndex = index
        }
      }

      px.proposerMgr.mu.Lock()
      for s, prop := range px.proposerMgr.proposers {
        if s <= px.minDoneSeq {
          prop.isDead = true
          delete(px.proposerMgr.proposers, s)
        }
      }
      px.proposerMgr.mu.Unlock()

      for s, _ := range px.instanceState {
        if s <= px.minDoneSeq {
          delete(px.instanceState, s)
        }
      }

      px.acceptorMgr.mu.Lock()
      for s, _ := range px.acceptorMgr.acceptors {
        if s <= px.minDoneSeq {
          delete(px.acceptorMgr.acceptors, s)
        }
      }
      px.acceptorMgr.mu.Unlock()

    }
  }
  return nil
}
//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  for me, peer := range px.peers {
    go func(me int, peer string) {
      args := &SeqArgs{Seq: seq, Sender: px.me}
      var reply SeqReply

      if me == px.me {
      	px.UpdateDoneSeqs(args, &reply)
	  } else {
	  	call(peer, "Paxos.UpdateDoneSeqs", args, &reply)
	  }
    }(me, peer)
  }
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  if px.acceptorMgr.seqMax > px.proposerMgr.seqMax {
    return px.acceptorMgr.seqMax
  } else {
    return px.proposerMgr.seqMax
  }
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
  return px.minDoneSeq + 1
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
  px.mu.Lock()
  defer px.mu.Unlock()
  if value, exist := px.instanceState[seq]; exist && seq > px.minDoneSeq {
    return true, value
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
  px.instanceState = make(map[int]interface{})

  px.doneSeqs = make([]int, len(px.peers))
  for i := 0; i < len(px.peers); i++ {
    px.doneSeqs[i] = -1
  }
  px.minDoneSeq = -1
  px.minDoneIndex = 0

  px.proposerMgr = &ProposerManager{peers: peers, me: me, proposers: make(map[int]*Proposer), seqMax: -1, seq_chosen_max: -1, px: px}
  px.acceptorMgr = &AcceptorManager{acceptors: make(map[int]*Acceptor), seqMax: -1}


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
