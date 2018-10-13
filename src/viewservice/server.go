
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.
  view        View
  view_primary  uint
  view_backup   uint
  count_primary uint
  count_backup  uint
  count_current uint
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
  vs.mu.Lock()
  defer vs.mu.Unlock()

  switch {

  case vs.view.Primary == "" && vs.view.Viewnum == 0:
    vs.view.Primary = args.Me
    vs.view.Viewnum = args.Viewnum + 1
    vs.view_primary = 0
    vs.count_primary = vs.count_current

  case vs.view.Primary == args.Me:
    if args.Viewnum == 0 {
      if vs.view.Backup != "" {
        vs.view.Primary = vs.view.Backup
        vs.view.Backup = ""
        vs.view.Viewnum++
        vs.view_primary = vs.view_backup
        vs.count_primary = vs.count_backup
      }
    } else {
      vs.view_primary = args.Viewnum
      vs.count_primary = vs.count_current
    }

  case vs.view.Backup == "" && vs.view.Viewnum == vs.view_primary:
    vs.view.Backup = args.Me
    vs.view.Viewnum++
    vs.count_backup = vs.count_current

  case vs.view.Backup == args.Me:
    if args.Viewnum == 0 && vs.view.Viewnum == vs.view_primary {
      vs.view.Backup = args.Me
      vs.view.Viewnum++
      vs.count_backup = vs.count_current
    } else if args.Viewnum != 0 {
      vs.count_backup = vs.count_current
    }
  }

  reply.View = vs.view


  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
  vs.mu.Lock()
  defer vs.mu.Unlock()
  reply.View = vs.view

  return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  // Your code here.
  vs.mu.Lock()
  defer vs.mu.Unlock()

  vs.count_current++
  if vs.count_current-vs.count_primary >= DeadPings && vs.view.Viewnum == vs.view_primary {
    if vs.view.Backup != "" {
      vs.view.Primary = vs.view.Backup
      vs.view.Backup = ""
      vs.view.Viewnum++
      vs.view_primary = vs.view_backup
      vs.count_primary = vs.count_backup
    }
  }

  if vs.view.Backup != "" && vs.count_current-vs.count_backup >= DeadPings && vs.view.Viewnum == vs.view_primary {
    vs.view.Backup = ""
    vs.view.Viewnum++
  }
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
  vs.view = View{0, "", ""}
  vs.view_primary = 0
  vs.view_backup = 0
  vs.count_primary = 0
  vs.count_backup = 0
  vs.count_current = 0


  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
