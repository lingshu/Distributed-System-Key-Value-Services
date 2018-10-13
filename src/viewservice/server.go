
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
  //m map[string]time.Time
  view View
  ack bool
  idle string
  count_primary int
  count_backup int
  dead_primary bool
  dead_backup bool
  backup_ready bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
  vs.mu.Lock()
  defer vs.mu.Unlock()

  if (vs.view.Viewnum == 0) {
   vs.view.Viewnum = 1
   vs.view.Primary = args.Me
   vs.ack = false
  }

  if (vs.view.Primary == args.Me) {
    vs.count_primary = 0
    if (args.Viewnum == 0 && vs.view.Viewnum != 1) {
      vs.dead_primary = true
    } else if (args.Viewnum == vs.view.Viewnum && vs.ack == false) {
      vs.ack = true
    }
  }

  if (vs.view.Backup == args.Me) {
    vs.count_backup = 0
    //fmt.Printf("%v\n", args.last)
    if (args.Viewnum == 0) {
      //args.last = args.Viewnum
      vs.backup_ready = false
      //vs.dead_backup = true
    } else if (args.Viewnum == vs.view.Viewnum) {
      //args.last = args.Viewnum
      vs.backup_ready = true
    }
  }

  if (vs.view.Primary != args.Me && vs.view.Backup != args.Me) {
    vs.idle = args.Me
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

func (vs *ViewServer) changeView(new_primary string, new_backup string) {
  vs.view = View{vs.view.Viewnum + 1, new_primary, new_backup}
  vs.ack = false
  vs.backup_ready = false
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

  //check whether overtime
  if vs.view.Primary != "" {
    if vs.count_primary == DeadPings {
      vs.count_primary = 0
      vs.dead_primary = true
    } else {
      vs.count_primary += 1
    }
  }

  if vs.view.Backup != "" {
    if vs.count_backup == DeadPings {
      vs.count_backup = 0
      vs.dead_backup = true
    } else {
      vs.count_backup += 1
    }
  }


  //if the server dead, change the current view
  if (vs.ack) {
    isViewChanged := false
    if vs.dead_primary {
      vs.view.Primary = ""
      if (vs.view.Backup != "" && vs.backup_ready) {
        //vs.changeView(vs.view.Backup, vs.idle)
        vs.view.Primary = vs.view.Backup
        vs.view.Backup = ""
        vs.dead_primary = false
        //vs.ack = false
        vs.backup_ready = false
        //vs.view.Viewnum += 1
        isViewChanged = true
      }
    }

    if vs.dead_backup {
      vs.view.Backup = ""
      if (vs.idle != "") {
        //vs.changeView(vs.view.Primary, vs.idle)
        vs.view.Backup = vs.idle
        vs.idle = ""
        vs.dead_backup = false
        vs.backup_ready = false
        isViewChanged = true
      }
    }

    if (vs.view.Backup == "" && vs.idle != "") {
      vs.view.Backup = vs.idle
      vs.idle = ""
      isViewChanged = true
      //vs.view.Viewnum += 1
      //vs.ack = false
    }

    if isViewChanged {
      vs.view.Viewnum += 1
      vs.ack = false
    }
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
  vs.dead_backup = false
  vs.dead_primary = false
  vs.idle = ""
  vs.count_backup = 0
  vs.count_primary = 0

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
