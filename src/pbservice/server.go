package pbservice

import (
	"net"
	"strconv"
)
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.
  mu sync.Mutex
  view viewservice.View
  key_value map[string]string
  requested map[string]int
  reply_dic map[string]string
  rw_mu sync.RWMutex
}

func concatHash(prevStr string, newStr string) string {
	concatStr := prevStr + newStr
	res := hash(concatStr)

	return strconv.Itoa(int(res))
}


func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  key := args.UniqueKey
  pb.rw_mu.Lock()
  defer pb.rw_mu.Unlock()
  value_requested, ok := pb.requested[key]
  //if value_requested == 2 {
  // reply.Err = ErrDuplicateKey
  //} else if !ok {
	//  fmt.Printf("yesyes\n")
	//  if pb.me == pb.view.Primary {
	//	  fmt.Printf("yesyesyes\n")
	//	  if args.UniqueKey == "" {
	//		  reply.Err = ErrEmptyKey
	//	  }
	//	  pb.requested[key] = 1
  //
	//	  ok_backup := false
	//	  args.ForwardClerk = pb.me
	//	  var reply_backup PutReply
	//	  if pb.view.Backup != "" {
	//		  ok_backup = call(pb.view.Backup, "PBServer.Put", args, reply_backup)
	//	  } else {
	//		  ok_backup = true
	//	  }
  //
	//	  for !ok_backup {
	//		  time.Sleep(viewservice.PingInterval)
	//		  if pb.me != pb.view.Primary {
	//			  ok_backup = false
	//			  break
	//		  } else if pb.view.Backup != "" {
	//			  ok_backup = call(pb.view.Backup, "PBServer.Put", args, &reply_backup)
	//		  } else {
	//			  ok_backup = true
	//		  }
	//	  }
  //
	//	  if ok_backup {
	//		  //pb.Dohash(args, reply)
	//		  if !args.DoHash {
	//			  pb.key_value[args.Key] = args.Value
	//			  reply.Err = OK
	//		  } else {
	//			  fmt.Printf("yesyesyoyoyo\n")
	//			  prevStr, exist := pb.key_value[args.Key]
	//			  if exist {
	//				  newVal := concatHash(prevStr, args.Value)
	//				  reply.PreviousValue = prevStr
	//				  pb.key_value[args.Key] = newVal
	//			  } else {
	//				  newVal := concatHash("", args.Value)
	//				  reply.PreviousValue = ""
	//				  pb.key_value[args.Key] = newVal
	//			  }
	//			  reply.Err = OK
	//			  pb.requested[key] = 2
	//		  }
	//	  } else {
	//		  reply.Err = ErrWrongServer
	//	  }
	//  } else if pb.me == pb.view.Backup && args.ForwardClerk == pb.view.Primary {
	//	  //pb.Dohash(args, reply)
	//	  if !args.DoHash {
	//		  pb.key_value[args.Key] = args.Value
	//		  reply.Err = OK
	//	  } else {
	//		  prevStr, exist := pb.key_value[args.Key]
	//		  if exist {
	//			  newVal := concatHash(prevStr, args.Value)
	//			  reply.PreviousValue = prevStr
	//			  pb.key_value[args.Key] = newVal
	//		  } else {
	//			  newVal := concatHash("", args.Value)
	//			  reply.PreviousValue = ""
	//			  pb.key_value[args.Key] = newVal
	//		  }
	//		  reply.Err = OK
	//		  pb.requested[key] = 2
	//	  }
	//  }
  //}
	if !ok || value_requested != 2 {
		if pb.me == pb.view.Primary {
			if args.UniqueKey == "" {
				reply.Err = ErrEmptyKey
			}
			pb.requested[key] = 1

			ok_backup := false
			args.ForwardClerk = pb.me
			//var reply_backup PutReply
			reply_backup := PutReply{}
			if pb.view.Backup != "" {
				ok_backup = call(pb.view.Backup, "PBServer.Put", args, &reply_backup)
			} else {
				ok_backup = true
			}

			for !ok_backup {
				time.Sleep(viewservice.PingInterval)
				if pb.me != pb.view.Primary {
					ok_backup = false
					break
				} else if pb.view.Backup != "" {
					reply_backup = PutReply{}
					ok_backup = call(pb.view.Backup, "PBServer.Put", args, &reply_backup)
				} else {
					ok_backup = true
				}
			}

			if ok_backup {
				//pb.Dohash(args, reply)
				if !args.DoHash {
					pb.key_value[args.Key] = args.Value
					reply.Err = OK
				} else {
					prevStr, exist := pb.key_value[args.Key]
					if exist {
						newVal := concatHash(prevStr, args.Value)
						reply.PreviousValue = prevStr
						pb.key_value[args.Key] = newVal
						pb.reply_dic[key] = prevStr
					} else {
						newVal := concatHash("", args.Value)
						reply.PreviousValue = ""
						pb.key_value[args.Key] = newVal
						pb.reply_dic[key] = ""
					}
					reply.Err = OK
					pb.requested[key] = 2
				}
			} else {
				reply.Err = ErrWrongServer
			}
		} else if pb.me == pb.view.Backup && args.ForwardClerk == pb.view.Primary {
				  if !args.DoHash {
					   	pb.key_value[args.Key] = args.Value
					   	reply.Err = OK
				  } else {
				    prevStr, exist := pb.key_value[args.Key]
				    if exist {
					  newVal := concatHash(prevStr, args.Value)
					  reply.PreviousValue = prevStr
					  pb.key_value[args.Key] = newVal
					  pb.reply_dic[key] = prevStr
				    } else {
					  newVal := concatHash("", args.Value)
					  reply.PreviousValue = ""
					  pb.key_value[args.Key] = newVal
					  pb.reply_dic[key] = ""
					}
				    reply.Err = OK
				    pb.requested[key] = 2
				  }
			  } else {
			  	reply.Err = ErrWrongServer
			}
		} else {
			//fmt.Printf("value_requested == 2 %v\n", value_requested)
			//fmt.Printf("hahahahah\n")

		    //if !args.DoHash {
		    //	pb.DoDuplicate("Put", args.UniqueKey, reply)
			//} else {
			//	pb.DoDuplicate("Hash", args.UniqueKey, reply)
			//}
			if args.DoHash {
				reply.PreviousValue = pb.reply_dic[key]
			}
			reply.Err = ErrDuplicateKey
	}


  return nil
}

//func (pb *PBServer) PutAppend(args *PutArgs, reply *PutReply) error {
//
//  //used for at-most-once
//  request_key := args.UniqueKey
//  pb.rw_mu.Lock()
//  request_value, ok := pb.requested[request_key]
//  if !ok || request_value != 2 {
//    //primary deal with the request
//    if pb.me == pb.view.Primary {
//      if args.Key == "" {
//        reply.Err = ErrEmptyKey
//      }
//      pb.requested[request_key] = 1
//      //transmit the request to the backup
//      args.ForwardClerk = pb.me
//      var backup_reply PutReply
//      backup_ok := false
//      if pb.view.Backup != "" {
//        backup_ok = call(pb.view.Backup, "PBServer.PutAppend", args, &backup_reply)
//      } else {
//        backup_ok = true
//      }
//      //if backup return false, maybe the backup is alive but
//      //a network error occurred, or the backup is dead. so
//      //wait ping interval to get the latest view, and then retry
//      for !backup_ok {
//        time.Sleep(viewservice.PingInterval)
//        if pb.view.Primary != pb.me {
//          backup_ok = false
//          break
//        } else if pb.view.Backup != "" {
//          backup_ok = call(pb.view.Backup, "PBServer.PutAppend", args, &backup_reply)
//        } else {
//          backup_ok = true
//        }
//      }
//      if backup_ok {
//        if _, exist := pb.key_value[args.Key]; args.DoHash == true && exist {
//          pb.key_value[args.Key] += args.Value
//        } else {
//          pb.key_value[args.Key] = args.Value
//        }
//        pb.requested[request_key] = 2
//        reply.Err = OK
//      } else {
//        reply.Err = ErrWrongServer
//      }
//    } else if pb.me == pb.view.Backup && args.ForwardClerk == pb.view.Primary {
//      //backup deal with the request transmitted from primary
//      if _, exist := pb.key_value[args.Key]; args.DoHash == true && exist {
//        pb.key_value[args.Key] += args.Value
//      } else {
//        pb.key_value[args.Key] = args.Value
//      }
//      pb.requested[request_key] = 2
//      reply.Err = OK
//    } else {
//      //if the server isn't primary, reject the request
//      reply.Err = ErrWrongServer
//    }
//  } else {
//    reply.Err = ErrDuplicateKey
//  }
//  defer pb.rw_mu.Unlock()
//  return nil
//}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  pb.rw_mu.RLock()
  defer pb.rw_mu.RUnlock()

  if pb.me == pb.view.Primary {
   value, ok := pb.key_value[args.Key]
   if ok {
     reply.Err = OK
     reply.Value = value
   } else {
     reply.Err = ErrNoKey
   }
  } else {
   reply.Err = ErrWrongServer
  }
  return nil
  //pb.rw_mu.RLock()
  //if pb.me == pb.view.Primary {
  //  value, ok := pb.key_value[args.Key]
  //  if ok {
  //    reply.Err = OK
  //    reply.Value = value
  //  } else {
  //    reply.Err = ErrNoKey
  //  }
  //} else {
  //  //if the server isn't primary, reject the request
  //  reply.Err = ErrWrongServer
  //}
  //
  //defer pb.rw_mu.RUnlock()
  //return nil
}

// handle the transfer of the complete key/value database from the primary to a new backup
func (pb *PBServer) Init(args *InitArgs, reply *InitReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  if pb.view.Primary == args.Primary {
    pb.rw_mu.Lock()
    pb.key_value = args.KeyValue
    //for k, v := range args.KeyValue {
    //	pb.key_value[k] = v
	//}
    pb.rw_mu.Unlock()
    reply.Err = OK
  } else {
    reply.Err = ErrWrongServer
  }
  return nil
}
//func (pb *PBServer) SyncBackup(args *InitArgs, reply *InitReply) error {
//
//  // Your code here.
//  pb.mu.Lock()
//  if args.Primary == pb.view.Primary {
//    pb.rw_mu.Lock()
//    pb.key_value = args.KeyValue
//    pb.rw_mu.Unlock()
//    reply.Err = OK
//  } else {
//    reply.Err = ErrWrongServer
//  }
//  defer pb.mu.Unlock()
//
//  return nil
//}
// ping the viewserver periodically.
// if new backup:
//    handle the transfer from primary to new backup
//    transfer PBServer's view to new view
func (pb *PBServer) tick() {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()

  v, err := pb.vs.Ping(pb.view.Viewnum)
  if err == nil {
	  if pb.me == v.Primary && v.Backup != "" && v.Backup != pb.view.Backup {
		  args_init := &InitArgs{pb.key_value, pb.view.Primary}
		  //var reply_init InitReply
		  reply_init := InitReply{}
		  ok := call(v.Backup, "PBServer.Init", args_init, &reply_init)
		  if !ok || reply_init.Err != OK {
		  	log.Fatal("Error: Initial Backup failed\n")
		  }
	  }
  }
  pb.view = v
   //if pb.me == v.Primary {
   //  if v.Backup != "" {
   //    if v.Backup != pb.view.Backup {
   //      args_init := &InitArgs{pb.key_value, pb.view.Primary}
   //      var reply_init InitReply
   //      ok := call(v.Backup, "PBServer.Init", args_init, &reply_init)
   //      fmt.Printf("%v\n", ok)
   //      fmt.Printf("%v\n", reply_init.Err)
   //      if !ok || reply_init.Err != OK {
   //        log.Fatal("Error: Initial Backup failed\n")
   //      }
   //    }
   //  }
   //}

  //pb.mu.Lock()
  //reply_view, err := pb.vs.Ping(pb.view.Viewnum)
  //if err == nil {
  //  if pb.me == reply_view.Primary && reply_view.Backup != "" && reply_view.Backup != pb.view.Backup {
  //    args := &InitArgs{pb.key_value, pb.view.Primary}
  //    var reply InitReply
  //    ok := call(reply_view.Backup, "PBServer.SyncBackup", args, &reply)
  //    if !ok || reply.Err != OK {
  //      log.Fatal("Error : Sync Backup failed\n")
  //    }
  //  }
  //}
  //pb.view = reply_view
  //defer pb.mu.Unlock()
}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  pb.view = viewservice.View{0, "", ""}
  pb.key_value = make(map[string]string)
  pb.requested = make(map[string]int)
  pb.reply_dic = make(map[string]string)

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait() 
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
