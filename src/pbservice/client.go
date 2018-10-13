package pbservice

import (
  "log"
  "math/big"
  "strconv"
  "time"

  //"math/rand"
  "crypto/rand"
  "viewservice"
)
import "net/rpc"
import "fmt"


// You'll probably need to uncomment these:
// import "time"
// import "crypto/rand"
// import "math/big"



type Clerk struct {
  vs *viewservice.Clerk
  // Your declarations here
  me string
  view viewservice.View
}


func MakeClerk(vshost string, me string) *Clerk {
  ck := new(Clerk)
  ck.vs = viewservice.MakeClerk(me, vshost)
  // Your ck.* initializations here
  ck.me = me
  ck.view = viewservice.View{0, "", ""}

  return ck
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

  // Your code here.
  args_get := &GetArgs{key}
  var reply_get GetReply
  ok := false
  for !ok {
   ok = call(ck.view.Primary, "PBServer.Get", args_get, &reply_get)
   if !ok || reply_get.Err == ErrWrongServer {
     time.Sleep(viewservice.PingInterval)
     v, err := ck.vs.Ping(ck.view.Viewnum)
     if err != nil {
       continue
     }
     ck.view = v
     ok = false
   }
  }

  return reply_get.Value
  //args := &GetArgs{key}
  //var reply GetReply
  //ok := false
  //
  //for !ok {
  //  ok = call(ck.view.Primary, "PBServer.Get", args, &reply)
  //  if !ok || reply.Err == ErrWrongServer {
  //    time.Sleep(viewservice.PingInterval)
  //    view, err := ck.vs.Ping(ck.view.Viewnum)
  //    if err != nil {
  //      //log.Printf("Client %s get view from viewservice failed\n", ck.me)
  //      continue
  //    }
  //    ck.view = view
  //    ok = false
  //  }
  //}
  //
  //return reply.Value
}

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}
//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {

  // Your code here.
  args_put := &PutArgs{key, value, dohash, strconv.FormatInt(nrand(), 10), ck.me}
  var reply_put PutReply
  for ok := false; !ok; {
    ok = call(ck.view.Primary, "PBServer.Put", args_put, &reply_put)
    if !ok || reply_put.Err == ErrWrongServer {
      time.Sleep(viewservice.PingInterval)
      v, err := ck.vs.Ping(ck.view.Viewnum)
      if err != nil {
        log.Printf("Client %s ping viewservice failed\n", ck.me)
        continue
      }
      ck.view = v
      ok = false
    } else {
      if reply_put.Err == ErrDuplicateKey {
        return reply_put.PreviousValue
      } else if !dohash {
        if reply_put.Err == OK {
          return ""
        }
      } else {
        if reply_put.Err == OK {
          return reply_put.PreviousValue
        }
      }
    }
    time.Sleep(3*viewservice.PingInterval)
  }
  //args := &PutArgs{key, value, dohash, strconv.FormatInt(nrand(), 10), ck.me}
  //var reply PutReply
  //
  //for ok := false; !ok; {
  //  ok = call(ck.view.Primary, "PBServer.PutAppend", args, &reply)
  //  if !ok || reply.Err == ErrWrongServer {
  //    time.Sleep(viewservice.PingInterval)
  //    view, err := ck.vs.Ping(ck.view.Viewnum)
  //    if err != nil {
  //      log.Printf("Client %s get view from viewservice failed\n", ck.me)
  //      continue
  //    }
  //    ck.view = view
  //    ok = false
  //  }
  //}
  return "???"
}

func (ck *Clerk) Put(key string, value string) {
  //DPrintf("Clerk.Put .... client sends put with key %s and value %s\n", key, value)
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
