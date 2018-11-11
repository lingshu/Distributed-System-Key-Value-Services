package kvpaxos

import (
  "net/rpc"
  "time"
)
import "fmt"
import "crypto/rand"
import "math/big"

type Clerk struct {
  servers []string
  // You will have to modify this struct.
}


func MakeClerk(servers []string) *Clerk {
  ck := new(Clerk)
  ck.servers = servers
  // You'll have to add code here.
  return ck
}

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
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
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  // You will have to modify this function.
  args := &GetArgs{}
  args.Key = key
  args.Uid = nrand()

  var reply GetReply
  //var isSuccessful bool

  for true {
    for _, v := range ck.servers {
      ok := call(v, "KVPaxos.Get", args, &reply)
      if ok && reply.Err == OK {
        //isSuccessful = true
        return reply.Value
      }else if ok && reply.Err == ErrNoKey{
        //isSuccessful = true
        return ""
      }else if ok && reply.Err == DuplicateOp{
        //in case the previous has already succeeded, but due to networking issues the client didn't know about it
        //isSuccessful = true
        return reply.Value
      }else{DPrintf("Client.Get(%s) | RPC call Get not successful, changing server.....\n", key)}
      time.Sleep(100*time.Millisecond)
    }
    //if isSuccessful {break}

  }
  return ""
}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  // You will have to modify this function.
  args := &PutArgs{}
  args.Key = key
  args.Uid = nrand()
  args.DoHash = dohash
  args.Value = value

  var reply PutReply
  //var isSuccessful bool

  DPrintf("Client.PutExt | key: %s, value: %s, dohash: %t\n", key, value, dohash)

  for true {
    // for put, we don't care about the return value, therefore simply return reply.value
    for _, v := range ck.servers {
      ok := call(v, "KVPaxos.Put", args, &reply)
      if ok && reply.Err == OK {
        //isSuccessful = true
        //fmt.Printf("Client.PutExt | reply: ok key: %s, value: %s, dohash: %t\n", key, value, dohash)
        return reply.PreviousValue
      }else if ok && reply.Err == DuplicateOp{
        //in case the previous has already succeeded, but due to networking issues the client didn't know about it
        //isSuccessful = true
        return reply.PreviousValue
      }else{DPrintf("Client.Put(%s, dohash: %t) | RPC call Put not successful, changing server.....\n", key, dohash)}
      time.Sleep(100*time.Millisecond)
    }

    DPrintf("Retrying...\n")
    //if isSuccessful {break}

  }
  return ""
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
