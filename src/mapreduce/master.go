package mapreduce

import (
  "container/list"
  "log"
  "sync"
  "time"
)
import "fmt"

type WorkerInfo struct {
  address string
  state int
  // You can add definitions here.
}
func (mr *MapReduce) assignJob(workerInfo *WorkerInfo, jobId int, comJob *int, job JobType, lock *sync.Mutex) (bool) {
  var numOtherPhase int
  if job == Map {
    numOtherPhase = mr.nReduce
  } else if job == Reduce {
    numOtherPhase = mr.nMap
  }

  jobArgs := &DoJobArgs{mr.file, job, jobId, numOtherPhase}

  var jobReply DoJobReply
  ok := call(workerInfo.address, "Worker.DoJob", jobArgs, &jobReply)
  if ok == false {
    fmt.Printf("call Worker.DoJob %d error", jobId)
    workerInfo.state = 2
    log.Printf("work failed")
    return false
  } else {
    workerInfo.state = 0
    lock.Lock()
    *comJob++
    lock.Unlock()
    return true
  }
}

func (mr *MapReduce) scheduleMap(workerInfo *WorkerInfo, comLock *sync.Mutex) {
  log.Printf("starting map jobs")

  workId, comJobNum := 0, 0
  for {
    if workId >= mr.nMap {
      log.Printf("all map tasks sent out")
      break
    }

    workerInfo = nil
    comLock.Lock()
    for _, workerInfo = range mr.Workers {
      if workerInfo.state == 0 {
        break
      }
    }
    comLock.Unlock()

    fmt.Printf("hello\n")

    if workerInfo != nil && workerInfo.state == 0 {
      workerInfo.state = 1
      fmt.Printf("helloagain\n")
      if mr.assignJob(workerInfo, workId , &comJobNum, Map, comLock) == true {
        fmt.Printf("helloworld\n")
        comLock.Lock()
        workId++
        comLock.Unlock()
      }
    }

  }

  for {
    if comJobNum >= mr.nMap {
      break
    }
    time.Sleep(time.Duration(1) * time.Second)
  }

  log.Printf("completed all map jobs")
}

func (mr *MapReduce) scheduleReduce(workerInfo *WorkerInfo, lock *sync.Mutex) {
  log.Printf("starting reduce jobs")

  workId, comJobNum := 0, 0
  for {
    if workId >= mr.nReduce {
      log.Printf("all reduce tasks sent out")
      break
    }

    lock.Lock()
    for _, workerInfo = range mr.Workers {
      if workerInfo.state == 0 {
        break
      }
    }
    lock.Unlock()

    fmt.Printf("hello\n")

    if workerInfo != nil && workerInfo.state == 0 {
      workerInfo.state = 1
      fmt.Printf("helloagain\n")
      if mr.assignJob(workerInfo, workId, &comJobNum, Reduce, lock) == true {
        lock.Lock()
        workId++
        lock.Unlock()
      }
    }
  }

  for {
    if comJobNum >= mr.nReduce {
      break
    }
    time.Sleep(time.Duration(1) * time.Second)
  }

  log.Printf("completed all reduce jobs")
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {
  log.Printf("starting RunMaster")
  // Your code here
  lock := new(sync.Mutex)
  go func() {
    for {
      workerAddr := <- mr.registerChannel
      lock.Lock()
      _, ok := mr.Workers[workerAddr]
      if !ok {
       workerInfo := new(WorkerInfo)
       workerInfo.address = workerAddr
       workerInfo.state = 0
       mr.Workers[workerAddr] = workerInfo
      } else {
        mr.Workers[workerAddr].address = workerAddr
        mr.Workers[workerAddr].state = 0
      }
      lock.Unlock()
    }
  }()

  var workerInfo *WorkerInfo

  mr.scheduleMap(workerInfo, lock)
  mr.scheduleReduce(workerInfo, lock)
  return mr.KillWorkers()
}
