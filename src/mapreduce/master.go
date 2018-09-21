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
  inUse bool
  // You can add definitions here.
}
func (mr *MapReduce) assignJob(workerInfo *WorkerInfo, JobNum int, comJob *int, job JobType, lock *sync.Mutex) {
  var numOtherPhase int
  if job == Map {
    numOtherPhase = mr.nReduce
  } else if job == Reduce {
    numOtherPhase = mr.nMap
  }

  jobArgs := &DoJobArgs{mr.file, job, JobNum, numOtherPhase}

  var jobReply DoJobReply
  ok := call(workerInfo.address, "Worker.DoJob", jobArgs, &jobReply)
  if ok == false {
    fmt.Printf("call Worker.DoJob %d error", JobNum)
  } else {
    workerInfo.inUse = false
    lock.Lock()
    *comJob++
    lock.Unlock()
  }
}

func (mr *MapReduce) scheduleMap(workerInfo *WorkerInfo, comLock *sync.Mutex) {
  log.Printf("starting map jobs")


  workId := 0
  comJob := 0
  for {
    if workId >= mr.nMap {
      log.Printf("all map tasks sent out")
      break
    }

    workerInfo = nil
    for _, workerInfo = range mr.Workers {
      if workerInfo.inUse == false {
        break
      }
    }

    if workerInfo != nil && workerInfo.inUse == false {
      workerInfo.inUse = true
      go mr.assignJob(workerInfo, workId , &comJob, Map, comLock)

      workId++
    }
  }

  for {
    if comJob >= mr.nMap {
      break
    }
    time.Sleep(time.Duration(1) * time.Second)
  }

  log.Printf("completed all map jobs")
}

func (mr *MapReduce) scheduleReduce(workerInfo *WorkerInfo, lock *sync.Mutex) {
  log.Printf("starting reduce jobs")

  workId := 0
  comJob := 0
  for {
    if workId >= mr.nReduce {
      log.Printf("all reduce tasks sent out")
      break
    }

    for _, workerInfo = range mr.Workers {
      if workerInfo.inUse == false {
        break
      }
    }

    if workerInfo != nil && workerInfo.inUse == false {
      workerInfo.inUse = true
      go mr.assignJob(workerInfo, workId, &comJob, Reduce, lock)
      workId++
    }
  }

  for {
    if comJob >= mr.nReduce {
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
  workerAddr := <- mr.registerChannel
  _, ok := mr.Workers[workerAddr]
  if !ok {
    workerInfo := new(WorkerInfo)
    workerInfo.address = workerAddr
    workerInfo.inUse = false
    mr.Workers[workerAddr] = workerInfo
  }

  workerAddr = <- mr.registerChannel
  _, ok = mr.Workers[workerAddr]
  if !ok {
    workerInfo := new(WorkerInfo)
    workerInfo.address = workerAddr
    workerInfo.inUse = false
    mr.Workers[workerAddr] = workerInfo
  }

  var workerInfo *WorkerInfo
  comLock := new(sync.Mutex)

  mr.scheduleMap(workerInfo, comLock)
  mr.scheduleReduce(workerInfo, comLock)
  return mr.KillWorkers()
}
