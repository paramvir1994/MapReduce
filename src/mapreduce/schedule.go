package mapreduce

import (
	"fmt"
	"sync"
)

func initiateScheduler(waitgroup *sync.WaitGroup, mr *Master, ntasks int, nios int, id int,
	phase jobPhase, mapPhase jobPhase) {

	defer waitgroup.Done()
	var taskArgs DoTaskArgs
	taskArgs.JobName = mr.jobName
	if phase == mapPhase {
		taskArgs.File = mr.files[id]
	}
	taskArgs.Phase = phase
	taskArgs.TaskNumber = id
	taskArgs.NumOtherPhase = nios

	//process new worker registrations by reading from mr.registerChannel.
	nextWorkerInChannel := <-mr.registerChannel

	// Call worker (RPC)
	success := call(nextWorkerInChannel, "Worker.DoTask", taskArgs, nil)

	go func() {
		mr.registerChannel <- nextWorkerInChannel
	}()

	for success == false {
		nextWorkerInChannel = <-mr.registerChannel
		success = call(nextWorkerInChannel, "Worker.DoTask", taskArgs, nil)
		go func() {
			// if a worker call fails reschedule the call on the next available worker
			// continue giving the tasks to the workers as they resume normal
			// operation after failing as well.
			mr.registerChannel <- nextWorkerInChannel

		}()

	}

}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	var waitgroup sync.WaitGroup
	waitgroup.Add(ntasks)

	for i := 0; i < ntasks; i++ {

		go initiateScheduler(&waitgroup, mr, ntasks, nios, i, phase, mapPhase)
	}
	waitgroup.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
