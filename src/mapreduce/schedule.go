package mapreduce

import (
	"fmt"
	"sync"
)

func myFunc(waitgroup *sync.WaitGroup, mr *Master, ntasks int, nios int, id int,
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

	// schedule should process new worker registrations by reading from mr.registerChannel channel.
	// to Workers which are doing nothing
	nextInLine := <-mr.registerChannel

	// send RPC call to worker
	okOrNot := call(nextInLine, "Worker.DoTask", taskArgs, nil)

	go func() {
		mr.registerChannel <- nextInLine
	}()

	for okOrNot == false {
		nextInLine = <-mr.registerChannel
		okOrNot = call(nextInLine, "Worker.DoTask", taskArgs, nil)
		go func() {
			// if a worker call fails reschedule the call on the next available worker
			// continue giving the tasks to the workers as they resume normal
			// operation after failing as well.
			mr.registerChannel <- nextInLine

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

	for t := 0; t < ntasks; t++ {

		go myFunc(&waitgroup, mr, ntasks, nios, t, phase, mapPhase)
		// task := DoTaskArgs{mr.jobName, mr.files[t], phase, t, nios}
		// wChannels <- &task
	}
	waitgroup.Wait()

	/*
		workerName := <-mr.registerChannel
		args := new(DoTaskArgs)
		args.Phase = phase
		args.NumOtherPhase = nios
		args.JobName = mr.jobName

		for i := 0; i < ntasks; i++ {
			args.File = mr.files[i]
			args.TaskNumber = i
			ok := call(workerName, "Worker.DoTask", args, new(struct{}))

			for !ok {
				outerBreak := false
				for j := 0; j < len(mr.workers); j++ {
					ok = call(mr.workers[j], "Worker.DoTask", args, new(struct{}))
					if ok {
						outerBreak = true
						break
					}
				}
				if outerBreak {
					break
				}
			}
		}
	*/

	fmt.Printf("Schedule: %v phase done\n", phase)
}
