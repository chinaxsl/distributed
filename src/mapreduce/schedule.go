package mapreduce

import (
	"fmt"
	"sync"
)
//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//  sync.WaitGroup 类似 Java的CountDownLatch
	var signals sync.WaitGroup = sync.WaitGroup{}
	signals.Add(ntasks)
	for i:=0; i < ntasks; i++ {
		// 设置task的参数
		args := DoTaskArgs{jobName,mapFiles[i],phase,i,n_other}
		go func() {
			defer signals.Done()
			registerWorker := <- registerChan
			// 当call()的结果为false时，rpc调用失败，另选择一个worker调用call()
			for ret:= call(registerWorker,"Worker.DoTask",args,nil);!ret; {
				registerWorker = <-registerChan
				ret = call(registerWorker,"Worker.DoTask",args,nil)
			}
			// 必须另起协程执行 否则会发生死锁
			go func() {
				registerChan <- registerWorker
			}()
		}()
	}
	// 主线程等待所有协程退出
	signals.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
