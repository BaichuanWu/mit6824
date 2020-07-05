package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "regexp"
import "fmt"
import "errors"
import "strconv"
import "time"


const (
	TaskFileStatusInit = 0
	TaskFileStatusApplied = 1
	TaskFileStatusFinished = 2
	ProcessApplyMap = 0
	ProcessApplyReduce = 1
	ProcessDone = 2
)

type TaskFile struct {
	Filename string
	TaskType int
	Status int
	WorkerID int
}

type Master struct {
	// Your definitions here.
	MappFiles []*TaskFile
	ReduceFiles []*TaskFile
	ReduceCnt int
	MappIncr int
	ReduceIncr int
	Process int
	Mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//


func (m *Master) checkTask(taskType int, workerID int) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	if taskType == TaskTypeMap {
		for _, f := range m.MappFiles {
			if f.WorkerID == workerID {
				if f.Status == TaskFileStatusFinished {
					return
				} else {
					f.Status = TaskFileStatusInit
					f.WorkerID = 0
				}
			}
		}
	} else if taskType == TaskTypeReduce {
		for _, f := range m.ReduceFiles {
			if f.WorkerID == workerID {
				if f.Status == TaskFileStatusFinished {
					return
				} else {
					f.Status = TaskFileStatusInit
					f.WorkerID = 0
				}
			}

		}
	}
}

func (m *Master) getReduceFilesByReduceID(reduceID int) (taskFiles []*TaskFile) {
	reg, _ := regexp.Compile(fmt.Sprintf(`^mr-\d+-%d$`, reduceID))
	for _, f := range m.ReduceFiles {
		if reg.MatchString(f.Filename) {
			taskFiles = append(taskFiles, f)
		}
	}
	return
}

func (t *TaskFile) GetReduceID() (int, error){
	if t.TaskType == TaskTypeReduce {
		reg, _ := regexp.Compile(`^mr-\d+-(\d+)`)
		reduceID := reg.FindAllStringSubmatch(t.Filename, 1)
		return strconv.Atoi(reduceID[0][1])
	} else {
		return 0, errors.New("not support")
	}
}

func (m *Master) ApplyTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	if m.Process == ProcessApplyMap {
		for _, f := range m.MappFiles {
			if f.Status == TaskFileStatusInit {
				f.Status = TaskFileStatusApplied
				f.WorkerID = m.MappIncr
				reply.TaskFiles = append(reply.TaskFiles, *f)
				reply.TaskType = TaskTypeMap
				break;
			}
		}
		wokerID := m.MappIncr
		time.AfterFunc(10 * time.Second, func(){m.checkTask(TaskTypeMap, wokerID)})
		m.MappIncr++
	} else if m.Process == ProcessApplyReduce {
		files := make([]*TaskFile, 0)
		for _, f := range m.ReduceFiles {
			if f.Status == TaskFileStatusInit {
				reduceID, err := f.GetReduceID()
				if err != nil {
					return err
				}
				files = m.getReduceFilesByReduceID(reduceID)
				break
			}
		}
		for _, f := range files {
			f.Status = TaskFileStatusApplied
			f.WorkerID = m.ReduceIncr
			reply.TaskFiles = append(reply.TaskFiles, *f)
			reply.TaskType = TaskTypeReduce
		}
		if len(files) != 0 {
			wokerID := m.ReduceIncr
			time.AfterFunc(10 * time.Second, func(){m.checkTask(TaskTypeReduce, wokerID)})
			m.ReduceIncr++
		}
	}
	reply.ReduceCnt = m.ReduceCnt
	return nil
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	if args.TaskType == TaskTypeMap {
		isDone := true
		for _, f := range m.MappFiles {
			if f.WorkerID == args.WorkerID {
				f.Status = TaskFileStatusFinished
			}
			if f.Status != TaskFileStatusFinished {
				isDone = false
			}
		}
		for _, f:= range args.OutFiles {
			reduceFile := f
			m.ReduceFiles = append(m.ReduceFiles, &reduceFile)
		}
		if isDone {
			m.Process = ProcessApplyReduce
		}
	} else if args.TaskType == TaskTypeReduce {
		isDone := true
		for _, f := range m.ReduceFiles {
			if f.WorkerID == args.WorkerID {
				f.Status = TaskFileStatusFinished
			}
			if f.Status != TaskFileStatusFinished {
				isDone = false
			}
		}
		if isDone {
			m.Process = ProcessDone
		}

	}
	return nil
}

func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) setup(filenames[] string, reduceCnt int) {
	for _, filename := range filenames {
		m.MappFiles = append(m.MappFiles, &TaskFile{
			Filename:filename,
			TaskType:TaskTypeMap,
			Status:0,
			WorkerID:0,
			})
	}
	m.ReduceCnt = reduceCnt
	m.MappIncr = 1
	m.ReduceIncr = 1
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.Process == ProcessDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.setup(files, nReduce)

	// Your code here.


	m.server()
	return &m
}
