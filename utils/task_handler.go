package utils

import (
    "time"
    "sync"
    "sync/atomic"
    "math"
)

type TaskConfiguration struct {
    TaskListener                   func(target interface{}) (interface{}, error)
    EventListener                  func(event TaskEvent)
    MinThreadsCount                int
    MaxThreadsCount                int
    BufferLength                   int
    ThresholdOfIncreaseThreads     int
    ThresholdOfDecreaseThreads     int
    ModifyThreadsCountTimeInterval time.Duration
}

func DefaultTaskConfiguration() TaskConfiguration {
    return TaskConfiguration{
        MinThreadsCount: 1,
        MaxThreadsCount: 5,
        BufferLength: 20,
        ThresholdOfIncreaseThreads: 6,
        ThresholdOfDecreaseThreads: 2,
        ModifyThreadsCountTimeInterval: 5 * time.Second,
    }
}

const (
    Setup = iota
    Destroy = iota
    ThreadsCountChanged = iota
    StartNewTask = iota
    CompleteTask = iota
    TaskFailWithError = iota
)

const (
    needHandle = iota
    deleteThread = iota
)

type task struct {
    state  int
    target interface{}
}

type TaskHandler struct {
    configuration                         TaskConfiguration
    lock                                  sync.Mutex
    handler                               func(task interface{}) error
    waitingTasksCount                     int
    planRunningThreadsCount               int
    runningThreadsCount                   int
    runningThreadsCountLock               sync.Mutex
    latestModifyThreadsCountTimestamp     time.Time
    tasksChan                             chan interface{}
    handlersChan                          chan task
    eventChan                             chan TaskEvent
    waitingTasksCountIncreaseNotification chan int
    destroyNotification                   chan bool
    finishDestroyNotification             chan bool
    didDestroy                            bool
}

type TaskEvent struct {
    EventType int
    Target    interface{}
}

func CreateTaskHandler(configuration TaskConfiguration, handler func(interface{}) error) *TaskHandler {
    taskHandler := &TaskHandler{
        configuration: configuration,
        handler:  handler,
        waitingTasksCount: 0,
        planRunningThreadsCount: 0,
        runningThreadsCount: 0,
        latestModifyThreadsCountTimestamp: time.Now().Sub(configuration.ModifyThreadsCountTimeInterval),
        tasksChan: make(chan interface{}, configuration.BufferLength),
        handlersChan: make(chan task, 1),
        eventChan: make(chan TaskEvent, 10),
        waitingTasksCountIncreaseNotification: make(chan int, 1),
        destroyNotification: make(chan bool, 1),
        finishDestroyNotification: make(chan bool, 1),
        didDestroy: false,
    }
    go taskHandler.scheduleLoop()
    go taskHandler.eventLoop()
    return taskHandler
}

func (taskHandler *TaskHandler) updateModifyThreadsCountTimestamp() {
    taskHandler.latestModifyThreadsCountTimestamp = time.Now()
}

func (taskHandler *TaskHandler) durationOfNextModifyThreadCountNeedWaitingFor() time.Duration {
    now := time.Now()
    interval := taskHandler.configuration.ModifyThreadsCountTimeInterval
    nextTimestamp := taskHandler.latestModifyThreadsCountTimestamp.Add(interval)
    duration := math.MaxInt64(0, nextTimestamp.Sub(now))
    return duration
}

func (taskHandler *TaskHandler) scheduleLoop() {

    for taskHandler.planRunningThreadsCount < taskHandler.configuration.MinThreadsCount {
        taskHandler.createNewThread()
        taskHandler.planRunningThreadsCount++
    }
    var waitingTasksCount int
    var target interface{}

    for true {
        duration := taskHandler.durationOfNextModifyThreadCountNeedWaitingFor()
        willDeleteThread := false

        if duration == 0 {
            if waitingTasksCount >= taskHandler.configuration.ThresholdOfIncreaseThreads &&
                taskHandler.planRunningThreadsCount < taskHandler.configuration.MaxThreadsCount {

                taskHandler.createNewThread()
                taskHandler.planRunningThreadsCount++

            } else if waitingTasksCount <= taskHandler.configuration.ThresholdOfDecreaseThreads &&
                taskHandler.planRunningThreadsCount > taskHandler.configuration.MinThreadsCount {

                willDeleteThread = true
            }
            taskHandler.updateModifyThreadsCountTimestamp()
        }

        if willDeleteThread {
            select {
            case taskHandler.handlersChan <- task{state: deleteThread}:
                taskHandler.planRunningThreadsCount--

            case waitingTasksCount <- taskHandler.waitingTasksCountIncreaseNotification:
            case <-taskHandler.destroyNotification:
                taskHandler.destroy()
                return
            }
        } else if target == nil {
            select {
            case target <- taskHandler.tasksChan:
            case <-taskHandler.destroyNotification:
                taskHandler.destroy()
                return
            }
        } else {
            select {
            case taskHandler.handlersChan <- task{state: needHandle, target: target}:
                taskHandler.waitingTasksCount = atomic.AddInt32(&taskHandler.waitingTasksCount, -1)

            case waitingTasksCount <- taskHandler.waitingTasksCountIncreaseNotification:
            case <-taskHandler.destroyNotification:
                taskHandler.destroy()
                return
            }
        }
    }
}

func (taskHandler *TaskHandler) createNewThread() {

    taskListener := taskHandler.configuration.TaskListener
    eventChan := taskHandler.eventChan
    taskHandler.increaseRunningThreadsCountLock(+1)

    for true {
        task := <-taskHandler.handlersChan
        switch task.state {
        case needHandle:
            eventChan <- TaskEvent{EventType:StartNewTask, Target:task.target}
            result, err := taskListener(task.target)
            if err != nil {
                eventChan <- TaskEvent{EventType:TaskFailWithError, Target:err}
            } else {
                eventChan <- TaskEvent{EventType:CompleteTask, Target:result}
            }
        case deleteThread:
            return
        }
    }
    taskHandler.increaseRunningThreadsCountLock(-1)
}

func (taskHandler *TaskHandler) eventLoop() {
    eventChan := taskHandler.eventChan
    eventListener := taskHandler.configuration.EventListener
    eventListener(TaskEvent{EventType:Setup})
    for true {
        taskEvent := <-eventChan
        eventListener(taskEvent)
        if taskEvent.EventType == ThreadsCountChanged {
            var runningThreadsCount int = taskEvent.Target
            if runningThreadsCount == 0 {
                break
            }
        }
    }
    eventListener(TaskEvent{EventType:Destroy})
    taskHandler.finishDestroyNotification <- true
}

func (taskHandler *TaskHandler) destroy() {
    if !taskHandler.didDestroy {
        taskHandler.didDestroy = true
        for taskHandler.planRunningThreadsCount > 0 {
            taskHandler.handlersChan <- task{state: deleteThread}
            taskHandler.planRunningThreadsCount--
        }
        <-taskHandler.finishDestroyNotification // waiting for thread destroyed.
    }
}

func (taskHandler *TaskHandler) increaseWaitingTasksCountAndNotify() {
    result := atomic.AddInt32(&taskHandler.waitingTasksCount, +1)
    select { // clean original notification.
    case <-taskHandler.waitingTasksCountIncreaseNotification:
    default:
    }
    taskHandler.waitingTasksCountIncreaseNotification <- result
}

func (taskHandler *TaskHandler) increaseRunningThreadsCountLock(delta int) {
    taskHandler.runningThreadsCountLock.Lock()
    taskHandler.runningThreadsCount += delta
    taskHandler.eventChan <- TaskEvent{EventType: ThreadsCountChanged, Target: taskHandler.runningThreadsCount}
    taskHandler.runningThreadsCountLock.Unlock()
}