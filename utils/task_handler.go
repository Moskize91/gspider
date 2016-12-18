package utils

import (
    "time"
    "sync"
    "sync/atomic"
    "math"
)

type TaskConfiguration struct {
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
    runningThreadsCount                   int
    latestModifyThreadsCountTimestamp     time.Time
    tasksChan                             chan interface{}
    handlersChan                          chan task
    waitingTasksCountIncreaseNotification chan int
    destroyNotification                   chan bool
    finishDestroyNotification             chan bool
    didDestroy                            bool
}

func CreateTaskHandler(configuration TaskConfiguration, handler func(interface{}) error) *TaskHandler {
    taskHandler := &TaskHandler{
        configuration: configuration,
        handler:  handler,
        waitingTasksCount: 0,
        runningThreadsCount: 0,
        latestModifyThreadsCountTimestamp: time.Now(),
        tasksChan: make(chan interface{}, configuration.BufferLength),
        handlersChan: make(chan task, 1),
        waitingTasksCountIncreaseNotification: make(chan int, 1),
        destroyNotification: make(chan bool, 1),
        finishDestroyNotification: make(chan bool, 1),
        didDestroy: false,
    }
    go func() {
        for i := 0; i < configuration.MinThreadsCount; i++ {
            taskHandler.createNewThread()
            taskHandler.runningThreadsCount++
        }
        for true {
            select {
            case target := <-taskHandler.tasksChan:
                waitingTasksCount := atomic.LoadInt32(&taskHandler.waitingTasksCount)
                taskHandler.adjustThreadsCountWithCount(waitingTasksCount)
                atomic.AddInt32(&taskHandler.waitingTasksCount, -1)

                if taskHandler.didDestroy {
                    return
                }
                taskHandler.handlersChan <- task{
                    state: needHandle,
                    target: target,
                }
            case <-taskHandler.destroyNotification:
                taskHandler.destroy()
                return
            }
        }
    }()
    return taskHandler
}

func (taskHandler *TaskHandler) adjustThreadsCountWithCount(waitingTasksCount int) {

    duration := taskHandler.durationOfNextModifyThreadCountNeedWaitingFor()
    if duration == 0 {
        for true {
            if waitingTasksCount >= taskHandler.configuration.ThresholdOfIncreaseThreads {
                taskHandler.createNewThread()
                taskHandler.runningThreadsCount++
                break

            } else if waitingTasksCount <= taskHandler.configuration.ThresholdOfDecreaseThreads {
                select {
                case waitingTasksCount <- taskHandler.waitingTasksCountIncreaseNotification:
                case taskHandler.handlersChan <- task{state: deleteThread}:
                    taskHandler.runningThreadsCount--
                    break
                case <-taskHandler.destroyNotification:
                    taskHandler.destroy()
                    return
                }
            }
        }
        taskHandler.latestModifyThreadsCountTimestamp = time.Now()
    }
}

func (taskHandler *TaskHandler) durationOfNextModifyThreadCountNeedWaitingFor() time.Duration {
    now := time.Now()
    interval := taskHandler.configuration.ModifyThreadsCountTimeInterval
    nextTimestamp := taskHandler.latestModifyThreadsCountTimestamp.Add(interval)
    duration := math.MaxInt64(0, nextTimestamp.Sub(now))
    return duration
}

func (taskHandler *TaskHandler) destroy() {
    if !taskHandler.didDestroy {
        taskHandler.didDestroy = true
        for taskHandler.runningThreadsCount > 0 {
            taskHandler.handlersChan <- task{state: deleteThread}
            taskHandler.runningThreadsCount--
        }
        taskHandler.finishDestroyNotification <- true
    }
}

func (taskHandler *TaskHandler) createNewThread() {

}

func (taskHandler *TaskHandler) increaseWaitingTasksCountAndNotify() {
    result := atomic.AddInt32(&taskHandler.waitingTasksCount, +1)
    select { // clean original notification.
    case <-taskHandler.waitingTasksCountIncreaseNotification:
    default:
    }
    taskHandler.waitingTasksCountIncreaseNotification <- result
}