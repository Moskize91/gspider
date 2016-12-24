package utils

import (
    "time"
    "sync"
    "sync/atomic"
)

type TaskConfiguration struct {
    TaskBufferConfiguration        TaskBufferConfiguration
    TaskListener                   func(target interface{}) (interface{}, error)
    EventListeners                 map[int]func(event TaskEvent)
    MinThreadsCount                int
    MaxThreadsCount                int
    BufferLength                   int
    ThresholdOfIncreaseThreads     int
    ThresholdOfDecreaseThreads     int
    ModifyThreadsCountTimeInterval time.Duration
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
    buffer                                *taskBuffer
    lock                                  sync.Mutex
    waitingTasksCount                     int32
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

func DefaultTaskConfiguration() TaskConfiguration {
    return TaskConfiguration{
        TaskListener: func(target interface{}) (interface{}, error) {
            return nil, nil
        },
        EventListeners: make(map[int]func(event TaskEvent)),
        MinThreadsCount: 1,
        MaxThreadsCount: 5,
        BufferLength: 1,
        ThresholdOfIncreaseThreads: 6,
        ThresholdOfDecreaseThreads: 2,
        ModifyThreadsCountTimeInterval: 5 * time.Second,
    }
}

func CreateTaskHandler(configuration TaskConfiguration) *TaskHandler {
    standardizeTaskConfiguration(&configuration)

    taskHandler := &TaskHandler{
        configuration: configuration,
        waitingTasksCount: 0,
        planRunningThreadsCount: 0,
        runningThreadsCount: 0,
        latestModifyThreadsCountTimestamp: time.Now().Add(-configuration.ModifyThreadsCountTimeInterval),
        tasksChan: make(chan interface{}, configuration.BufferLength),
        handlersChan: make(chan task, 1),
        eventChan: make(chan TaskEvent, 10),
        waitingTasksCountIncreaseNotification: make(chan int, 1),
        destroyNotification: make(chan bool, 1),
        finishDestroyNotification: make(chan bool, 1),
        didDestroy: false,
    }

    bufferConfiguration := configuration.TaskBufferConfiguration
    if bufferConfiguration.used {
        buffer := createTaskBuffer(bufferConfiguration, taskHandler.tasksChan, func(delta int) {
            taskHandler.increaseWaitingTasksCountAndNotify(delta)
        })
        taskHandler.buffer = buffer
    }
    go taskHandler.scheduleLoop()
    go taskHandler.eventLoop()

    return taskHandler
}

func (taskHandler *TaskHandler) AddTask(target interface{}) {
    if !taskHandler.didDestroy {
        bufferConfiguration := taskHandler.configuration.TaskBufferConfiguration
        if bufferConfiguration.used {
            taskHandler.buffer.inputChan <- target
        } else {
            taskHandler.tasksChan <- target
            taskHandler.increaseWaitingTasksCountAndNotify(+1)
        }
    }
}

func (taskHandler *TaskHandler) Destroy() {
    if !taskHandler.didDestroy {
        taskHandler.didDestroy = true
        if taskHandler.configuration.TaskBufferConfiguration.used {
            taskHandler.buffer.destroy()
        }
        taskHandler.destroyNotification <- true
        <-taskHandler.finishDestroyNotification // waiting for thread destroyed.
    }
}

func standardizeTaskConfiguration(configuration *TaskConfiguration) {
    eventListeners := make(map[int]func(event TaskEvent))
    for key, value := range configuration.EventListeners {
        eventListeners[key] = value
    }
    configuration.EventListeners = eventListeners
}

func (taskHandler *TaskHandler) increaseWaitingTasksCountAndNotify(delta int) {
    result := int(atomic.AddInt32(&taskHandler.waitingTasksCount, int32(delta)))
    select { // clean original notification.
    case <-taskHandler.waitingTasksCountIncreaseNotification:
    default:
    }
    taskHandler.waitingTasksCountIncreaseNotification <- result
}

func (taskHandler *TaskHandler) updateModifyThreadsCountTimestamp() {
    taskHandler.latestModifyThreadsCountTimestamp = time.Now()
}

func (taskHandler *TaskHandler) durationOfNextModifyThreadCountNeedWaitingFor() time.Duration {
    now := time.Now()
    interval := taskHandler.configuration.ModifyThreadsCountTimeInterval
    nextTimestamp := taskHandler.latestModifyThreadsCountTimestamp.Add(interval)
    duration := nextTimestamp.Sub(now)
    if duration < 0 {
        duration = 0
    }
    return duration
}

func (taskHandler *TaskHandler) scheduleLoop() {

    for taskHandler.planRunningThreadsCount < taskHandler.configuration.MinThreadsCount {
        taskHandler.createNewThread()
        taskHandler.planRunningThreadsCount++
    }
    var waitingTasksCount int
    var target interface{}

    mainLoop: for true {
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
            case <-taskHandler.destroyNotification:
                taskHandler.destroy()
                break mainLoop

            case taskHandler.handlersChan <- task{state: deleteThread}:
                taskHandler.planRunningThreadsCount--

            case waitingTasksCount = <-taskHandler.waitingTasksCountIncreaseNotification:
            }
        } else if target == nil {
            select {
            case <-taskHandler.destroyNotification:
                taskHandler.destroy()
                break mainLoop

            case target = <-taskHandler.tasksChan:
            }
        } else {
            select {
            case <-taskHandler.destroyNotification:
                taskHandler.destroy()
                break mainLoop

            case taskHandler.handlersChan <- task{state: needHandle, target: target}:
                target = nil
                taskHandler.waitingTasksCount = atomic.AddInt32(&taskHandler.waitingTasksCount, -1)

            case waitingTasksCount = <-taskHandler.waitingTasksCountIncreaseNotification:
            }
        }
    }
}

func (taskHandler *TaskHandler) createNewThread() {

    taskListener := taskHandler.configuration.TaskListener
    eventChan := taskHandler.eventChan
    taskHandler.increaseRunningThreadsCountLock(+1)

    go func() {
        mainLoop: for true {
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
                break mainLoop
            }
        }
        taskHandler.increaseRunningThreadsCountLock(-1)
    }()
}

func (taskHandler *TaskHandler) eventLoop() {
    eventChan := taskHandler.eventChan
    taskHandler.handleEvent(TaskEvent{EventType:Setup})
    for true {
        taskEvent := <-eventChan
        taskHandler.handleEvent(taskEvent)
        if taskEvent.EventType == ThreadsCountChanged {
            runningThreadsCount, ok := taskEvent.Target.(int)
            if ok && runningThreadsCount == 0 {
                break
            }
        }
    }
    taskHandler.handleEvent(TaskEvent{EventType:Destroy})
    taskHandler.finishDestroyNotification <- true
}

func (taskHandler *TaskHandler) handleEvent(taskEvent TaskEvent) {
    eventListeners := taskHandler.configuration.EventListeners
    listener := eventListeners[taskEvent.EventType]
    if listener != nil {
        listener(taskEvent)
    }
}

func (taskHandler *TaskHandler) destroy() {
    for taskHandler.planRunningThreadsCount > 0 {
        taskHandler.handlersChan <- task{state: deleteThread}
        taskHandler.planRunningThreadsCount--
    }
}

func (taskHandler *TaskHandler) increaseRunningThreadsCountLock(delta int) {
    taskHandler.runningThreadsCountLock.Lock()
    taskHandler.runningThreadsCount += delta
    taskHandler.eventChan <- TaskEvent{EventType: ThreadsCountChanged, Target: taskHandler.runningThreadsCount}
    taskHandler.runningThreadsCountLock.Unlock()
}