package gs

import (
    "time"

    "github.com/moskize91/gspider/utils"
)

type ProcessConf struct {
    MinThreadsCount                int
    MaxThreadsCount                int
    BufferLength                   int
    ThresholdOfIncreaseThreads     int
    ThresholdOfDecreaseThreads     int
    ModifyThreadsCountTimeInterval time.Duration
}

func CreateProcessConf() ProcessConf {
    return ProcessConf{
        MinThreadsCount: 1,
        MaxThreadsCount: 5,
        BufferLength: 20,
        ThresholdOfIncreaseThreads: 6,
        ThresholdOfDecreaseThreads: 2,
        ModifyThreadsCountTimeInterval: 5 * time.Second,
    }
}

type TaskPoolConf struct {
    // AddTaskFunc Add a task to the pool. Can only increase count or keep count.
    AddTaskFunc         func(task interface{})
    // GetTaskFunc Return task, true while the pool isn't empty. otherwise nil, false.
    // Must decrease count -1 once while success.
    GetTaskFunc         func() (interface{}, bool)
    // TaskFetchNotSuccess call when task fetch not success.
    TaskFetchNotSuccess func(task interface{}, fetchResult FetchResult)
    // CountFunc Get current count of tasks in pool.
    // Can't modify returning value unless called GetTaskFunc or AddTaskFunc.
    CountFunc           func() int
}

const (
    FetchResult_Success = iota
    FetchResult_404NoFound = iota
    FetchResult_Fail = iota
)

type FetchResult struct {
    FetchCode int
    Entity interface{}
    err error
}

type FetchConf struct {
    // FetchFunc Handle task and return fetch result.
    FetchFunc func(task interface{}) FetchResult
    ProcessConf ProcessConf
}

type EntityHandlerConf struct {
    HandleEntity func(entity interface{})
}

type EntityPersistenceConf struct {
    PersistenceEntity func(entity interface{}) error
    ProcessConf ProcessConf
}

type Scheduler struct {
    fetchTaskHandler *utils.TaskHandler
    persistenceTaskHandler *utils.TaskHandler
    fetchFunc func(task interface{}) FetchResult
    handleEntity func(entity interface{})
    taskFetchNotSuccess func(task interface{}, fetchResult FetchResult)
    persistenceEntity func(entity interface{}) error
}

func CreateScheduler(
    taskPoolConf TaskPoolConf, fetchConf FetchConf,
    entityHandlerConf EntityHandlerConf,
    entityPersistenceConf EntityPersistenceConf,
) *Scheduler {

    var scheduler *Scheduler = &Scheduler{
        fetchFunc: fetchConf.FetchFunc,
        handleEntity: entityHandlerConf.HandleEntity,
        taskFetchNotSuccess: taskPoolConf.TaskFetchNotSuccess,
    }

    bufferConfiguration := utils.DefaultTaskBufferConfiguration()
    bufferConfiguration.InputHandler = taskPoolConf.AddTaskFunc
    bufferConfiguration.OutputHandler = taskPoolConf.GetTaskFunc
    bufferConfiguration.CountFunc = taskPoolConf.CountFunc
    bufferConfiguration.BufferLength = fetchConf.ProcessConf.BufferLength

    fetchTaskConfiguration := utils.DefaultTaskConfiguration()
    fetchTaskConfiguration.TaskBufferConfiguration = bufferConfiguration
    fetchTaskConfiguration.MinThreadsCount = fetchConf.ProcessConf.MinThreadsCount
    fetchTaskConfiguration.MaxThreadsCount = fetchConf.ProcessConf.MaxThreadsCount
    fetchTaskConfiguration.ThresholdOfIncreaseThreads = fetchConf.ProcessConf.ThresholdOfIncreaseThreads
    fetchTaskConfiguration.ThresholdOfDecreaseThreads = fetchConf.ProcessConf.ThresholdOfDecreaseThreads
    fetchTaskConfiguration.ModifyThreadsCountTimeInterval = fetchConf.ProcessConf.ModifyThreadsCountTimeInterval
    fetchTaskConfiguration.BufferLength = 1

    fetchTaskConfiguration.TaskListener = func(task interface{}) (interface{}, error) {
        result := scheduler.fetchFunc(task)
        if result.FetchCode == FetchResult_Success {
            entity := result.Entity
            scheduler.handleEntity(entity)
        } else {
            scheduler.taskFetchNotSuccess(task, result)
        }
        return task, nil
    }
    scheduler.fetchTaskHandler = utils.CreateTaskHandler(fetchTaskConfiguration)

    persistenceTaskConfiguration := utils.DefaultTaskConfiguration()
    persistenceTaskConfiguration.TaskBufferConfiguration = bufferConfiguration
    persistenceTaskConfiguration.MinThreadsCount = entityPersistenceConf.ProcessConf.MinThreadsCount
    persistenceTaskConfiguration.MaxThreadsCount = entityPersistenceConf.ProcessConf.MaxThreadsCount
    persistenceTaskConfiguration.ThresholdOfIncreaseThreads = entityPersistenceConf.ProcessConf.ThresholdOfIncreaseThreads
    persistenceTaskConfiguration.ThresholdOfDecreaseThreads = entityPersistenceConf.ProcessConf.ThresholdOfDecreaseThreads
    persistenceTaskConfiguration.ModifyThreadsCountTimeInterval = entityPersistenceConf.ProcessConf.ModifyThreadsCountTimeInterval
    persistenceTaskConfiguration.BufferLength = entityPersistenceConf.ProcessConf.BufferLength

    persistenceTaskConfiguration.TaskListener = func(entity interface{}) (interface{}, error) {
        err := scheduler.persistenceEntity(entity)
        return entity, err
    }
    scheduler.persistenceTaskHandler = utils.CreateTaskHandler(persistenceTaskConfiguration)

    return scheduler
}