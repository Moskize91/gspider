package utils

import "sync/atomic"

type TaskBufferConfiguration struct {
    used          bool
    // InputHandler can only increase count or keep count.
    InputHandler  func(target interface{})
    // OutputHandler decrease count -1 once.
    OutputHandler func() (interface{}, bool)
    // can't modify returning value unless called InputHandler or OutputHandler.
    CountFunc     func() int
    BufferLength  int
}

func DefaultTaskBufferConfiguration() TaskBufferConfiguration {
    return TaskBufferConfiguration{
        used: true,
        InputHandler: func(target interface{}) {},
        OutputHandler: func() (interface{}, bool) {
            return nil, false
        },
        BufferLength: 20,
        CountFunc: nil,
    }
}

type taskBuffer struct {
    inputChan                 chan interface{}
    outputChan                chan interface{}
    destroyNotification       chan bool
    finishDestroyNotification chan bool
    countDidIncrease          func(delta int)
    count                     int
    didDestroy                bool
    configuration             TaskBufferConfiguration
}

func createTaskBuffer(configuration TaskBufferConfiguration, outputChan chan interface{}, countDidIncrease func(delta int)) *taskBuffer {
    inputChan := make(chan interface{}, configuration.BufferLength)
    buffer := taskBuffer{
        inputChan: inputChan,
        outputChan: outputChan,
        destroyNotification: make(chan bool, 1),
        finishDestroyNotification: make(chan bool, 1),
        countDidIncrease: countDidIncrease,
        count: 0,
        configuration: configuration,
    }
    go buffer.handleLoop()

    return buffer
}

func (buffer *taskBuffer) handleLoop() {

    outputHandler := buffer.configuration.OutputHandler
    willInputNow := true

    var outputTarget interface{}
    var exists bool

    for true {
        if willInputNow {
            willInputNow = false
            if buffer.count() > 0 {
                select {
                case <-buffer.destroyNotification:
                    return

                case inputTarget := <-buffer.inputChan:
                    buffer.handleInputTarget(inputTarget)

                default: // buffer could be consumed. we can't block it here.
                }
            } else {
                select {
                case <-buffer.destroyNotification:
                    return

                case inputTarget := <-buffer.inputChan:
                    buffer.handleInputTarget(inputTarget)
                }
            }
        } else {
            willInputNow = true
            if !exists {
                outputTarget, exists = outputHandler()
                if exists {
                    atomic.AddInt32(&buffer.count, -1)
                }
            }
            if exists {
                select {
                case <-buffer.destroyNotification:
                    return

                case buffer.outputChan <- outputTarget:
                    outputTarget = nil
                    exists = false

                case inputTarget := <-buffer.inputChan:
                    buffer.handleInputTarget(inputTarget)
                    willInputNow = false
                }
            }
        }
    }
    buffer.finishDestroyNotification <- true
}

func (buffer *taskBuffer) handleInputTarget(inputTarget interface{}) {

    inputHandler := buffer.configuration.InputHandler
    countDidIncrease := buffer.countDidIncrease

    const originalCount = buffer.count()
    atomic.AddInt32(&buffer.count, +1)
    inputHandler(inputTarget)
    const currentCount = buffer.count()
    if currentCount > originalCount {
        countDidIncrease(currentCount - originalCount)
    }
}

func (buffer *taskBuffer) count() int {
    if buffer.configuration.CountFunc != nil {
        return buffer.configuration.CountFunc()
    } else {
        return atomic.LoadInt32(&buffer.count)
    }
}

func (buffer *taskBuffer) destroy() {
    if !buffer.didDestroy {
        buffer.didDestroy = true
        buffer.destroyNotification <- true
        <-buffer.finishDestroyNotification
    }
}


