package utils

import "sync/atomic"

type TaskBufferConfiguration struct {
    InputHandler  func(target interface{})
    OutputHandler func() (interface{}, bool)
    BufferLength  int
    CountFunc     func() int
}

func DefaultTaskBufferConfiguration() TaskBufferConfiguration {
    return TaskBufferConfiguration{
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
    afterOutputFunc           func()
    count                     int
    didDestroy                bool
    configuration             TaskBufferConfiguration
}

func createTaskBuffer(configuration TaskBufferConfiguration, outputChan chan interface{}, afterOutputFunc func()) *taskBuffer {
    inputChan := make(chan interface{}, configuration.BufferLength)
    buffer := taskBuffer{
        inputChan: inputChan,
        outputChan: outputChan,
        destroyNotification: make(chan bool, 1),
        finishDestroyNotification: make(chan bool, 1),
        afterOutputFunc: afterOutputFunc,
        count: 0,
        configuration: configuration,
    }
    go buffer.handleLoop()

    return buffer
}

func (buffer *taskBuffer) handleLoop() {

    inputHandler := buffer.configuration.InputHandler
    outputHandler := buffer.configuration.OutputHandler
    afterOutputFunc := buffer.afterOutputFunc
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
                    atomic.AddInt32(&buffer.count, +1)
                    inputHandler(inputTarget)

                default: // buffer could be consumed. we can't block it here.
                }
            } else {
                select {
                case <-buffer.destroyNotification:
                    return

                case inputTarget := <-buffer.inputChan:
                    atomic.AddInt32(&buffer.count, +1)
                    inputHandler(inputTarget)
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
                    afterOutputFunc()

                case inputTarget := <-buffer.inputChan:
                    atomic.AddInt32(&buffer.count, +1)
                    inputHandler(inputTarget)
                    willInputNow = false
                }
            }
        }
    }
    buffer.finishDestroyNotification <- true
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


