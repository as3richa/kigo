package kigo

import (
	"fmt"
	"reflect"
	"time"
)

const defaultQueueName = "default"

var typeOfError = reflect.TypeOf((*error)(nil)).Elem()
var typeOfTerminator = reflect.TypeOf((*chan struct{})(nil)).Elem()

type task struct {
	callback        reflect.Value
	takesTerminator bool
}

var taskDefinitions = map[string]task{}

func RegisterTask(name string, callback interface{}) {
	cvalue := reflect.ValueOf(callback)
	ctype := reflect.TypeOf(callback)

	if ctype.Kind() != reflect.Func {
		panic("task callback must be function")
	}

	if ctype.NumOut() != 1 || ctype.Out(0) != typeOfError {
		panic("task callback must return a single error")
	}

	takesTerminator := false
	if ctype.NumIn() >= 1 && ctype.In(1) == typeOfTerminator {
		takesTerminator = true
	}

	taskDefinitions[name] = task{
		callback:        cvalue,
		takesTerminator: takesTerminator,
	}
}

func (c Connection) PerformTask(taskName string, parameters []interface{}) (uint, error) {
	return c.PerformTaskOnQueueAt(taskName, parameters, defaultQueueName, time.Now())
}

func (c Connection) PerformTaskAt(taskName string, parameters []interface{}, startAt time.Time) (uint, error) {
	return c.PerformTaskOnQueueAt(taskName, parameters, defaultQueueName, startAt)
}

func (c Connection) PerformTaskOnQueue(taskName string, parameters []interface{}, queueName string) (uint, error) {
	return c.PerformTaskOnQueueAt(taskName, parameters, queueName, time.Now())
}

func (c Connection) PerformTaskOnQueueAt(taskName string, parameters []interface{}, queueName string, startAt time.Time) (uint, error) {
	return c.pushJobTo(queueName, taskName, parameters, startAt)
}

func performTaskAsync(name string, parameters []interface{}, callback func(error)) (chan struct{}, error) {
	task, ok := taskDefinitions[name]
	if !ok {
		return nil, fmt.Errorf("no such task %s", name)
	}

	var terminator chan struct{} = nil
	if task.takesTerminator {
		terminator = make(chan struct{})
	}

	var allValues []reflect.Value
	var parameterValues []reflect.Value

	if task.takesTerminator {
		allValues = make([]reflect.Value, len(parameterValues)+1)
		parameterValues = allValues[1:]
		allValues[0] = reflect.ValueOf(terminator)
	} else {
		allValues = make([]reflect.Value, len(parameterValues))
		parameterValues = allValues
	}

	parameterValues = make([]reflect.Value, len(parameters))
	for i := 0; i < len(parameters); i++ {
		parameterValues[i] = reflect.ValueOf(parameters[i])
	}

	go func() {
		var err error

		func() {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic: %v", r)
				}
			}()

			valueResult := task.callback.Call(allValues)[0]
			if !valueResult.IsNil() {
				err = valueResult.Interface().(error)
			}
		}()

		callback(err)
	}()

	return terminator, nil
}
