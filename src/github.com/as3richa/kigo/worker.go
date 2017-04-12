package kigo

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

const fallbackHostname = "anonymous"

const pollingInterval = 2 * time.Second
const heartbeatInterval = 10 * time.Second

var signals = []os.Signal{os.Interrupt, os.Kill, syscall.SIGTERM}
var signalError = errors.New("received termination signal")
var terminatorError = errors.New("external terminator was triggered")

type Job struct {
	ID         uint
	TaskName   string
	Parameters []interface{}
}

type WorkerOptions struct {
	CustomName string

	ApiAddr string

	Logger       logrus.FieldLogger
	CatchSignals bool

	TermGracePeriod time.Duration

	Terminator chan struct{}

	BootHook  func(string, []string, uint, *WorkerOptions)
	ErrorHook func(error)
	TermHook  func(error)

	JobRunningHook func(*Job)
	JobFailedHook  func(*Job, error)
	JobDoneHook    func(*Job)
}

type threadInfo struct {
	job *Job

	startedAt time.Time

	terminator chan struct{}
	terminated bool
}

type threadResult struct {
	id  uint
	err error
}

type worker struct {
	log logrus.FieldLogger

	id uint

	globalTerminator     chan error
	subroutineTerminator chan struct{}

	sharedState struct {
		sync.Mutex

		queueNames []string

		concurrency uint
		counter     uint

		activeThreads map[uint]threadInfo
	}
}

var DefaultWorkerOptions = &WorkerOptions{
	ApiAddr:         "0.0.0.0:32600",
	CatchSignals:    true,
	TermGracePeriod: 10 * time.Second,
}

func (c Connection) RunWorker(queueNames []string, concurrency uint) error {
	return c.RunWorkerWithOptions(queueNames, concurrency, DefaultWorkerOptions)
}

func (c Connection) RunWorkerWithOptions(queueNames []string, concurrency uint, options *WorkerOptions) error {
	if queueNames == nil {
		queueNames = []string{defaultQueueName}
	}

	var workerName string
	if options.CustomName != "" {
		workerName = options.CustomName
	} else {
		workerName = defaultWorkerName()
	}

	log := options.Logger
	if log == nil {
		log = DefaultWorkerLogger
	}

	worker := &worker{
		log:                  log.WithFields(logrus.Fields{"workerName": workerName}),
		globalTerminator:     make(chan error, 3),
		subroutineTerminator: make(chan struct{}),
	}
	worker.sharedState.queueNames = queueNames
	worker.sharedState.concurrency = concurrency
	worker.sharedState.activeThreads = map[uint]threadInfo{}

	var err error

	if worker.id, err = c.createWorker(workerName, queueNames, concurrency); err != nil {
		if options.ErrorHook != nil {
			options.ErrorHook(err)
		}
		return err
	}

	log.Info("working booting up")
	if options.BootHook != nil {
		options.BootHook(workerName, queueNames, concurrency, options)
	}

	if options.CatchSignals {
		go worker.signalCatcher()
	}

	if options.Terminator != nil {
		go worker.externalTerminatorHandler(options.Terminator)
	}

	if options.ApiAddr != "" {
		go worker.apiHttpServer(options.ApiAddr)
	}

	go worker.heartbeat(c)

	go worker.scheduler(c)

	err = <-worker.globalTerminator
	close(worker.subroutineTerminator)

	if err == signalError || err == terminatorError {
		log.WithFields(logrus.Fields{"reason": err}).Info("worker terminating")
		err = nil
	} else {
		log.WithFields(logrus.Fields{"error": err}).Error("worker terminating")
		if options.ErrorHook != nil {
			options.ErrorHook(err)
		}
	}

	_ = c.terminateWorker(worker.id)

	if options.TermHook != nil {
		options.TermHook(err)
	}

	log.Info("worker terminated")
	return err
}

func (w *worker) signalCatcher() {
	c := make(chan os.Signal, 1)

	signal.Notify(c, signals...)
	defer signal.Stop(c)

	w.log.WithFields(logrus.Fields{"signals": signals}).Info("listening for signals")

	select {
	case signal := <-c:
		w.log.WithFields(logrus.Fields{"signal": signal}).Info("received signal")
		w.globalTerminator <- signalError
	case <-w.subroutineTerminator:
	}
}

func (w *worker) externalTerminatorHandler(externalTerminator <-chan struct{}) {
	w.log.Info("listening on external terminator")
	select {
	case <-externalTerminator:
		w.log.Info("external terminator triggered")
		w.globalTerminator <- terminatorError
	case <-w.subroutineTerminator:
	}
}

func (w *worker) scheduler(c Connection) {
	ticker := time.NewTicker(pollingInterval)
	defer ticker.Stop()

	results := make(chan threadResult)

	w.sharedState.Lock()
	w.log.WithFields(logrus.Fields{"queues": w.sharedState.queueNames}).Info("starting scheduler")
	w.sharedState.Unlock()

	for {
		w.sharedState.Lock()

		if w.sharedState.concurrency > uint(len(w.sharedState.activeThreads)) {
			job, err := c.popJobFrom(w.id, w.sharedState.queueNames)
			if err != nil {
				w.log.WithFields(logrus.Fields{"error": err}).Error("couldn't pop job")
			} else if job != nil {
				w.log.WithFields(logrus.Fields{"id": job.ID, "taskName": job.TaskName}).Info("popped job")
				if err := w.spawnThread(job, results); err != nil {
					w.log.WithFields(logrus.Fields{"id": job.ID, "taskName": job.TaskName, "error": err}).Info("couldn't start job")
					c.failJob(job.ID, fmt.Errorf("couldn't start job %d: %v", job.ID, err))
				}
			}
		}

		for resultsEmpty := false; !resultsEmpty; {
			select {
			case result := <-results:
				threadID := result.id
				returnValue := result.err
				job := w.sharedState.activeThreads[threadID].job
				delete(w.sharedState.activeThreads, threadID)

				fmt.Printf("%v %v %v\n", threadID, returnValue, job)

				if returnValue != nil {
					w.log.WithFields(logrus.Fields{"id": job.ID, "taskName": job.TaskName, "error": returnValue}).Error("job failed")
					c.failJob(job.ID, fmt.Errorf("job %d failed: %v", job.ID, returnValue))
				} else {
					w.log.WithFields(logrus.Fields{"id": job.ID, "taskName": job.TaskName}).Info("job finished peacefully")
					c.finishJob(job.ID)
				}
			default:
				resultsEmpty = true
			}
		}

		w.sharedState.Unlock()

		select {
		case <-w.subroutineTerminator:
			w.log.Info("terminating scheduler")
			return
		case <-ticker.C:
		}
	}
}

func (w *worker) spawnThread(job *Job, results chan<- threadResult) error {
	now := time.Now()

	threadID := w.sharedState.counter
	w.sharedState.counter++

	fmt.Printf("%v\n", threadID)

	callback := func(err error) {
		results <- threadResult{
			threadID,
			err,
		}
	}

	terminator, err := performTaskAsync(job.TaskName, job.Parameters, callback)
	if err != nil {
		return err
	}

	w.sharedState.activeThreads[threadID] = threadInfo{
		job:        job,
		startedAt:  now,
		terminator: terminator,
	}

	return nil
}

func (w *worker) heartbeat(c Connection) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		if err := c.beatWorker(w.id); err != nil {
			w.log.WithFields(logrus.Fields{"error": err}).Error("worker heartbeat failed")
		} else {
			w.log.Info("heartbeat")
		}

		select {
		case <-w.subroutineTerminator:
			return
		case <-ticker.C:
		}
	}
}

func defaultWorkerName() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = fallbackHostname
	}
	return fmt.Sprintf("%s/%d", hostname, os.Getpid())
}
