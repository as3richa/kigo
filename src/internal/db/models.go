package db

import (
	"database/sql/driver"
	"time"
)

type JobState string

const (
	JobEnqueued JobState = "enqueued"
	JobRunning  JobState = "running"
	JobFailed   JobState = "failed"
	JobFinished JobState = "finished"
)

func (u *JobState) Scan(value interface{}) error { *u = JobState(value.([]byte)); return nil }
func (u JobState) Value() (driver.Value, error)  { return string(u), nil }

type Job struct {
	ID uint

	QueueName string
	ProcessID *uint
	Process   *Process

	Name string

	ParamBlob []byte
	Params    []interface{} `sql:"-"`

	State JobState `sql:"type:JobState"`

	EnqueuedAt time.Time
	StartAfter time.Time
	StartedAt  *time.Time
	FinishedAt *time.Time
}

func (j *Job) BeforeCreate() error {
	j.State = JobEnqueued
	j.EnqueuedAt = time.Now()
	return nil
}

func (j *Job) BeforeUpdate() error {
	now := time.Now()
	if j.State == JobRunning {
		j.StartedAt = &now
	} else if j.State == JobFailed || j.State == JobFinished {
		j.FinishedAt = &now
	}
	return nil
}

type Queue struct {
	Name string `gorm:"primary_key"`

	Processes []Process
	Jobs      []Job `gorm:"ForeignKey:QueueName"`
}

type Process struct {
	ID uint

	Name          string
	Concurrency   uint
	ActiveThreads uint
	Queues        []Queue

	StartedAt   time.Time
	HeartbeatAt time.Time
	StoppedAt   time.Time
}
