package kigo

import "time"

type jobState uint

const (
	jobEnqueued jobState = iota
	jobRunning  jobState = iota
	jobFailed   jobState = iota
	jobFinished jobState = iota
)

type workerModel struct {
	ID uint

	Name        string
	Queues      []queueModel `gorm:"many2many:worker_queues"`
	Concurrency uint

	StartedAt   time.Time
	HeartbeatAt time.Time
	StoppedAt   *time.Time
}

type queueModel struct {
	Name string `gorm:"primary_key"`

	Workers []workerModel `gorm:"many2many:worker_queues"`
	Jobs    []jobModel    `gorm:"ForeignKey:QueueName"`
}

type jobModel struct {
	ID uint

	QueueName string
	Queue     *queueModel

	WorkerID       *uint
	Worker         *workerModel
	WorkerThreadID *uint

	TaskName string

	ParamBlob []byte

	State jobState

	EnqueuedAt time.Time
	StartAt    time.Time
	StartedAt  *time.Time
	FinishedAt *time.Time

	Error *string
}

func (workerModel) TableName() string { return "workers" }
func (queueModel) TableName() string  { return "queues" }
func (jobModel) TableName() string    { return "jobs" }

var jobIndexes = [][]string{
	[]string{"jobs_queue_name_and_state_and_start_at_and_enqueued_at", "queue_name", "state", "start_at", "enqueued_at"},
	[]string{"jobs_started_at", "started_at"},
}

func (c Connection) Migrate() error {
	if err := c.db.AutoMigrate(&workerModel{}, &queueModel{}, &jobModel{}).Error; err != nil {
		return err
	}

	if err := c.db.Model(&jobModel{}).AddForeignKey("queue_name", "queues(name)", "RESTRICT", "RESTRICT").Error; err != nil {
		return err
	}

	for _, index := range jobIndexes {
		if err := c.db.Model(&jobModel{}).AddIndex(index[0], index[1:]...).Error; err != nil {
			return err
		}
	}

	return nil
}

func (c Connection) DropAll() error {
	return c.db.DropTableIfExists(&jobModel{}, &workerModel{}, &queueModel{}).Error
}
