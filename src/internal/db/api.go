package db

import (
	"database/sql"
	"fmt"
	"time"

	"gopkg.in/vmihailenco/msgpack.v2"
)

func (db DB) EnsureQueuesExist(queueNames []string) error {
	for _, queueName := range queueNames {
		if err := db.FirstOrCreate(&Queue{}, Queue{Name: queueName}).Error; err != nil {
			return fmt.Errorf("couldn't create queue %s: %v", queueName, err)
		}
	}
	return nil
}

func (db DB) PopJobFrom(queueNames []string, processID uint) (*Job, error) {
	var id int
	var job Job

	tx := db.Begin()

	err := tx.Raw(`
    SELECT id FROM jobs
    WHERE queue_name IN (?) AND state = ? AND start_at <= ?
    ORDER BY enqueued_at ASC LIMIT 1 FOR UPDATE`, queueNames, JobEnqueued, time.Now()).Row().Scan(&id)

	if err != nil && err != sql.ErrNoRows {
		tx.Rollback()
		return nil, err
	} else if err == sql.ErrNoRows {
		tx.Rollback()
		return nil, nil
	}

	err = tx.Model(&job).Where("id = ?", id).Update(Job{State: JobRunning, ProcessID: &processID}).Error
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	if err = tx.Commit().Error; err != nil {
		tx.Rollback()
		return nil, err
	}

	if err = db.First(&job, id).Error; err != nil {
		return nil, err
	}

	if err = msgpack.Unmarshal(job.ParamBlob, &job.Params); err != nil {
		// TODO(as3richa) - record this failure
		return nil, err
	}

	return &Job, nil
}

func (db DB) PushJob(job *Job) error {
	return db.Create(job).Error
}

func (db DB) FinishJob(job *Job) error {
	return db.Model(job).Where("id = ?", job.ID).Update("state", JobFinished).Error
}

func (db DB) FailJob(job *Job) error {
	return db.Model(job).Where("id = ?", job.ID).Update("state", JobFailed).Error
}

func (db DB) BuildJob(
	queueName string,
	jobName string,
	params []interface{},
	startAfter time.Time,
	retryCount uint,
) (*Job, error) {
	serializedParams, err := msgpack.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("couldn't marshal parameters: %v", err)
	}

	return &Job{
		QueueName:  queueName,
		Name:       jobName,
		ParamBlob:  serializedParams,
		StartAfter: startAfter,
	}, nil
}
