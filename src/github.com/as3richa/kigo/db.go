package kigo

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"time"
)

func (c Connection) createWorker(workerName string, queueNames []string, concurrency uint) (uint, error) {
	queueRecords := make([]queueModel, len(queueNames))
	for i := 0; i < len(queueNames); i++ {
		queueRecords[i].Name = queueNames[i]
	}

	now := time.Now()

	workerRecord := workerModel{
		Name:        workerName,
		Queues:      queueRecords,
		Concurrency: concurrency,
		StartedAt:   now,
		HeartbeatAt: now,
	}

	if err := c.db.Create(&workerRecord).Error; err != nil {
		return 0, err
	}

	return workerRecord.ID, nil
}

func (c Connection) beatWorker(id uint) error {
	return c.db.Model(&workerModel{}).Where("id = ?", id).Update("heartbeat_at", time.Now()).Error
}

func (c Connection) terminateWorker(id uint) error {
	message := "worker was terminated"

	tx := c.db.Begin()

	err := tx.Model(&jobModel{}).Where("worker_id = ?", id).Update(jobModel{
		State:    jobFailed,
		WorkerID: nil,
		Error:    &message,
	}).Error

	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Model(&workerModel{}).Delete(workerModel{ID: id}).Error
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit().Error
	if err != nil {
		tx.Rollback()
		return err
	}

	return nil
}

func (c Connection) pushJobTo(queueName string, taskName string, parameters []interface{}, startAt time.Time) (uint, error) {
	paramBuffer := bytes.Buffer{}
	encoder := gob.NewEncoder(&paramBuffer)
	encoder.Encode(parameters)

	jobRecord := jobModel{
		QueueName:  queueName,
		Queue:      &queueModel { Name: queueName },
		TaskName:   taskName,
		ParamBlob:  paramBuffer.Bytes(),
		State:      jobEnqueued,
		EnqueuedAt: time.Now(),
		StartAt:    startAt,
	}

	if err := c.db.Create(&jobRecord).Error; err != nil {
		return 0, err
	}

	return jobRecord.ID, nil
}

func (c Connection) popJobFrom(workerID uint, queueNames []string) (*Job, error) {
	now := time.Now()

	var id int
	var jobRecord jobModel

	tx := c.db.Begin()

	err := tx.Raw(`
    SELECT id FROM jobs
    WHERE queue_name IN (?) AND state = ? AND start_at <= ?
    ORDER BY enqueued_at ASC LIMIT 1 FOR UPDATE`, queueNames, jobEnqueued, now).Row().Scan(&id)

	if err != nil && err != sql.ErrNoRows {
		tx.Rollback()
		return nil, err
	} else if err == sql.ErrNoRows {
		tx.Rollback()
		return nil, nil
	}

	err = tx.Model(&jobRecord).Where("id = ?", id).Update(jobModel{
		State:     jobRunning,
		WorkerID:  &workerID,
		StartedAt: &now}).Error

	if err != nil {
		tx.Rollback()
		return nil, err
	}

	if err = tx.Commit().Error; err != nil {
		tx.Rollback()
		return nil, err
	}

	if err = c.db.First(&jobRecord, id).Error; err != nil {
		return nil, err
	}

	paramBuffer := bytes.NewBuffer(jobRecord.ParamBlob)
	decoder := gob.NewDecoder(paramBuffer)
	var parameters []interface{}

	if err = decoder.Decode(&parameters); err != nil {
		return nil, fmt.Errorf("deserialization failure: %v", err)
	}

	return &Job{
		ID:         jobRecord.ID,
		TaskName:   jobRecord.TaskName,
		Parameters: parameters,
	}, nil
}

func (c Connection) finishJob(id uint) error {
	return c.db.Model(&jobModel{}).Where("id = ?", id).Update(map[string]interface{}{
		"state":     jobFinished,
		"worker_id": nil,
		"error":     nil,
	}).Error
}

func (c Connection) failJob(id uint, err error) error {
	return c.db.Model(&jobModel{}).Where("id = ?", id).Update(map[string]interface{}{
		"state":     jobFailed,
		"worker_id": nil,
		"error":     err.Error(),
	}).Error
}
