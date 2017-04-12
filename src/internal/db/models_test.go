package db

import (
	"testing"
	"time"
)

func TestCannotCreateJobWithSpuriousQueueName(t *testing.T) {
	withDB(func(db DB) {
		job := Job{
			Name:       "fubar",
			QueueName:  "alpha",
			EnqueuedAt: time.Now(),
		}

		expectFailure(t, db.Create(&job).Error)
	})
}

func TestCanCreateQueue(t *testing.T) {
	withDB(func(db DB) {
		queue := Queue{Name: "alpha"}
		expectSuccess(t, db.Create(&queue).Error)
	})
}

func TestCannotCreateTwoQueuesWithEqualName(t *testing.T) {
	withDB(func(db DB) {
		expectSuccess(t, db.Create(&Queue{Name: "alpha"}).Error)
		expectFailure(t, db.Create(&Queue{Name: "alpha"}).Error)
	})
}

func TestCanCreateMultipleQueuesWithDifferentNames(t *testing.T) {
	withDB(func(db DB) {
		expectSuccess(t, db.Create(&Queue{Name: "alpha"}).Error)
		expectSuccess(t, db.Create(&Queue{Name: "beta"}).Error)
	})
}

func TestCanCreateJobWithRealQueueName(t *testing.T) {
	withDB(func(db DB) {
		expectSuccess(t, db.Create(&Queue{Name: "alpha"}).Error)

		job := Job{
			Name:       "fubar",
			State:      JobEnqueued,
			QueueName:  "alpha",
			EnqueuedAt: time.Now(),
		}

		expectSuccess(t, db.Create(&job).Error)
	})
}
