package db

import (
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

var createJobstateTypeQuery = `CREATE TYPE jobstate AS ENUM ('enqueued', 'running', 'failed', 'done')`

type DB struct {
	*gorm.DB
}

var allModels = []interface{}{
	&Job{},
	&Queue{},
	&Process{},
}

func Connect(url string) (DB, error) {
	var g *gorm.DB
	var err error

	if g, err = gorm.Open("postgres", url); err != nil {
		return DB{}, err
	}
	g.DB().SetMaxOpenConns(80)
	g.DB().SetMaxIdleConns(80)

	db := DB{g}

	if err := db.initializeBackend(); err != nil {
		db.Close()
		return DB{}, err
	}

	return db, nil
}

func (db DB) initializeBackend() error {
	_ = db.Exec(createJobstateTypeQuery).Error

	if err := db.AutoMigrate(allModels...).Error; err != nil {
		return err
	}

	if err := db.Model(&Job{}).AddForeignKey("queue_name", "queues(name)", "RESTRICT", "RESTRICT").Error; err != nil {
		return err
	}

	if err := db.Model(&Job{}).AddIndex("jobs_queue_name_and_state_and_start_after_and_enqueued_at", "queue_name", "state", "start_after", "enqueued_at").Error; err != nil {
		return err
	}

	return nil
}

func (db DB) DropAll() error {
	if err := db.DropTableIfExists(allModels...).Error; err != nil {
		return err
	}
	return db.initializeBackend()
}
