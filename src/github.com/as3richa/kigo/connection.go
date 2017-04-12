package kigo

import (
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

type Connection struct {
	db *gorm.DB
}

func Connect(url string) (Connection, error) {
	g, err := gorm.Open("postgres", url)
	if err != nil {
		return Connection{}, err
	}
	g.LogMode(false)
	return Connection{db: g}, nil
}

func (c Connection) SetConnMaxLifetime(d time.Duration) {
	c.db.DB().SetConnMaxLifetime(d)
}

func (c Connection) SetMaxIdleConns(n int) {
	c.db.DB().SetMaxIdleConns(n)
}

func (c Connection) SetMaxOpenConns(n int) {
	c.db.DB().SetMaxOpenConns(n)
}
