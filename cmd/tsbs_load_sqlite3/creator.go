package main

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	"github.com/timescale/tsbs/pkg/targets"
)

type dbCreator struct {
	dbName string
	ds     targets.DataSource
}

func (d *dbCreator) Init() {
	// This creates our tables
	d.ds.Headers()

	d.dbName = "SQLite3DB"
	db, err := sql.Open("sqlite3", fmt.Sprintf("./%s.db", d.dbName))
	if err != nil {
		fatal("Error: %v", err)
	}
	defer db.Close()
}

func (d *dbCreator) DBExists(dbName string) bool {
	return false
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	return nil
}
