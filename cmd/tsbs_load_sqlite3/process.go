package main

import (
	"fmt"

	"database/sql"

	"github.com/timescale/tsbs/pkg/targets"
)

// Allows for testing
var printFn = fmt.Printf

type processor struct {
	db  *sql.DB
	err error
}

func (p *processor) Init(numWorker int, _, _ bool) {
	// Get handle for sql
	dbName := "SQLite3DB"
	p.db, p.err = sql.Open("sqlite3", fmt.Sprintf("./%s.db", dbName))
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)
	stmt := batch.buf.s

	// Execute each batch, which is already in the format of an SQL command
	if doLoad {
		var err error
		_, err = p.db.Exec(stmt)
		if err != nil {
			fatal("Error writing %s\n", err.Error())
		}
	}

	metricCnt := batch.metrics
	rowCnt := batch.rows

	// Return the batch buffer to the pool
	// batch.buf.Reset()
	batch.buf.s = ""
	bufPool.Put(batch.buf)
	return metricCnt, uint64(rowCnt)
}

func (p *processor) Close(_ bool) {
	defer p.db.Close()
}
