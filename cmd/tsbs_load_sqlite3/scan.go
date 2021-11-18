package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"

	_ "github.com/mattn/go-sqlite3"
)

const errNotTwoTuplesFmt = "parse error: line does not have 2 tuples, has %d"

type fileDataSource struct {
	scanner *bufio.Scanner
}

func (d *fileDataSource) NextItem() data.LoadedPoint {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil {
		// Nothing scanned and no error = EOF
		return data.LoadedPoint{}
	} else if !ok {
		fatal("Scan error: %v", d.scanner.Err())
		return data.LoadedPoint{}
	}

	// Return the command
	return data.NewLoadedPoint(d.scanner.Text())
}

func (d *fileDataSource) Headers() *common.GeneratedDataHeaders {
	// The headers are the lines that start with 'CREATE'
	// We will read the lines until a line that starts with
	// 'CREATE' appears. These lines will not be executed. All others
	// will be ignored.

	// Here is the name of the database
	dbName := "SQLite3DB"

	// Let's open up the database
	db, err := sql.Open("sqlite3", fmt.Sprintf("./%s.db", dbName))
	if err != nil {
		fatal("Error: %v", err)
	}
	defer db.Close()

	for {
		ok := d.scanner.Scan()
		if !ok && d.scanner.Err() == nil {
			// Nothing scanned and no error = EOF
			return nil
		} else if !ok {
			fatal("Scan error: %v", d.scanner.Err())
			return nil
		}

		if strings.HasPrefix(d.scanner.Text(), "CREATE") {
			// Remove the '^' character
			line := strings.Split(d.scanner.Text(), "^")
			// Make sure that there are 2 peices
			if len(line) != 2 {
				panic("There are not 2 pieces in the line.")
			}

			stmt := line[0] // Contains the actual statement to execute
			db.Exec(stmt)
		}
	}
}

type batch struct {
	buf     *bytes.Buffer
	rows    uint
	metrics uint64
}

func (b *batch) Len() uint {
	return b.rows
}

func (b *batch) Append(item data.LoadedPoint) {
	that := item.Data.(string)

	// Extract the number of metrics, which is a number followed by '^'
	args := strings.Split(that, "^")
	if len(args) != 2 {
		fatal(errNotTwoTuplesFmt, len(args))
		return
	}

	// Don't append the creation lines
	if strings.HasPrefix(args[0], "CREATE") {
		return
	}

	// Increment the number of rows by one
	b.rows++

	metrics, _ := strconv.Atoi(args[1])

	b.metrics += uint64(metrics)

	b.buf.Write([]byte(args[0]))
	b.buf.Write([]byte("\n"))
}

type factory struct{}

func (f *factory) New() targets.Batch {
	return &batch{buf: bufPool.Get().(*bytes.Buffer)}
}
