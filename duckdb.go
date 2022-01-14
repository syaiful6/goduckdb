package duckdb

/*
#cgo LDFLAGS: -lduckdb
#include <duckdb.h>
*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/url"
	"strings"
	"unsafe"
)

var (
	errError = errors.New("could not open database")
)

func init() {
	sql.Register("duckdb", duckdb{})
}

type duckdb struct{}

func (d duckdb) Open(dsn string) (driver.Conn, error) {
	var (
		config C.duckdb_config
		db     C.duckdb_database
		con    C.duckdb_connection
	)
	if err := C.duckdb_create_config(&config); err == C.DuckDBError {
		return nil, errError
	}

	defer C.duckdb_destroy_config(&config)

	pos := strings.IndexRune(dsn, '?')
	if pos >= 1 {
		params, err := url.ParseQuery(dsn[pos+1:])
		if err != nil {
			return nil, err
		}
		mode := ""
		threads := ""
		maxMemory := ""
		defaultOrder := ""
		if val := params.Get("access_mode"); val != "" {
			mode = val
		}
		if val := params.Get("threads"); val != "" {
			threads = val
		}
		if val := params.Get("max_memory"); val != "" {
			maxMemory = val
		}
		if val := params.Get("default_order"); val != "" {
			defaultOrder = val
		}
		if mode != "" {
			d.setConfig(config, "access_mode", mode)
		}
		if threads != "" {
			d.setConfig(config, "threads", threads)
		}
		if maxMemory != "" {
			d.setConfig(config, "max_memory", maxMemory)
		}
		if defaultOrder != "" {
			d.setConfig(config, "default_order", defaultOrder)
		}

		dsn = dsn[:pos]
	}

	cname := C.CString(dsn)
	defer C.free(unsafe.Pointer(cname))

	if err := C.duckdb_open_ext(cname, &db, config, nil); err == C.DuckDBError {
		return nil, errError
	}
	if err := C.duckdb_connect(db, &con); err == C.DuckDBError {
		return nil, errError
	}

	return &conn{db: &db, con: &con}, nil
}

func (d duckdb) setConfig(config C.duckdb_config, flag, value string) {
	modeFlag := C.CString(flag)
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(modeFlag))
	defer C.free(unsafe.Pointer(cValue))

	C.duckdb_set_config(config, modeFlag, cValue)
}
