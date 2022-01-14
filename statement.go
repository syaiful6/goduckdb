package duckdb

/*
#include <duckdb.h>
*/
import "C"
import (
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"time"
	"unsafe"
)

var (
	errCouldNotBind = errors.New("could not bind parameter")
)

type stmt struct {
	mu     sync.Mutex
	c      *conn
	stmt   *C.duckdb_prepared_statement
	closed bool
	rows   bool
}

func (s *stmt) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.rows {
		return errors.New("database/sql/driver: misuse of duckdb driver: Close with active Rows")
	}

	if s.closed {
		return errors.New("database/sql/driver: misuse of duckdb driver: double Close of Stmt")
	}

	s.closed = true
	C.duckdb_destroy_prepare(s.stmt)
	return nil
}

func (s *stmt) NumInput() int {
	var pc C.idx_t
	pc = C.duckdb_nparams(*s.stmt)
	return int(pc)
}

func (s *stmt) start(args []driver.Value) error {
	if s.NumInput() != len(args) {
		return fmt.Errorf("incorrect argument count for command: have %d want %d", len(args), s.NumInput())
	}

	for i, v := range args {
		switch v := v.(type) {
		case int8:
			if rv := C.duckdb_bind_int8(*s.stmt, C.idx_t(i+1), C.int8_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
			continue
		case int16:
			if rv := C.duckdb_bind_int16(*s.stmt, C.idx_t(i+1), C.int16_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
			continue
		case int32:
			if rv := C.duckdb_bind_int32(*s.stmt, C.idx_t(i+1), C.int32_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
			continue
		case int64:
			if rv := C.duckdb_bind_int64(*s.stmt, C.idx_t(i+1), C.int64_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
			continue
		case float64:
			if rv := C.duckdb_bind_double(*s.stmt, C.idx_t(i+1), C.double(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
			continue
		case bool:
			if rv := C.duckdb_bind_boolean(*s.stmt, C.idx_t(i+1), C.bool(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
			continue
		case time.Time:
			duckdbTimestamp := C.duckdb_timestamp{micros: C.int64_t(v.UnixMicro())}
			if rv := C.duckdb_bind_timestamp(*s.stmt, C.idx_t(i+1), duckdbTimestamp); rv == C.DuckDBError {
				return errCouldNotBind
			}
			continue
		case string:
			str := C.CString(v)
			if rv := C.duckdb_bind_varchar(*s.stmt, C.idx_t(i+1), str); rv == C.DuckDBError {
				C.free(unsafe.Pointer(str))
				return errCouldNotBind
			}
			C.free(unsafe.Pointer(str))
			continue
		default:
			return driver.ErrSkip
		}
	}

	return nil
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	if s.closed {
		panic("database/sql/driver: misuse of duckdb driver: Exec after Close")
	}
	if s.rows {
		panic("database/sql/driver: misuse of duckdb driver: Exec with active Rows")
	}

	err := s.start(args)
	if err != nil {
		return nil, err
	}

	var res C.duckdb_result
	if err := C.duckdb_execute_prepared(*s.stmt, &res); err == C.DuckDBError {
		return nil, errors.New(C.GoString(C.duckdb_result_error(&res)))
	}
	if err != nil {
		return nil, err
	}
	defer C.duckdb_destroy_result(&res)

	ra := int64(C.duckdb_value_int64(&res, 0, 0))

	return duckdbResult{ra: ra}, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	if s.closed {
		panic("database/sql/driver: misuse of duckdb driver: Query after Close")
	}
	if s.rows {
		panic("database/sql/driver: misuse of duckdb driver: Query with active Rows")
	}

	err := s.start(args)
	if err != nil {
		return nil, err
	}

	s.rows = true
	var res C.duckdb_result

	if err := C.duckdb_execute_prepared(*s.stmt, &res); err == C.DuckDBError {
		return nil, errors.New(C.GoString(C.duckdb_result_error(&res)))
	}

	return &duckdbRows{r: &res, s: s}, nil
}
