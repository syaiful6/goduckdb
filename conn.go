package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

var (
	errInvalidType = errors.New("invalid data type")
)

type conn struct {
	mu  sync.Mutex
	db  *C.duckdb_database
	con *C.duckdb_connection
}

type duckdbResult struct {
	ra int64
}

type duckdbRows struct {
	r      *C.duckdb_result
	s      *stmt
	cursor int64
}

func (r duckdbResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r duckdbResult) RowsAffected() (int64, error) {
	return r.ra, nil
}

func (r *duckdbRows) Columns() []string {
	if r.r == nil {
		panic("database/sql/driver: misuse of duckdb driver: Columns of closed rows")
	}

	columnCount := C.duckdb_column_count(r.r)

	cols := make([]string, int64(columnCount))
	for i := 0; i < int(columnCount); i++ {
		cols[i] = C.GoString(C.duckdb_column_name(r.r, C.idx_t(i)))
	}

	return cols
}

func (r *duckdbRows) Next(dst []driver.Value) error {
	if r.r == nil {
		panic("database/sql/driver: misuse of duckdb driver: Next of closed rows")
	}

	if r.cursor >= int64(C.duckdb_row_count(r.r)) {
		return io.EOF
	}

	columnCount := C.duckdb_column_count(r.r)

	for i := 0; i < int(columnCount); i++ {
		colType := C.duckdb_column_type(r.r, C.idx_t(i))
		colData := C.duckdb_column_data(r.r, C.idx_t(i))
		switch colType {
		case C.DUCKDB_TYPE_INVALID:
			return errInvalidType
		case C.DUCKDB_TYPE_BOOLEAN:
			dst[i] = (*[1 << 31]bool)(unsafe.Pointer(colData))[r.cursor]
		case C.DUCKDB_TYPE_TINYINT:
			dst[i] = (*[1 << 31]int8)(unsafe.Pointer(colData))[r.cursor]
		case C.DUCKDB_TYPE_SMALLINT:
			dst[i] = (*[1 << 31]int16)(unsafe.Pointer(colData))[r.cursor]
		case C.DUCKDB_TYPE_INTEGER:
			dst[i] = (*[1 << 31]int32)(unsafe.Pointer(colData))[r.cursor]
		case C.DUCKDB_TYPE_BIGINT:
			dst[i] = (*[1 << 31]int64)(unsafe.Pointer(colData))[r.cursor]
		case C.DUCKDB_TYPE_FLOAT:
			dst[i] = (*[1 << 31]float32)(unsafe.Pointer(colData))[r.cursor]
		case C.DUCKDB_TYPE_DOUBLE:
			dst[i] = (*[1 << 31]float64)(unsafe.Pointer(colData))[r.cursor]
		case C.DUCKDB_TYPE_DATE:
			date := (*[1 << 31]C.duckdb_date)(unsafe.Pointer(colData))[r.cursor]
			val := C.duckdb_from_date(date)
			dst[i] = time.Date(
				int(val.year),
				time.Month(val.month),
				int(val.day),
				0, 0, 0, 0,
				time.UTC,
			)
		case C.DUCKDB_TYPE_VARCHAR:
			dst[i] = C.GoString((*[1 << 31]*C.char)(unsafe.Pointer(colData))[r.cursor])
		case C.DUCKDB_TYPE_TIMESTAMP:
			ts := (*[1 << 31]C.duckdb_timestamp)(unsafe.Pointer(colData))[r.cursor]
			val := C.duckdb_from_timestamp(ts)
			dst[i] = time.Date(
				int(val.date.year),
				time.Month(val.date.month),
				int(val.date.day),
				int(val.time.hour),
				int(val.time.min),
				int(val.time.sec),
				int(val.time.micros),
				time.UTC,
			)
		}
	}

	r.cursor++

	return nil
}

// implements driver.RowsColumnTypeScanType
func (r *duckdbRows) ColumnTypeScanType(index int) reflect.Type {
	colType := C.duckdb_column_type(r.r, C.idx_t(index))
	switch colType {
	case C.DUCKDB_TYPE_BOOLEAN:
		return reflect.TypeOf(true)
	case C.DUCKDB_TYPE_TINYINT:
		return reflect.TypeOf(int8(0))
	case C.DUCKDB_TYPE_SMALLINT:
		return reflect.TypeOf(int16(0))
	case C.DUCKDB_TYPE_INTEGER:
		return reflect.TypeOf(int(0))
	case C.DUCKDB_TYPE_BIGINT:
		return reflect.TypeOf(int64(0))
	case C.DUCKDB_TYPE_FLOAT:
		return reflect.TypeOf(float32(0))
	case C.DUCKDB_TYPE_DOUBLE:
		return reflect.TypeOf(float64(0))
	case C.DUCKDB_TYPE_DATE, C.DUCKDB_TYPE_TIMESTAMP:
		return reflect.TypeOf(time.Time{})
	case C.DUCKDB_TYPE_VARCHAR:
		return reflect.TypeOf("")
	}
	return nil
}

// implements driver.RowsColumnTypeScanType
func (r *duckdbRows) ColumnTypeDatabaseTypeName(index int) string {
	colType := C.duckdb_column_type(r.r, C.idx_t(index))
	switch colType {
	case C.DUCKDB_TYPE_BOOLEAN:
		return "BOOLEAN"
	case C.DUCKDB_TYPE_TINYINT:
		return "TINYINT"
	case C.DUCKDB_TYPE_SMALLINT:
		return "SMALLINT"
	case C.DUCKDB_TYPE_INTEGER:
		return "INT"
	case C.DUCKDB_TYPE_BIGINT:
		return "BIGINT"
	case C.DUCKDB_TYPE_FLOAT:
		return "FLOAT"
	case C.DUCKDB_TYPE_DOUBLE:
		return "DOUBLE"
	case C.DUCKDB_TYPE_DATE:
		return "DATE"
	case C.DUCKDB_TYPE_VARCHAR:
		return "VARCHAR"
	case C.DUCKDB_TYPE_TIMESTAMP:
		return "TIMESTAMP"
	}
	return ""
}

func (r *duckdbRows) Close() error {
	if r.r == nil {
		panic("database/sql/driver: misuse of duckdb driver: Close of already closed rows")
	}

	C.duckdb_destroy_result(r.r)

	r.r = nil
	if r.s != nil {
		r.s.rows = false
		r.s = nil
	}

	return nil
}

func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	res, err := c.exec(query)
	if err != nil {
		return nil, err
	}
	defer C.duckdb_destroy_result(res)

	ra := int64(C.duckdb_value_int64(res, 0, 0))

	return duckdbResult{ra: ra}, nil
}

func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.query(query, args)
}

func (c *conn) Prepare(cmd string) (driver.Stmt, error) {
	cmdstr := C.CString(cmd)
	defer C.free(unsafe.Pointer(cmdstr))

	var s C.duckdb_prepared_statement
	C.duckdb_prepare(*c.con, cmdstr, &s)

	return &stmt{c: c, stmt: &s}, nil
}

func (c *conn) Begin() (driver.Tx, error) {
	if _, err := c.exec("BEGIN TRANSACTION"); err != nil {
		return nil, err
	}

	return &duckdbTx{c}, nil
}

func (c *conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db == nil {
		return errors.New("Duckdb database already closed")
	}

	C.duckdb_disconnect(c.con)
	C.duckdb_close(c.db)
	c.db = nil

	return nil
}

func (c *conn) query(query string, args []driver.Value) (driver.Rows, error) {
	queryStr, err := c.interpolateParams(query, args)
	if err != nil {
		return nil, err
	}

	res, err := c.exec(queryStr)
	if err != nil {
		return nil, err
	}

	return &duckdbRows{r: res}, nil
}

func (c *conn) exec(cmd string) (*C.duckdb_result, error) {
	cmdstr := C.CString(cmd)
	defer C.free(unsafe.Pointer(cmdstr))

	var res C.duckdb_result

	if err := C.duckdb_query(*c.con, cmdstr, &res); err == C.DuckDBError {
		return nil, errors.New(C.GoString(C.duckdb_result_error(&res)))
	}

	return &res, nil
}

// interpolateParams is taken from
// https://github.com/go-sql-driver/mysql
func (c *conn) interpolateParams(query string, args []driver.Value) (string, error) {
	if strings.Count(query, "?") != len(args) {
		return "", driver.ErrSkip
	}

	buf := []byte{}
	argPos := 0

	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?')
		if q == -1 {
			buf = append(buf, query[i:]...)
			break
		}
		buf = append(buf, query[i:i+q]...)
		i += q

		arg := args[argPos]
		argPos++

		if arg == nil {
			buf = append(buf, "NULL"...)
			continue
		}

		switch v := arg.(type) {
		case int8:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int16:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int32:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int64:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case bool:
			if v {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case time.Time:
			buf = strconv.AppendInt(buf, v.Unix(), 10)
		case string:
			buf = append(buf, '\'')
			buf = append(buf, escapeValue(v)...)
			buf = append(buf, '\'')
		default:
			return "", driver.ErrSkip
		}
	}

	if argPos != len(args) {
		return "", driver.ErrSkip
	}

	return string(buf), nil
}

func escapeValue(v string) []byte {
	buf := bytes.NewBuffer(nil)

	for i := 0; i < len(v); i++ {
		c := v[i]
		switch c {
		case '\x00':
			buf.WriteString("\\\\0")
		case '\n':
			buf.WriteString("\\\\n")
		case '\r':
			buf.WriteString("\\\\r")
		case '\x1a':
			buf.WriteString("\\\\Z")
		case '\'':
			buf.WriteString("\\\\'")
		case '"':
			buf.WriteString("\\\"")
		case '\\':
			buf.WriteString("\\\\")
		default:
			buf.WriteByte(c)
		}
	}

	return buf.Bytes()
}

type duckdbTx struct {
	c *conn
}

func (t *duckdbTx) Commit() error {
	_, err := t.c.exec("COMMIT TRANSACTION")
	t.c = nil

	return err
}

func (t *duckdbTx) Rollback() error {
	_, err := t.c.exec("ROLLBACK")
	t.c = nil

	return err
}
