package db_handler

import (
	"crypto/md5"
	"database/sql"
	"fmt"
	"runtime"
	"sync"

	_ "github.com/lib/pq"
)

// DbHandler is using in db_object package
type DbHandler struct {
	DatabaseHandler  *sql.DB
	Debug            bool
	DeleteNullValues bool
	dbURLHash        string
}

var dbh map[string]*sql.DB
var activeConnections map[string]uint
var activeConnectionsLocker, dbhLocker *sync.RWMutex
var dbURLHashes chan string

func init() {
	dbh = make(map[string]*sql.DB)
	activeConnections = make(map[string]uint)
	activeConnectionsLocker = &sync.RWMutex{}
	dbhLocker = &sync.RWMutex{}
	dbURLHashes = make(chan string, runtime.NumCPU()*runtime.NumCPU())
	go dbURLHashesUpdate()
}

func dbURLHashesUpdate() {
	for {
		dbURLHash := <-dbURLHashes
		activeConnectionsLocker.Lock()
		activeConnections[dbURLHash]++
		activeConnectionsLocker.Unlock()
	}
}

func New() *DbHandler {
	h := &DbHandler{}
	return h
}

func (h *DbHandler) Connect(dbURL string) (err error) {
	dbURLHash := fmt.Sprintf("%x", md5.Sum([]byte(dbURL)))
	dbhLocker.RLock()
	if dbh[dbURLHash] == nil {
		dbhLocker.RUnlock()
		dbhLocker.Lock()
		if dbh[dbURLHash], err = sql.Open("postgres", dbURL); err != nil {
			return err
		}
		dbhLocker.Unlock()
		dbhLocker.RLock()
	}
	h.DatabaseHandler = dbh[dbURLHash]
	dbhLocker.RUnlock()
	h.dbURLHash = dbURLHash
	dbURLHashes <- h.dbURLHash
	return nil
}

func (h *DbHandler) Disconnect() (err error) {
	defer func() {
		h.DatabaseHandler = nil
	}()
	activeConnectionsLocker.Lock()
	defer activeConnectionsLocker.Unlock()
	if activeConnections[h.dbURLHash] > 0 {
		activeConnections[h.dbURLHash]--
	}
	if activeConnections[h.dbURLHash] > 0 {
		return nil
	}
	delete(activeConnections, h.dbURLHash)
	dbhLocker.Lock()
	defer dbhLocker.Unlock()
	delete(dbh, h.dbURLHash)
	if err = h.DatabaseHandler.Close(); err != nil {
		fmt.Printf("DatabaseHandler.Close: %s\n", err)
		return err
	}
	return nil
}

func DisconnectAll() {
	activeConnectionsLocker.Lock()
	defer activeConnectionsLocker.Unlock()
	dbhLocker.Lock()
	defer dbhLocker.Unlock()
	for dbURLHash := range dbh {
		if dbh[dbURLHash] != nil {
			if err := dbh[dbURLHash].Close(); err != nil {
				fmt.Printf(`dbh["%s"].Close: %s\n`, dbURLHash, err)
			}
			delete(activeConnections, dbURLHash)
			delete(dbh, dbURLHash)
		}
	}
}

func getData(deleteNullValues bool, check, finish chan bool, dbData chan interface{}, columns []string, data *[]map[string]string) {
	lnc := len(columns)
	for <-check {
		row := make(map[string]string)
		for i := 0; i < lnc; i++ {
			el := <-dbData
			b, ok := el.([]byte)
			if ok {
				row[columns[i]] = string(b)
			} else {
				if el == nil {
					if deleteNullValues == false {
						row[columns[i]] = ""
					}
				} else {
					row[columns[i]] = fmt.Sprint(el)
				}
			}
		}
		*data = append(*data, row)
	}
	finish <- true
}

func transferData(values []interface{}, dbData chan interface{}, working, check chan bool) {
	for <-working {
		check <- true
		for _, v := range values {
			dbData <- v
		}
	}
}

func (h *DbHandler) QueryColumns(query string, args ...interface{}) (data []map[string]string, columns []string, err error) {
	rows, err := h.DatabaseHandler.Query(query, args...)
	if err != nil {
		fmt.Printf("DatabaseHandler.Query: %s\n%s\n%#v\n", err, query, args)
		return nil, nil, err
	}

	defer func() {
		if err = rows.Err(); err != nil {
			fmt.Printf("rows.Err: %s\n%s\n%#v\n", err, query, args)
		}
		if err := rows.Close(); err != nil {
			fmt.Printf("rows.Close: %s\n%s\n%#v\n", err, query, args)
		}
	}()

	columns, err = rows.Columns()
	if err != nil {
		fmt.Printf("rows.Columns: %s\n", err)
		return nil, nil, err
	}
	count := len(columns)

	values := make([]interface{}, count)
	valuesPtrs := make([]interface{}, count)

	for i := range columns {
		valuesPtrs[i] = &values[i]
	}

	check := make(chan bool, count)
	finish := make(chan bool, 1)

	working := make(chan bool, 2)

	dbData := make(chan interface{}, count*count)

	go getData(h.DeleteNullValues, check, finish, dbData, columns, &data)
	go transferData(values, dbData, working, check)

	var chnl bool

	for rows.Next() {
		if err = rows.Scan(valuesPtrs...); err != nil {
			fmt.Printf("rows.Scan: %s\n%s\n%#v\n", err, query, args)
			return nil, nil, err
		}
		working <- true
		chnl = !chnl
	}

	check <- false
	working <- false
	<-finish

	return data, columns, nil
}

func (h *DbHandler) Query(query string, args ...interface{}) (data []map[string]string, err error) {
	data, _, err = h.QueryColumns(query, args...)
	return data, err
}

func (h *DbHandler) QueryRow(query string, args ...interface{}) (data map[string]string, err error) {
	dataRows, err := h.Query(query, args...)
	if err != nil {
		fmt.Printf("h.Query: %s\n", err)
		return nil, err
	}
	if len(dataRows) > 0 {
		data = dataRows[0]
	} else {
		data = make(map[string]string)
	}
	return data, nil
}

func (h *DbHandler) QueryRowColumns(query string, args ...interface{}) (data map[string]string, columns []string, err error) {
	dataRows, columns, err := h.QueryColumns(query, args...)
	if err != nil {
		fmt.Printf("h.QueryColumns: %s\n", err)
		return nil, nil, err
	}
	if len(dataRows) > 0 {
		data = dataRows[0]
	} else {
		data = make(map[string]string)
	}
	return data, columns, nil
}

func (h *DbHandler) Exec(query string, args ...interface{}) (result sql.Result, err error) {
	if result, err = h.DatabaseHandler.Exec(query, args...); err != nil {
		fmt.Printf("DatabaseHandler.Exec: %s\n%s\n%#v\n", err, query, args)
		return nil, err
	}
	return result, nil
}
