package main

import (
	"fmt"
	"math/rand"
	"time"
	"github.com/gopostgres/packages/db/postgresql/db_handler"
	// "github.com/gopostgres/packages/db/postgresql/db_handler.one"
)

func main() {
	dbHandler := db_handler.New()
	dbHandler.Connect("postgres://postgres:postgres@localhost:5432/testdb?sslmode=disable")
	if false {
		// fill the database
		query := `
		INSERT INTO "test_table"
			("column_1", "column_2", "column_3", "column_4", "column_5", "column_6", "column_7", "column_8", "column_9", "column_10", "column_11", "column_12", "column_13", "column_14", "column_15", "column_16", "column_17", "column_18", "column_19", "column_20", "column_21", "column_22", "column_23", "column_24", "column_25", "column_26", "column_27", "column_28", "column_29", "column_30")
		VALUES
			($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30)
	`
		for i := 0; i < 100000; i++ {
			var values []interface{}
			for j := 1; j <= 30; j++ {
				values = append(values, rand.Intn(1000000))
			}
			dbHandler.Exec(query, values...)
		}
	}
	start := time.Now()
	for i := 0; i < 100; i++ {
		dbHandler.Query(`SELECT * FROM "test_table"`)
	}
	stop := time.Now().Sub(start)
	fmt.Printf("Time spent: %s\n", stop.String())
	dbHandler.Disconnect()
}
