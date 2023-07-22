package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

func main() {
	// Empty datasource means, that DB will be solely in-memory, otherwise you could specify a filename here
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`
		CREATE TABLE employee (
		    id INTEGER,
			name VARCHAR(20),
			start_dt TIMESTAMP,
			is_remote BOOLEAN
		)`)
	if err != nil {
		log.Fatal(err)
	}

	// Insert some data in table
	_, err = db.Exec(`
		INSERT INTO employee (id, name, start_dt, is_remote) 
		VALUES
		    (1, 'John Doe', '2022-01-01 09:00:00', true),
       		(2, 'Jane Smith', '2023-03-15 10:00:00', false)`)
	if err != nil {
		log.Fatal(err)
	}

	// Error handling and transactions
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO employee (id, name, start_dt, is_remote)
		VALUES
		    (3000000000, 'id int64 instead of int32', '2022-06-17 11:00:00', true)`)
	if err != nil {
		log.Printf("ERROR: %s\n", err.Error()) // Do not fail, just print the error in output
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

	// Variables to store query result
	var id int
	var name string
	var startDt time.Time
	var isRemote bool

	// Query single row
	if err = db.QueryRow("SELECT id, name, start_dt, is_remote FROM employee WHERE id = ?", 1).Scan(&id, &name, &startDt, &isRemote); err != nil {
		if err == sql.ErrNoRows {
			log.Println("No rows found.")
		} else {
			log.Fatalf("unable to execute query: %v", err)
		}
	} else {
		fmt.Println("Select 1 row result:\nID:", id, "Name:", name, "Start Datetime:", startDt, "Is Remote:", isRemote)
	}

	// Query multiple rows
	rows, err := db.Query("SELECT id, name, start_dt, is_remote FROM employee")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Print the results
	fmt.Println("All Results:")
	for rows.Next() {
		err = rows.Scan(&id, &name, &startDt, &isRemote)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("ID:", id, "Name:", name, "Start Datetime:", startDt, "Is Remote:", isRemote)
	}

	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
}
