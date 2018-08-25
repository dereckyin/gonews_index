package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

// PGCli Database instance
type PGCli struct {
	db *sql.DB
}

var instancePGCli *PGCli = nil

// ConnectPM to databas
func ConnectPM(pghost string, pgport int, pguser string, pgpassword string, pgdbname string) (db *sql.DB, err error) {
	if instancePGCli == nil {
		instancePGCli = new(PGCli)
		var err error

		psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
			"password=%s dbname=%s sslmode=disable",
			pghost, pgport, pguser, pgpassword, pgdbname)

		// instancePGCli.db, err = sql.Open("postgres", "user:password@/database")

		instancePGCli.db, err = sql.Open("postgres", psqlInfo)
		if err != nil {
			return nil, err
		}

		instancePGCli.db.SetMaxOpenConns(30)
		instancePGCli.db.SetMaxIdleConns(5)
	}

	return instancePGCli.db, nil
}

// ClosePG database
func ClosePM() {
	if instancePGCli != nil {
		instancePGCli.db.Close()
	}
}
