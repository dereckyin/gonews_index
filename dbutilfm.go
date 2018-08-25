package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

// FMCli Database instance
type FMCli struct {
	db *sql.DB
}

var instanceFmCli *FMCli = nil

// ConnectFM to databas
func ConnectFM(fmhost string, fmport int, fmuser string, fmpassword string, fmdbname string) (db *sql.DB, err error) {
	if instanceFmCli == nil {
		instanceFmCli = new(FMCli)
		var err error

		psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
			"password=%s dbname=%s sslmode=disable",
			fmhost, fmport, fmuser, fmpassword, fmdbname)

		// instancePGCli.db, err = sql.Open("postgres", "user:password@/database")

		instanceFmCli.db, err = sql.Open("postgres", psqlInfo)
		if err != nil {
			return nil, err
		}

		instanceFmCli.db.SetMaxOpenConns(30)
		instanceFmCli.db.SetMaxIdleConns(5)
	}

	return instanceFmCli.db, nil
}

// ClosePG database
func CloseFM() {
	if instanceFmCli != nil {
		instanceFmCli.db.Close()
	}
}
