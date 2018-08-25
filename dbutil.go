package main

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

// MySQLCli Database instance
type MySQLCli struct {
	db *sql.DB
}

var instanceMySQLCli *MySQLCli = nil

// Connect to databas
func Connect(host string, port int, user string, password string, dbname string) (db *sql.DB, err error) {
	if instanceMySQLCli == nil {
		instanceMySQLCli = new(MySQLCli)
		var err error

		//psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		//	"password=%s dbname=%s sslmode=disable",
		//	host, port, user, password, dbname)

		psqlInfo := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=true",
			user, password, host, port, dbname)

		//instanceMySQLCli.db, err = sql.Open("postgres", "user:password@/database")

		instanceMySQLCli.db, err = sql.Open("mysql", psqlInfo)
		if err != nil {
			return nil, err
		}

		instanceMySQLCli.db.SetMaxOpenConns(30)
		instanceMySQLCli.db.SetMaxIdleConns(5)
	}

	return instanceMySQLCli.db, nil
}

// Close database
func Close() {
	if instanceMySQLCli != nil {
		instanceMySQLCli.db.Close()
	}
}
