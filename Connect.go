package escondb

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	// Postgresql Driver
	_ "github.com/lib/pq"
)

// Connect is create ESCONDB object
func Connect(txtType string, txtHost string, intPort int, txtUsername string, txtPassword string, txtDBName string) (*ESCONDB, error) {
	txtTypeNew := strings.TrimSpace(strings.ToUpper(txtType))

	gqConn := new(ESCONDB)
	gqConn.Type = txtTypeNew
	gqConn.Host = txtHost
	gqConn.Port = intPort
	gqConn.Username = txtUsername
	gqConn.Password = txtPassword
	gqConn.Database = txtDBName

	if gqConn.Type == "POSTGRES" || gqConn.Type == "POSTGRESQL" || gqConn.Type == "PGSQL" {
		gqConn.Type = "POSTGRESQL"
		
		psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", txtHost, intPort, txtUsername, txtPassword, txtDBName)
		conn, errConn := sql.Open("postgres", psqlInfo)
		if errConn != nil {
			return nil, errConn
		}

		errPing := conn.Ping()
		if errPing != nil {
			conn.Close()
			conn = nil
			return nil, errPing
		}

		gqConn.DB = conn
	} else {
		return nil, errors.New("Database driver not support")
	}

	return gqConn, nil
}
