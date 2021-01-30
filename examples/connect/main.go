package main

import (
	"fmt"

	"github.com/SERV4BIZ/escondb"
)

func main() {
	dbConn, errConn := escondb.Connect("postgresql", "localhost", 5432, "postgres", "Qaz74100!", "beebber")
	if errConn != nil {
		panic(errConn)
	}
	defer dbConn.Close()

	fmt.Println("Connected")
}
