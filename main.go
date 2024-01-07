package main

import (
	"fmt"
	"log"
	"strconv"
)

func main() {

	//dbName := os.Args[1]
	//tableName := os.Args[2]
	//searchId := os.Args[3]

	dbName := "sample_database.db"
	tableName := "users"
	searchId := "90000"

	db := getDb(dbName)

	table, err := db.GetTable(tableName)

	if err != nil {
		log.Panic("Could not get Table", err)
	}

	rowId, err := strconv.Atoi(searchId)
	payload := table.GetPayload(int64(rowId))
	if payload != nil {
		PrintRow(payload)
	} else {
		fmt.Println("Could not find rowId", rowId)
	}

}
