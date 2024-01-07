package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

type Db struct {
	file   *os.File
	schema []Payload
	tables map[string]*Payload
	Header Header
}

type Header struct {
	buf           [100]byte
	HeaderString  string
	PageSize      uint16
	TotalPages    uint32
	ReservedSpace byte
}

func getDb(dbPath string) *Db {
	f, err := os.Open(dbPath)

	if err != nil {
		log.Panic(err)
	}

	db := Db{file: f}
	db.Init()
	return &db
}

func (db *Db) Init() {

	// init header
	db.Header = Header{}
	db.file.Read(db.Header.buf[:])
	db.Header.HeaderString = string(db.Header.buf[:16])
	db.Header.PageSize = binary.BigEndian.Uint16(db.Header.buf[16:18])
	db.Header.TotalPages = binary.BigEndian.Uint32(db.Header.buf[28:32])

	db.schema = db.GetPage(1).BTree().GetAllChildren()

	// create tables map
	db.tables = map[string]*Payload{}
	for i := 0; i < len(db.schema); i++ {
		payload := &db.schema[i]
		if string(payload.Columns[0].buf) == "table" {
			db.tables[string(payload.Columns[1].buf)] = payload
		}
	}

}

func (db *Db) GetTable(name string) (*BTree, error) {
	table, exists := db.tables[name]
	if !exists {
		return nil, fmt.Errorf("Table %s either does not exist or db is not initialized.", name)
	}
	return db.GetPage(int(table.Columns[3].value)).BTree(), nil
}

func (db *Db) GetPage(i int) *Page {
	// Get the page
	offset := (i - 1) * int(db.Header.PageSize)
	buf := make([]byte, db.Header.PageSize)
	db.file.ReadAt(buf, int64(offset))

	return &Page{
		buf:   buf,
		index: i,
		db:    db,
	}
}
