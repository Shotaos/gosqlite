package main

import (
	"encoding/binary"
	"fmt"
	"log"
)

const (
	NULL      byte = 0
	INT8      byte = 1
	INT16     byte = 2
	INT24     byte = 3
	INT32     byte = 4
	INT48     byte = 5
	INT64     byte = 6
	FLOAT64   byte = 7
	ZERO      byte = 8
	ONE       byte = 9
	RESERVED1 byte = 10
	RESERVED2 byte = 11
	BLOB      byte = 12
	STRING    byte = 13
)

type Payload struct {
	TotalSize     int64
	RowId         int64
	LocalPageData int
	buf           []byte
	Columns       []Record
	OverflowPages []Page
}

type Record struct {
	Length     int16
	SerialType byte
	value      int64
	buf        []byte
}

func (payload *Payload) Init() {

	if int(payload.TotalSize) > len(payload.buf) {
		return
	}

	offset := 0
	records := []Record{}

	// Decode the payload header length
	header, length := DecodeVariant(payload.buf)
	offset += int(length)

	// Decode the Serial Type of columns
	for offset < int(header) {
		serialType, length := DecodeVariant(payload.buf[offset:])
		recordLength := 0

		if serialType >= 12 && serialType%2 == 0 {
			recordLength = (int(serialType) - 12) / 2
			serialType = 12
		}

		if serialType >= 13 && serialType%2 == 1 {
			recordLength = (int(serialType) - 13) / 2
			serialType = 13
		}

		records = append(records, Record{SerialType: byte(serialType), Length: int16(recordLength)})
		offset += int(length)
	}

	// Decode the record Value
	for j := 0; j < len(records); j++ {
		record := &records[j]
		switch record.SerialType {
		case NULL:
		case INT8:
			record.value = int64(payload.buf[offset])
			record.Length = 1
		case INT16:
			record.value = int64(binary.BigEndian.Uint16(payload.buf[offset:]))
			record.Length = 2
		case INT24:
			record.value = int64(binary.BigEndian.Uint16(payload.buf[offset:]))
			record.Length = 3
		case INT32:
			record.value = int64(binary.BigEndian.Uint32(payload.buf[offset:]))
			record.Length = 4
		case INT48:
			record.value = int64(binary.BigEndian.Uint32(payload.buf[offset:]))
			record.Length = 6
		case INT64:
			record.value = int64(binary.BigEndian.Uint64(payload.buf[offset:]))
			record.Length = 8
		case FLOAT64:
			record.value = int64(binary.BigEndian.Uint64(payload.buf[offset:]))
			record.Length = 8
		case ZERO:
			record.value = 0
		case ONE:
			record.value = 1
		case RESERVED1:
		case RESERVED2:
		case STRING:
			record.buf = payload.buf[offset : offset+int(record.Length)]
		case BLOB:
			record.buf = payload.buf[offset : offset+int(record.Length)]
		default:
			log.Panicf("Sumimasen WTF?, incorrect serial type %d\n", record.SerialType)
		}

		offset += int(record.Length)
	}
	payload.Columns = records

}

func PrintRow(payload *Payload) {
	if payload == nil {
		log.Panic("Gonna panic because empty payload was passed")
	}
	for j := 0; j < len(payload.Columns); j++ {
		record := payload.Columns[j]

		if record.SerialType > 11 {
			fmt.Print(string(record.buf))
		} else {
			fmt.Print(record.value)
		}

		if j != len(payload.Columns)-1 {
			fmt.Print("|")

		}
	}
	fmt.Print("\n")
}
