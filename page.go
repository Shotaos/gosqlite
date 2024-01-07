package main

import (
	"encoding/binary"
)

type Page struct {
	db    *Db
	buf   []byte
	index int
}

func (page *Page) BTree() *BTree {
	offset := 0

	if page.index == 1 {
		offset = 100
	}
	return &BTree{
		Page:                page,
		Type:                page.buf[offset],
		FreeBlockOffset:     binary.BigEndian.Uint16(page.buf[offset+1:]),
		NumOfCells:          binary.BigEndian.Uint16(page.buf[offset+3:]),
		CellContentArea:     binary.BigEndian.Uint16(page.buf[offset+5:]),
		FragmentedFreeBytes: page.buf[offset+7],
		RightmostChild:      binary.BigEndian.Uint32(page.buf[offset+8:]),
	}

}
