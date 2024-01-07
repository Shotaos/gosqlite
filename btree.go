package main

import (
	"encoding/binary"
	"log"
)

const (
	INTERIOR_INDEX = 0x02
	INTERIOR_TABLE = 0x05
	LEAF_INDEX     = 0x0a
	LEAF_TABLE     = 0x0d
)

type BTree struct {
	*Page
	Type                byte
	FreeBlockOffset     uint16
	NumOfCells          uint16
	CellContentArea     uint16
	FragmentedFreeBytes byte
	RightmostChild      uint32
}

func (btree *BTree) GetCellOffsets() []uint16 {
	result := make([]uint16, btree.NumOfCells)
	offset := 8

	if btree.Type == INTERIOR_TABLE {
		offset = 12
	}

	if btree.index == 1 {
		offset += 100
	}

	for i := 0; i < int(btree.NumOfCells); i++ {
		result[i] = binary.BigEndian.Uint16((btree.buf[offset+2*i:]))
	}

	return result
}

// The number of bytes of the payload that is locally stored
func (btree *BTree) getLocalPayload(nTotal int) int {
	nUsable := int(btree.Page.db.Header.PageSize - uint16(btree.Page.db.Header.ReservedSpace))
	nLocal := 0
	nMinLocal := 0
	nMaxLocal := 0

	nMinLocal = (nUsable-12)*32/255 - 23
	nMaxLocal = nUsable - 35

	nLocal = nMinLocal + (nTotal-nMinLocal)%(nUsable-4)

	if nLocal > nMaxLocal {
		nLocal = nMinLocal
	}

	return nLocal
}

func GetBTreeContent(btree *BTree) []Payload {
	result := []Payload{}
	for _, offsetStart := range btree.GetCellOffsets() {
		offset := offsetStart
		var l byte

		total, l := DecodeVariant(btree.buf[offset:])
		offset += uint16(l)

		rowId, l := DecodeVariant(btree.buf[offset:])
		offset += uint16(l)

		buffer := btree.buf[offset:]

		localData := btree.getLocalPayload(int(total))

		// Handle Overflow pages
		if total > int64(localData) {
			log.Panic("We can not handle overflow pages")
			nextPage := binary.BigEndian.Uint32(btree.buf[int(offset)+int(localData)-4:])

			for nextPage != 0 {
				overflowPage := btree.db.GetPage(int(nextPage))
				nextPage = binary.BigEndian.Uint32(overflowPage.buf)
				buffer = append(buffer, overflowPage.buf[5:]...)
			}

		}

		payload := Payload{buf: buffer, TotalSize: total, RowId: rowId, LocalPageData: localData}
		payload.Init()
		result = append(result, payload)
	}
	return result

}

func (btree *BTree) GetAllChildren() []Payload {

	if btree.Type == LEAF_TABLE {
		return GetBTreeContent(btree)
	}

	result := []Payload{}
	children := make([]int64, btree.NumOfCells+1)
	offsets := btree.GetCellOffsets()

	for i, offset := range offsets {
		pointer := binary.BigEndian.Uint32(btree.buf[offset:])
		children[i] = int64(pointer)
	}

	children[btree.NumOfCells] = int64(btree.RightmostChild)

	for _, child := range children {
		childPage := btree.db.GetPage(int(child))
		childBtree := childPage.BTree()
		result = append(result, childBtree.GetAllChildren()...)

	}

	return result
}

func (btree *BTree) GetPayload(rowId int64) *Payload {
	leaf := btree.getLeafNodeByRowId(rowId)
	for _, payload := range leaf.GetAllChildren() {

		if payload.RowId == rowId {
			return &payload
		}
	}
	return nil
}

func (btree *BTree) getLeafNodeByRowId(rowId int64) *BTree {

	if btree.Type != LEAF_TABLE && btree.Type != INTERIOR_TABLE {
		log.Panic("This method can only be called on tables")
	}

	if btree.Type == LEAF_TABLE {
		return btree
	}

	offsets := btree.GetCellOffsets()
	for _, offset := range offsets {
		pointer := binary.BigEndian.Uint32(btree.buf[offset:])
		key, _ := DecodeVariant(btree.buf[offset+4:])

		if rowId < key {
			return btree.db.GetPage(int(pointer)).BTree().getLeafNodeByRowId(rowId)
		}

	}

	return btree.db.GetPage(int(btree.RightmostChild)).BTree().getLeafNodeByRowId(rowId)

}
