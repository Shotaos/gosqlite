package main

func DecodeVariant(buf []byte) (int64, byte) {
	var result int64
	var i byte

	for ; i < 8; i++ {
		result = result<<7 | int64(buf[i]&0x7f)

		if buf[i]&0x80 == 0 {
			return result, i + 1
		}
	}

	return result<<8 | int64(buf[i]), 9

}
