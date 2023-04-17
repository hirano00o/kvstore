package kvstore

import "encoding/binary"

const metadataPageNum = 0

type metadata struct {
	freeListPage pgnum
}

func newEmptyMetadata() *metadata {
	return &metadata{}
}

func (m *metadata) serialize(buf []byte) {
	pos := 0
	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.freeListPage))
	pos += pageNumSize
}

func (m *metadata) deserialize(buf []byte) {
	pos := 0
	m.freeListPage = pgnum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumSize
}
