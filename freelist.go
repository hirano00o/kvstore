package kvstore

import "encoding/binary"

const initialPage = 0

type freeList struct {
	maxPage       pgnum
	releasedPages []pgnum
}

func newFreeList() *freeList {
	return &freeList{
		maxPage:       initialPage,
		releasedPages: []pgnum{},
	}
}

func (f *freeList) GetNextPage() pgnum {
	if len(f.releasedPages) != 0 {
		pageID := f.releasedPages[len(f.releasedPages)-1]
		f.releasedPages = f.releasedPages[:len(f.releasedPages)-1]
		return pageID
	}
	f.maxPage += 1
	return f.maxPage
}

func (f *freeList) ReleasePage(page pgnum) {
	f.releasedPages = append(f.releasedPages, page)
}

func (f *freeList) serialize(buf []byte) []byte {
	pos := 0

	binary.LittleEndian.PutUint16(buf[pos:], uint16(f.maxPage))
	pos += 2

	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(f.releasedPages)))
	pos += 2

	for _, page := range f.releasedPages {
		binary.LittleEndian.PutUint64(buf[pos:], uint64(page))
		pos += pageNumSize
	}

	return buf
}

func (f *freeList) deserialize(buf []byte) {
	pos := 0
	f.maxPage = pgnum(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2

	releasedPageCount := int(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2

	for i := 0; i < releasedPageCount; i++ {
		f.releasedPages = append(f.releasedPages, pgnum(binary.LittleEndian.Uint64(buf[pos:])))
		pos += pageNumSize
	}
}
