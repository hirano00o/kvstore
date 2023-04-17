package kvstore

import (
	"fmt"
	"os"
)

type pgnum uint64

type page struct {
	Num  pgnum
	Data []byte
}

type dal struct {
	file     *os.File
	pageSize int

	*freeList
}

func NewDal(path string, pageSize int) (*dal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	dal := &dal{
		file:     f,
		pageSize: pageSize,
		freeList: newFreeList(),
	}

	return dal, nil
}

func (d *dal) Close() error {
	if d.file != nil {
		err := d.file.Close()
		if err != nil {
			return fmt.Errorf("could not close file: %v", err)
		}
		d.file = nil
	}
	return nil
}

func (d *dal) AllocateEmptyPage() *page {
	return &page{
		Data: make([]byte, d.pageSize),
	}
}

func (d *dal) ReadPage(pageNum pgnum) (*page, error) {
	p := d.AllocateEmptyPage()

	offset := int(pageNum) * d.pageSize

	_, err := d.file.ReadAt(p.Data, int64(offset))
	if err != nil {
		return nil, err
	}

	return p, err
}

func (d *dal) WritePage(p *page) error {
	offset := int64(p.Num) * int64(d.pageSize)
	_, err := d.file.WriteAt(p.Data, offset)
	return err
}
