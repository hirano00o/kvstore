package kvstore

import (
	"errors"
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

	*metadata
	*freeList
}

func NewDal(path string) (*dal, error) {
	d := &dal{
		metadata: newEmptyMetadata(),
		pageSize: os.Getpagesize(),
	}
	if _, err := os.Stat(path); err == nil {
		d.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, err
		}

		d.metadata, err = d.readMetadata()
		if err != nil {
			return nil, err
		}

		d.freeList, err = d.readFreeList()
		if err != nil {
			return nil, err
		}
	} else if errors.Is(err, os.ErrNotExist) {
		d.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, err
		}

		d.freeList = newFreeList()
		d.freeListPage = d.GetNextPage()
		_, err := d.WriteFreeList()
		if err != nil {
			return nil, err
		}
		_, err = d.writeMetadata(d.metadata)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}

	return d, nil
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
		Data: make([]byte, d.pageSize, d.pageSize),
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

func (d *dal) writeMetadata(m *metadata) (*page, error) {
	p := d.AllocateEmptyPage()
	p.Num = metadataPageNum
	m.serialize(p.Data)

	err := d.WritePage(p)
	if err != nil {
		return nil, err
	}

	return p, err
}

func (d *dal) readMetadata() (*metadata, error) {
	p, err := d.ReadPage(metadataPageNum)
	if err != nil {
		return nil, err
	}

	m := newEmptyMetadata()
	m.deserialize(p.Data)

	return m, nil
}

func (d *dal) WriteFreeList() (*page, error) {
	p := d.AllocateEmptyPage()
	p.Num = d.freeListPage
	d.freeList.serialize(p.Data)

	err := d.WritePage(p)
	if err != nil {
		return nil, err
	}
	d.freeListPage = p.Num

	return p, nil
}

func (d *dal) readFreeList() (*freeList, error) {
	p, err := d.ReadPage(d.freeListPage)
	if err != nil {
		return nil, err
	}
	f := newFreeList()
	f.deserialize(p.Data)

	return f, nil
}
