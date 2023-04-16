package kvstore

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

func (f *freeList) getNextPage() pgnum {
	if len(f.releasedPages) != 0 {
		pageID := f.releasedPages[len(f.releasedPages)-1]
		f.releasedPages = f.releasedPages[:len(f.releasedPages)-1]
		return pageID
	}
	f.maxPage += 1
	return f.maxPage
}

func (f *freeList) releasePage(page pgnum) {
	f.releasedPages = append(f.releasedPages, page)
}
