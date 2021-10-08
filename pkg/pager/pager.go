package pager

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	config "github.com/brown-csci1270/db/pkg/config"
	list "github.com/brown-csci1270/db/pkg/list"

	directio "github.com/ncw/directio"
)

// Page size - defaults to 4kb.
const PAGESIZE = int64(directio.BlockSize)

// Number of pages.
const NUMPAGES = config.NumPages

// Pagers manage pages of data read from a file.
type Pager struct {
	file         *os.File             // File descriptor.
	nPages       int64                // The number of pages used by this database.
	ptMtx        sync.Mutex           // Page table mutex.
	freeList     *list.List           // Free page list.
	unpinnedList *list.List           // Unpinned page list.
	pinnedList   *list.List           // Pinned page list.
	pageTable    map[int64]*list.Link // Page table.
}

// Construct a new Pager.
func NewPager() *Pager {
	var pager *Pager = &Pager{}
	pager.pageTable = make(map[int64]*list.Link)
	pager.freeList = list.NewList()
	pager.unpinnedList = list.NewList()
	pager.pinnedList = list.NewList()
	frames := directio.AlignedBlock(int(PAGESIZE * NUMPAGES))
	for i := 0; i < NUMPAGES; i++ {
		frame := frames[i*int(PAGESIZE) : (i+1)*int(PAGESIZE)]
		page := Page{
			pager:    pager,
			pagenum:  NOPAGE,
			pinCount: 0,
			dirty:    false,
			data:     &frame,
		}
		pager.freeList.PushTail(&page)
	}
	return pager
}

// HasFile checks if the pager is backed by disk.
func (pager *Pager) HasFile() bool {
	return pager.file != nil
}

// GetFileName returns the file name.
func (pager *Pager) GetFileName() string {
	return pager.file.Name()
}

// GetNumPages returns the number of pages.
func (pager *Pager) GetNumPages() int64 {
	return pager.nPages
}

// GetFreePN returns the next available page number.
func (pager *Pager) GetFreePN() int64 {
	// Assign the first page number beyond the end of the file.
	return pager.nPages
}

// Open initializes our page with a given database file.
func (pager *Pager) Open(filename string) (err error) {
	// Create the necessary prerequisite directories.
	if idx := strings.LastIndex(filename, "/"); idx != -1 {
		err = os.MkdirAll(filename[:idx], 0775)
		if err != nil {
			return err
		}
	}
	// Open or create the db file.
	pager.file, err = directio.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	// Get info about the size of the pager.
	var info os.FileInfo
	var len int64
	if info, err = pager.file.Stat(); err == nil {
		len = info.Size()
		if len%PAGESIZE != 0 {
			return errors.New("open: DB file has been corrupted")
		}
	}
	// Set the number of pages and hand off initialization to someone else.
	pager.nPages = len / PAGESIZE
	return nil
}

// Close signals our pager to flush all dirty pages to disk.
func (pager *Pager) Close() (err error) {
	// Prevent new data from being paged in.
	pager.ptMtx.Lock()
	// Check if all refcounts are 0.
	curLink := pager.pinnedList.PeekHead()
	if curLink != nil {
		fmt.Println("ERROR: pages are still pinned on close")
	}
	// Cleanup.
	pager.FlushAllPages()
	if pager.file != nil {
		err = pager.file.Close()
	}
	pager.ptMtx.Unlock()
	return err
}

// Populate a page's data field, given a pagenumber.
func (pager *Pager) ReadPageFromDisk(page *Page, pagenum int64) error {
	if _, err := pager.file.Seek(pagenum*PAGESIZE, 0); err != nil {
		return err
	}
	if _, err := pager.file.Read(*page.data); err != nil && err != io.EOF {
		return err
	}
	return nil
}

// NewPage returns an unused buffer from the free or unpinned list
// the ptMtx should be locked on entry
func (pager *Pager) NewPage(pagenum int64) (*Page, error) {
	pager.ptMtx.Lock()
	defer pager.ptMtx.Unlock()
	freeHead := pager.freeList.PeekHead()
	if freeHead != nil {
		page := freeHead.GetKey().(*Page)
		freeHead.PopSelf()
		page.pinCount = 1
		page.pagenum = pagenum
		pager.nPages += 1
		pager.pinnedList.PushTail(page)
		pager.pageTable[pagenum] = pager.pinnedList.PeekTail()
		return page, nil
	}
	unpinnedHead := pager.unpinnedList.PeekHead()
	if unpinnedHead == nil {
		return nil, fmt.Errorf("No pages available!")
	}
	page := unpinnedHead.GetKey().(*Page)
	if page.IsDirty() {
		// TODO: flush to disk
		pager.FlushPage(page)
	}
	unpinnedHead.PopSelf()
	page.pinCount = 1
	page.pagenum = pagenum
	pager.nPages += 1
	pager.pinnedList.PushTail(page)
	pager.pageTable[pagenum] = pager.pinnedList.PeekTail()
	return page, nil
}

// getPage returns the page corresponding to the given pagenum.
func (pager *Pager) GetPage(pagenum int64) (page *Page, err error) {
	pager.ptMtx.Lock()
	defer pager.ptMtx.Unlock()
	if pagenum > pager.nPages+1 {
		return nil, fmt.Errorf("Invalid page number %v, current pages: %v", pagenum, pager.nPages)
	}
	pLink, ok := pager.pageTable[pagenum]
	if !ok {
		page, err := pager.NewPage(pagenum)
		if err != nil {
			return nil, err
		}
		err = pager.ReadPageFromDisk(page, pagenum)
		if err != nil {
			return nil, err
		}
	} else {
		page = pLink.GetKey().(*Page)
		err = pager.ReadPageFromDisk(page, pagenum)
		if err != nil {
			return nil, err
		}
		if page.pinCount == 0 {
			page.pinCount = 1
			// TODO: move to pinned list
			pLink.PopSelf()
			pager.pinnedList.PushTail(page)
		} else {
			page.pinCount += 1
		}
	}
	return page, nil
}

// Flush a particular page to disk.
func (pager *Pager) FlushPage(page *Page) {
	pager.ptMtx.Lock()
	defer pager.ptMtx.Unlock()
	if pager.file == nil {
		return
	}
	pager.file.WriteAt(*page.GetData(), page.pagenum*PAGESIZE)
}

// Flushes all dirty pages.
func (pager *Pager) FlushAllPages() {
	pager.ptMtx.Lock()
	defer pager.ptMtx.Unlock()
	cur := pager.pinnedList.PeekHead()
	for cur != nil {
		page := cur.GetKey().(*Page)
		if page.IsDirty() {
			pager.FlushPage(page)
		}
		cur = cur.GetNext()
	}

	cur = pager.unpinnedList.PeekHead()
	for cur != nil {
		page := cur.GetKey().(*Page)
		if page.IsDirty() {
			pager.FlushPage(page)
		}
		cur = cur.GetNext()
	}
}
