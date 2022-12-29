package unite

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"
	"sync"
	"unsafe"
)

const (
	METASIZE = 1024 * 128
	MAGIC    = "unit"
)

var (
	NotFindFileError = errors.New("not find file")
	FileTypeError    = errors.New("the file is not a unite file")
	DamagedError     = errors.New("damaged file")
	AlreadyExists    = errors.New("file already exists")
	NameTooLongError = errors.New("file name too long")
)

type header struct {
	offset     [8]byte
	fileSymbol [8]byte
	createTime [8]byte
	modifyTime [8]byte
	size       [8]byte
	originData [8]byte
	fileName   [32]byte
}

type headerArea struct {
	headers  [1024]header
	offset   [8]byte
	next     [8]byte
	previous [8]byte
}

func createHeaderArea(offset, previous [8]byte) *headerArea {
	var ha headerArea

	ha.offset = offset
	ha.previous = previous

	size := int64(unsafe.Sizeof(hMode))
	for k := range ha.headers {
		ha.headers[k].offset = BigEndian(UnBigEndian(offset) + int64(k)*size)
	}

	return &ha
}

var hMode header
var mMode meta
var haMode headerArea

var originMeta = meta{
	offset:    BigEndian(0),
	seq:       BigEndian(-1),
	start:     BigEndian(0),
	end:       BigEndian(0),
	effective: BigEndian(0),
	next:      BigEndian(0),
	previous:  BigEndian(0),
}

var zero = BigEndian(0)
var full = [8]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

func BigEndian(i int64) [8]byte {
	b := [8]byte{}
	binary.BigEndian.PutUint64(b[:], uint64(i))
	return b
}

func UnBigEndian(b [8]byte) int64 {
	return int64(binary.BigEndian.Uint64(b[:]))
}

type meta struct {
	offset    [8]byte
	seq       [8]byte
	start     [8]byte // meta.offset-based offset
	end       [8]byte // meta.offset-based offset
	effective [8]byte
	next      [8]byte
	previous  [8]byte
	data      [METASIZE]byte
}

type file struct {
	head    *meta
	current *meta
	cursor  int64
	u       *Unite
}

func (f *file) Write(b []byte) (n int, err error) {

	if f.head == nil {
		f.u.mu.Lock()
		meta, err := f.u.createNewMeta(&originMeta)
		f.u.mu.Unlock()
		if err != nil {
			return 0, err
		}

		f.head = meta
		f.current = meta
	}

	if UnBigEndian(f.current.effective) == UnBigEndian(f.current.end)-UnBigEndian(f.current.start) {
		// meta full
		for {
			if UnBigEndian(f.current.next) == 0 {

				f.u.mu.Lock()
				meta, err := f.u.createNewMeta(f.current)
				f.u.mu.Unlock()
				if err != nil {
					return 0, err
				}

				f.current = meta
				break

			} else {
				err = f.nextMeta()
				if err != nil {
					return 0, err
				}

				if UnBigEndian(f.current.effective) != UnBigEndian(f.current.end)-UnBigEndian(f.current.start) {
					break
				}
			}
		}
	}

	for {

		metaRemain := METASIZE - UnBigEndian(f.current.effective)
		offset := UnBigEndian(f.current.offset) + int64(unsafe.Offsetof(f.current.data)) + UnBigEndian(f.current.effective)

		var bb []byte
		var nn int

		if metaRemain >= int64(len(b)) {
			bb = b
			b = b[:0]
		} else {
			bb = b[:metaRemain]
			b = b[metaRemain:]
		}

		nn, err = f.u.file.WriteAt(bb, offset)
		if err != nil {
			return 0, err
		}

		n += nn

		f.current.effective = BigEndian(UnBigEndian(f.current.effective) + int64(nn))

		// update meta.effective
		_, err = f.u.file.WriteAt(f.current.effective[:],
			UnBigEndian(f.current.offset)+int64(unsafe.Offsetof(f.current.effective)))
		if err != nil {
			return 0, err
		}

		if len(b) == 0 {
			return n, nil
		} else {
			f.u.mu.Lock()
			meta, err := f.u.createNewMeta(f.current)
			f.u.mu.Unlock()
			if err != nil {
				return 0, err
			}
			f.current = meta
		}
	}
}

func (f *file) Read(b []byte) (n int, err error) {

	for {
		beginAddr := UnBigEndian(f.current.seq) * METASIZE
		currentMetaOffset := f.cursor - beginAddr

		//metaRemain := UnBigEndian(f.current.effective) - currentMetaOffset

		nn := copy(b, f.current.data[currentMetaOffset:UnBigEndian(f.current.effective)])

		b = b[nn:]
		f.cursor += int64(nn)
		n += nn

		if len(b) != 0 {
			err = f.nextMeta()
			if err != nil {
				return n, err
			}
		} else {
			return n, nil
		}
	}
}

func (*file) Close() error {
	return nil
}

func (f *file) nextMeta() error {

	if UnBigEndian(f.current.next) == 0 {
		return io.EOF
	}

	// find next meta
	var nextMeta *meta
	b := make([]byte, unsafe.Sizeof(mMode))
	_, err := f.u.file.ReadAt(b, UnBigEndian(f.current.next))
	if err != nil {
		return err
	}
	nextMeta = (*meta)(unsafe.Pointer(&b[0]))
	f.current = nextMeta
	return nil
}

type Unite struct {
	mu          sync.RWMutex
	file        *syncFile
	headerArea  *headerArea
	headerMap   map[[8]byte]*header
	headerList  []*header
	idleHeaders []*header
	idleMetas   *meta
}

func (u *Unite) Open(name string) (*file, error) {

	u.mu.RLock()
	defer u.mu.RUnlock()

	sha := sha256.New()
	sha.Write([]byte(name))
	var symbol [8]byte

	copy(symbol[:], sha.Sum(nil)[:8])

	h, exist := u.headerMap[symbol]
	if !exist {
		return nil, NotFindFileError
	}

	var hMeta meta
	b := make([]byte, unsafe.Sizeof(mMode))
	_, err := u.file.ReadAt(b, UnBigEndian(h.originData))
	if err != nil {
		return nil, err
	}
	hMeta = *(*meta)(unsafe.Pointer(&b[0]))

	return &file{
		head:    &hMeta,
		current: &hMeta,
		cursor:  0,
		u:       u,
	}, nil
}

func (u *Unite) Create(name string) (*file, error) {

	nb := []byte(name)
	if len(nb) > 32 {
		return nil, NameTooLongError
	}

	nameBytes := [32]byte{}
	copy(nameBytes[:], nb)

	u.mu.Lock()
	defer u.mu.Unlock()

	sha := sha256.New()
	sha.Write([]byte(name))
	symbol := sha.Sum(nil)[:8]

	sy := [8]byte{}
	copy(sy[:], symbol)

	h, exist := u.headerMap[sy]
	if exist {
		return nil, AlreadyExists
	}

	if len(u.idleHeaders) != 0 {

		h = u.idleHeaders[0]

		h.fileSymbol = sy
		h.fileName = nameBytes

		_, err := u.file.WriteAt(sy[:], UnBigEndian(h.offset)+int64(unsafe.Offsetof(h.fileSymbol)))
		if err != nil {
			return nil, err
		}

		_, err = u.file.WriteAt(nameBytes[:], UnBigEndian(h.offset)+int64(unsafe.Offsetof(h.fileName)))
		if err != nil {
			return nil, err
		}

		// create new meta
		meta, err := u.createNewMeta(&originMeta)
		if err != nil {
			return nil, err
		}

		h.originData = meta.offset
		_, err = u.file.WriteAt(h.originData[:], UnBigEndian(h.offset)+int64(unsafe.Offsetof(h.originData)))
		if err != nil {
			return nil, err
		}

		u.headerMap[sy] = h
		u.idleHeaders = u.idleHeaders[1:]

		return &file{
			u:       u,
			head:    meta,
			current: meta,
			cursor:  0,
		}, err
	} else {

		offset, err := u.file.Seek(0, 2)
		if err != nil {
			return nil, err
		}

		ha := createHeaderArea(BigEndian(offset), u.headerArea.offset)
		ha.headers[0].fileSymbol = sy
		ha.headers[0].fileName = nameBytes
		ha.headers[0].originData = BigEndian(offset + int64(unsafe.Sizeof(ha)))
		ha.headers[0].size = zero

		m := meta{
			offset:    BigEndian(offset + int64(unsafe.Sizeof(ha))),
			seq:       zero,
			start:     zero,
			end:       BigEndian(METASIZE),
			effective: zero,
			next:      zero,
			previous:  zero,
		}

		_, err = u.file.Write((*(*[unsafe.Sizeof(ha)]byte)(unsafe.Pointer(&ha)))[:])
		if err != nil {
			return nil, err
		}

		// write meta to header tail
		_, err = u.file.Write((*(*[unsafe.Sizeof(m)]byte)(unsafe.Pointer(&m)))[:])
		if err != nil {
			return nil, err
		}

		u.headerList = append(u.headerList, &ha.headers[0])
		u.headerMap[sy] = &ha.headers[0]

		u.headerArea.next = BigEndian(offset)

		foo := BigEndian(offset)
		_, err = u.file.WriteAt(foo[:], UnBigEndian(u.headerArea.offset)+int64(unsafe.Offsetof(haMode.next)))
		if err != nil {
			return nil, err
		}

		var idleHeader []*header
		for k := range ha.headers[1:] {
			idleHeader = append(idleHeader, &ha.headers[k])
		}

		u.idleHeaders = append(u.idleHeaders, idleHeader...)

		return &file{
			u:       u,
			head:    &m,
			current: &m,
			cursor:  0,
		}, nil
	}
}

func (u *Unite) Remove(name string) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	sha := sha256.New()
	sha.Write([]byte(name))
	var symbol [8]byte
	copy(symbol[:], sha.Sum(nil)[:8])

	h, exist := u.headerMap[symbol]
	if !exist {
		return nil
	}

	h.fileSymbol = zero
	_, err := u.file.WriteAt(zero[:], UnBigEndian(h.offset)+int64(unsafe.Offsetof(hMode.fileSymbol)))
	if err != nil {
		return err
	}

	if h.originData != zero {

		var next [8]byte
		next = h.originData

		for {

			if next == zero {
				break
			}

			var m *meta
			b := make([]byte, unsafe.Offsetof(mMode.data))
			_, err = u.file.ReadAt(b, UnBigEndian(next))
			if err != nil {
				return err
			}

			m = (*meta)(unsafe.Pointer(&b[0]))
			m.seq = zero
			m.effective = zero
			m.start = zero
			next = m.next

			if u.idleMetas != nil {

				u.idleMetas.next = m.offset
				_, err = u.file.WriteAt(m.offset[:], UnBigEndian(u.idleMetas.offset)+int64(unsafe.Offsetof(mMode.next)))
				if err != nil {
					return err
				}

				m.next = u.idleMetas.offset
				m.previous = zero
			}

			_, err = u.file.WriteAt(b[:], UnBigEndian(m.offset))
			if err != nil {
				return err
			}

			u.idleMetas = m
		}
	}

	return nil
}

// FileList return all file name
func (u *Unite) FileList() []string {
	u.mu.Lock()
	defer u.mu.Unlock()

	var list []string

	for _, v := range u.headerMap {
		if v.fileSymbol != zero && v.fileSymbol != full {
			i := bytes.IndexByte(v.fileName[:], 0)
			list = append(list, string(v.fileName[:i]))
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i] < list[j]
	})

	return list
}

func (u *Unite) Close() error {
	return u.file.Close()
}

func (u *Unite) createNewMeta(previous *meta) (*meta, error) {
	//
	//u.mu.Lock()
	//defer u.mu.Unlock()

	var newMeta *meta

	if u.idleMetas != nil {

		m := u.idleMetas

		b := make([]byte, unsafe.Offsetof(mMode.data))

		if m.previous == zero {
			u.idleMetas = nil
		} else {

			_, err := u.file.ReadAt(b, UnBigEndian(m.previous))
			if err != nil {
				return nil, err
			}

			pre := (*meta)(unsafe.Pointer(&b[0]))

			pre.next = zero
			pre.seq = zero
			pre.start = zero
			pre.effective = zero

			_, err = u.file.WriteAt(pre.next[:], UnBigEndian(pre.offset)+int64(unsafe.Offsetof(mMode.next)))
			if err != nil {
				return nil, err
			}

			u.idleMetas = pre
		}

		return m, nil

	} else {

		// create new meta
		offset, err := u.file.Seek(0, 2)
		if err != nil {
			return nil, err
		}

		newMeta = &meta{
			offset:    BigEndian(offset),
			seq:       BigEndian(UnBigEndian(previous.seq) + 1),
			start:     zero,
			end:       BigEndian(METASIZE),
			effective: zero,
			next:      zero,
			previous:  previous.offset,
		}

		_, err = u.file.Write((*(*[unsafe.Offsetof(mMode.data)]byte)(unsafe.Pointer(newMeta)))[:])
		if err != nil {
			return nil, err
		}

		// populate meta data
		_, err = u.file.WriteAt([]byte{0}, UnBigEndian(newMeta.offset)+int64(unsafe.Offsetof(mMode.data))+int64(METASIZE))
		if err != nil {
			return nil, err
		}

		b := BigEndian(offset)
		if previous.offset != zero {
			_, err = u.file.WriteAt(b[:], UnBigEndian(previous.offset)+int64(unsafe.Offsetof(previous.next)))
			if err != nil {
				return nil, err
			}
		}

		previous.next = BigEndian(offset)
	}

	return newMeta, nil
}

func CreateUniteFile(path string) (*Unite, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	_, err = f.Write([]byte(MAGIC))
	if err != nil {
		return nil, err
	}

	originHeader := header{
		offset:     BigEndian(int64(len([]byte(MAGIC)))),
		fileSymbol: full,
		fileName:   [32]byte{},
		originData: zero,
		size:       zero,
	}

	ha := createHeaderArea(originHeader.offset, zero)
	ha.headers[0] = originHeader

	_, err = f.Write((*(*[unsafe.Sizeof(haMode)]byte)(unsafe.Pointer(ha)))[:unsafe.Sizeof(haMode)])
	if err != nil {
		return nil, err
	}

	var idleHeaders []*header

	for k := range ha.headers[1:] {
		idleHeaders = append(idleHeaders, &ha.headers[k])
	}

	return &Unite{
		file:        &syncFile{File: f},
		headerMap:   map[[8]byte]*header{full: &originHeader},
		headerList:  []*header{&originHeader},
		headerArea:  ha,
		idleHeaders: idleHeaders,
	}, nil
}

func OpenUniteFile(path string) (*Unite, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	magic := make([]byte, 4)

	_, err = io.ReadFull(f, magic)
	if err != nil {
		return nil, err
	}

	if string(magic) != MAGIC {
		return nil, FileTypeError
	}

	headerAreaSize := unsafe.Sizeof(headerArea{})
	haR := make([]byte, headerAreaSize)

	_, err = io.ReadFull(f, haR)
	if err != nil {
		return nil, err
	}

	var idleHeader []*header
	var hList []*header
	var m *meta
	hMap := map[[8]byte]*header{}

	for {

		ha := *(*headerArea)(unsafe.Pointer(&haR[0]))

		for k := range ha.headers {
			h := ha.headers[k]

			hList = append(hList, &h)

			if h.fileSymbol == zero {
				idleHeader = append(idleHeader, &h)
			} else {
				hMap[h.fileSymbol] = &h

				if h.fileSymbol == full {

					next := h.originData
					if next != zero {
						b := make([]byte, unsafe.Offsetof(mMode.data))
						_, err = f.ReadAt(b, UnBigEndian(next))
						if err != nil {
							return nil, err
						}

						m = (*meta)(unsafe.Pointer(&b[0]))
					}
				}
			}
		}

		if ha.next == zero {
			break
		} else {
			_, err = f.ReadAt(haR, UnBigEndian(ha.next))
			if err != nil {
				return nil, err
			}
		}
	}

	return &Unite{
		file:        &syncFile{File: f},
		headerMap:   hMap,
		headerList:  hList,
		idleMetas:   m,
		idleHeaders: idleHeader,
	}, nil
}
