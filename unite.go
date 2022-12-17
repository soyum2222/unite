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
	end        bool
	fileName   [32]byte
	fileSymbol [8]byte
	createTime [8]byte
	modifyTime [8]byte
	seq        [8]byte
	offset     [8]byte
	size       [8]byte
	next       [8]byte
	previous   [8]byte
	originData [8]byte
}

var hMode header
var mMode meta

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
		meta, err := f.u.createNewMeta(&mMode)
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

				meta, err := f.u.createNewMeta(f.current)
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
			meta, err := f.u.createNewMeta(f.current)
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

		return &file{
			u:       u,
			head:    nil,
			current: nil,
			cursor:  0,
		}, err
	}

	tail := u.headerList[len(u.headerList)-1]

	if tail.end {

		offset, err := u.file.Seek(0, 2)
		if err != nil {
			return nil, err
		}

		h := header{
			offset:     BigEndian(offset),
			seq:        BigEndian(UnBigEndian(tail.seq) + 1),
			fileSymbol: sy,
			fileName:   nameBytes,
			originData: BigEndian(offset + int64(unsafe.Sizeof(hMode))),
			size:       zero,
			next:       zero,
			previous:   tail.offset,
			end:        true,
		}

		m := meta{
			offset:    BigEndian(offset + int64(unsafe.Sizeof(hMode))),
			seq:       zero,
			start:     zero,
			end:       BigEndian(METASIZE),
			effective: zero,
			next:      zero,
			previous:  zero,
		}

		_, err = u.file.Write((*(*[unsafe.Sizeof(h)]byte)(unsafe.Pointer(&h)))[:])
		if err != nil {
			return nil, err
		}

		// write meta to header tail
		_, err = u.file.Write((*(*[unsafe.Sizeof(m)]byte)(unsafe.Pointer(&m)))[:])
		if err != nil {
			return nil, err
		}

		// update tail
		_, err = u.file.WriteAt([]byte{0}, UnBigEndian(tail.offset)+int64(unsafe.Offsetof(hMode.end)))
		if err != nil {
			return nil, err
		}

		_, err = u.file.WriteAt(h.offset[:], UnBigEndian(tail.offset)+int64(unsafe.Offsetof(hMode.next)))
		if err != nil {
			return nil, err
		}

		u.headerList = append(u.headerList, &h)
		u.headerMap[sy] = &h

		return &file{
			u:       u,
			head:    &m,
			current: &m,
			cursor:  0,
		}, nil

	} else {
		return nil, errors.New("unknown error")
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

			next = m.next

			if u.idleMetas != nil {

				u.idleMetas.next = m.offset
				_, err = u.file.WriteAt(m.offset[:], UnBigEndian(u.idleMetas.previous)+int64(unsafe.Offsetof(mMode.next)))
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

	u.mu.Lock()
	defer u.mu.Unlock()

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

			previous := (*meta)(unsafe.Pointer(&b[0]))

			previous.next = zero
			_, err = u.file.WriteAt(previous.next[:], UnBigEndian(previous.offset)+int64(unsafe.Offsetof(mMode.next)))
			if err != nil {
				return nil, err
			}

			u.idleMetas = previous
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

		b := BigEndian(offset)
		_, err = u.file.WriteAt(b[:], UnBigEndian(previous.offset)+int64(unsafe.Offsetof(previous.next)))
		if err != nil {
			return nil, err
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
		seq:        zero,
		fileSymbol: full,
		fileName:   [32]byte{},
		originData: zero,
		size:       zero,
		next:       zero,
		previous:   zero,
		end:        true,
	}

	_, err = f.Write((*(*[unsafe.Sizeof(hMode)]byte)(unsafe.Pointer(&originHeader)))[:unsafe.Sizeof(hMode)])
	if err != nil {
		return nil, err
	}

	return &Unite{
		file:       &syncFile{File: f},
		headerMap:  map[[8]byte]*header{full: &originHeader},
		headerList: []*header{&originHeader},
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

	headerSize := unsafe.Sizeof(hMode)
	hd := make([]byte, headerSize)

	_, err = io.ReadFull(f, hd)
	if err != nil {
		return nil, err
	}

	var idleHeader []*header
	var hList []*header
	var m *meta
	hMap := map[[8]byte]*header{}

	for {
		h := *(*header)(unsafe.Pointer(&hd[0]))

		hList = append(hList, &h)

		if h.fileSymbol == zero {
			idleHeader = append(idleHeader, &h)
		} else {
			hMap[h.fileSymbol] = &h

			if h.fileSymbol == full {

				next := h.originData
				if next != zero {
					b := make([]byte, unsafe.Offsetof(mMode.data))
					_, err = f.ReadAt(hd, UnBigEndian(next))
					if err != nil {
						return nil, err
					}

					m = (*meta)(unsafe.Pointer(&b[0]))
				}
			}
		}

		if h.end {
			break
		}

		n, err := f.ReadAt(hd, UnBigEndian(h.next))
		if err != nil {
			return nil, err
		}

		if n != int(headerSize) {
			return nil, DamagedError
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
