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
	offset   [8]byte
	previous [8]byte
	next     [8]byte
	headers  [1024]header
}

func createHeaderArea(offset, previous [8]byte) *headerArea {
	var ha headerArea

	ha.offset = offset
	ha.previous = previous

	size := int64(unsafe.Sizeof(hMode))
	for k := range ha.headers {
		ha.headers[k].offset = BigEndian(UnBigEndian(offset) +
			int64(unsafe.Offsetof(haMode.headers)) + int64(k)*size)
	}

	return &ha
}

var hMode header
var mMode meta
var haMode headerArea

var originMeta = meta{
	offset: BigEndian(0),
	seq:    BigEndian(-1),
	//start:     BigEndian(0),
	//end:       BigEndian(0),
	metaSize: BigEndian(0),
	next:     BigEndian(0),
	previous: BigEndian(0),
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
	offset [8]byte
	seq    [8]byte
	//start     [8]byte // meta.offset-based offset
	//end       [8]byte // meta.offset-based offset
	metaSize [8]byte
	next     [8]byte
	previous [8]byte

	dataOffset [8]byte
	dataSize   [8]byte
}

func (m *meta) bytes() [unsafe.Sizeof(mMode)]byte {
	var b [unsafe.Sizeof(mMode)]byte
	a := *((*[unsafe.Sizeof(mMode)]byte)(unsafe.Pointer(m)))
	copy(b[:], a[:])
	return b
}

type file struct {
	header      *header
	headMeta    *meta
	currentMeta *meta
	cursor      int64
	metaCursor  int64
	u           *Unite
}

func (f *file) Write(b []byte) (n int, err error) {

	f.u.mu.Lock()
	defer f.u.mu.Unlock()

	meta := f.currentMeta

	if meta == nil {
		meta, err = f.u.createNewMeta(&originMeta, int64(len(b)))
		if err != nil {
			return 0, err
		}

		f.header.originData = BigEndian(UnBigEndian(meta.offset))

		_, err = f.u.file.WriteAt(f.header.originData[:], UnBigEndian(f.header.offset)+int64(unsafe.Offsetof(hMode.originData)))
		if err != nil {
			return 0, err
		}

		f.headMeta = meta
		f.currentMeta = meta
	}

	for {

		var endOffset int64

		endOffset, err = f.u.file.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, err
		}

		if endOffset == UnBigEndian(meta.dataOffset)+UnBigEndian(meta.dataSize) || meta.dataOffset == zero {

			n, err = f.u.file.WriteAt(b, endOffset)
			if err != nil {
				return 0, err
			}

			meta.dataSize = BigEndian(UnBigEndian(meta.dataSize) + int64(n))
			meta.metaSize = BigEndian(UnBigEndian(meta.metaSize) + int64(n))

			if meta.dataOffset == zero {
				meta.dataOffset = BigEndian(endOffset)
			}

			_b := meta.bytes()
			_, err = f.u.file.WriteAt(_b[:], UnBigEndian(meta.offset))
			if err != nil {
				return 0, err
			}

			return
		}

		if UnBigEndian(meta.metaSize)-UnBigEndian(meta.dataSize) >= int64(len(b)) {

			n, err = f.u.file.WriteAt(b, UnBigEndian(meta.dataOffset)+UnBigEndian(meta.dataSize))
			if err != nil {
				return 0, err
			}

			meta.dataSize = BigEndian(UnBigEndian(meta.dataSize) + int64(n))

			_, err = f.u.file.WriteAt(meta.dataSize[:], UnBigEndian(meta.dataOffset)+int64(unsafe.Offsetof(mMode.dataSize)))
			if err != nil {
				return 0, err
			}

			return
		} else {

			metaRemain := UnBigEndian(meta.metaSize) - UnBigEndian(meta.dataSize)

			cutB := b[:metaRemain]

			var nn int
			nn, err = f.u.file.WriteAt(cutB, UnBigEndian(meta.dataOffset)+UnBigEndian(meta.dataSize))
			if err != nil {
				return 0, err
			}

			meta.dataSize = BigEndian(UnBigEndian(meta.dataSize) + int64(nn))

			_, err = f.u.file.WriteAt(meta.dataSize[:], UnBigEndian(meta.dataOffset)+int64(unsafe.Offsetof(mMode.dataSize)))
			if err != nil {
				return 0, err
			}

			n += nn

			b = b[nn:]

			newMeta, err := f.u.createNewMeta(meta, int64(len(b)))
			if err != nil {
				return 0, err
			}

			f.currentMeta = newMeta
			meta = newMeta
			continue
		}
	}
}

func (f *file) Read(b []byte) (n int, err error) {

	if f.currentMeta == nil {
		return 0, io.EOF
	}

	for {

		currentMetaOffset := f.metaCursor

		metaRemain := UnBigEndian(f.currentMeta.dataSize) - currentMetaOffset

		var nn int
		var err error
		if metaRemain >= int64(len(b)) {
			nn, err = f.u.file.ReadAt(b, UnBigEndian(f.currentMeta.dataOffset)+currentMetaOffset)
			if err != nil {
				return n, err
			}
		} else {
			nn, err = f.u.file.ReadAt(b[:metaRemain], UnBigEndian(f.currentMeta.dataOffset)+currentMetaOffset)
			if err != nil {
				return n, err
			}
		}

		b = b[nn:]
		f.cursor += int64(nn)
		f.metaCursor += int64(nn)
		n += nn

		if len(b) != 0 {
			f.metaCursor = 0
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

	if UnBigEndian(f.currentMeta.next) == 0 {
		return io.EOF
	}

	// find next meta
	var nextMeta *meta
	b := make([]byte, unsafe.Sizeof(mMode))
	_, err := f.u.file.ReadAt(b, UnBigEndian(f.currentMeta.next))
	if err != nil {
		return err
	}
	nextMeta = (*meta)(unsafe.Pointer(&b[0]))
	f.currentMeta = nextMeta
	return nil
}

type Unite struct {
	mu          sync.RWMutex
	file        *syncFile
	headerArea  *headerArea
	headerMap   map[[8]byte]*header
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

	var hMeta *meta
	if h.originData != zero {
		b := make([]byte, unsafe.Sizeof(mMode))
		_, err := u.file.ReadAt(b, UnBigEndian(h.originData))
		if err != nil {
			return nil, err
		}
		hMeta = (*meta)(unsafe.Pointer(&b[0]))
	}

	return &file{
		header:      h,
		headMeta:    hMeta,
		currentMeta: hMeta,
		cursor:      0,
		u:           u,
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

		//// create new meta
		//meta, err := u.createNewMeta(&originMeta)
		//if err != nil {
		//	return nil, err
		//}
		//
		//h.originData = meta.offset
		//_, err = u.file.WriteAt(h.originData[:], UnBigEndian(h.offset)+int64(unsafe.Offsetof(h.originData)))
		//if err != nil {
		//	return nil, err
		//}

		u.headerMap[sy] = h
		u.idleHeaders = u.idleHeaders[1:]

		return &file{
			header:      h,
			u:           u,
			headMeta:    nil,
			currentMeta: nil,
			cursor:      0,
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

		//m := meta{
		//	offset:    BigEndian(offset + int64(unsafe.Sizeof(ha))),
		//	seq:       zero,
		//	start:     zero,
		//	end:       BigEndian(METASIZE),
		//	metaSize: zero,
		//	next:      zero,
		//	previous:  zero,
		//}

		_, err = u.file.Write((*(*[unsafe.Sizeof(haMode)]byte)(unsafe.Pointer(ha)))[:])
		if err != nil {
			return nil, err
		}

		// write meta to header tail
		//_, err = u.file.Write((*(*[unsafe.Sizeof(mMode)]byte)(unsafe.Pointer(&m)))[:])
		//if err != nil {
		//	return nil, err
		//}

		u.headerMap[sy] = &ha.headers[0]

		u.headerArea.next = BigEndian(offset)

		_, err = u.file.WriteAt(ha.offset[:], UnBigEndian(u.headerArea.offset)+
			int64(unsafe.Offsetof(haMode.next)))
		if err != nil {
			return nil, err
		}

		u.headerArea = ha

		var idleHeader []*header
		for k := range ha.headers {
			if k == 0 {
				continue
			}
			idleHeader = append(idleHeader, &ha.headers[k])
		}

		u.idleHeaders = append(u.idleHeaders, idleHeader...)

		return &file{
			header:      &ha.headers[0],
			u:           u,
			headMeta:    nil,
			currentMeta: nil,
			cursor:      0,
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
			b := make([]byte, unsafe.Sizeof(mMode))
			_, err = u.file.ReadAt(b, UnBigEndian(next))
			if err != nil {
				return err
			}

			m = (*meta)(unsafe.Pointer(&b[0]))
			m.seq = zero
			m.dataSize = zero
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

func (u *Unite) createNewMeta(previous *meta, dataSize int64) (*meta, error) {

	var newMeta *meta

	if u.idleMetas != nil {

		first := u.idleMetas
		current := first

		b := make([]byte, unsafe.Sizeof(mMode))

		for {

			var pre *meta

			if current.previous != zero {
				_, err := u.file.ReadAt(b, UnBigEndian(current.previous))
				if err != nil {
					return nil, err
				}

				pre = (*meta)(unsafe.Pointer(&b[0]))
			}

			if UnBigEndian(current.metaSize) >= dataSize {

				if current != first {
					pre.next = current.next
					_, err := u.file.WriteAt(pre.next[:], UnBigEndian(pre.offset)+int64(unsafe.Offsetof(mMode.next)))
					if err != nil {
						return nil, err
					}
					u.idleMetas = first

				} else {
					u.idleMetas = pre
				}

				current.next = zero
				current.previous = zero
				current.seq = zero
				current.dataSize = zero

				_b := current.bytes()

				_, err := u.file.WriteAt(_b[:], UnBigEndian(current.offset))
				if err != nil {
					return nil, err
				}

				return current, nil

			} else {
				if pre == nil {
					break
				}
				current = pre
			}
		}
	}

	// create new meta
	offset, err := u.file.Seek(0, 2)
	if err != nil {
		return nil, err
	}

	newMeta = &meta{
		offset:   BigEndian(offset),
		seq:      BigEndian(UnBigEndian(previous.seq) + 1),
		metaSize: zero,
		next:     zero,
		previous: previous.offset,
		//dataSize:  BigEndian(dataSize),
	}

	previous.next = BigEndian(offset)

	if previous.offset != zero {
		_, err = u.file.WriteAt(previous.next[:], UnBigEndian(previous.offset)+int64(unsafe.Offsetof(previous.next)))
		if err != nil {
			return nil, err
		}
	}
	_b := newMeta.bytes()
	_, err = u.file.WriteAt(_b[:], UnBigEndian(newMeta.offset))
	if err != nil {
		return nil, err
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

	ha := createHeaderArea(BigEndian(int64(len([]byte(MAGIC)))), zero)
	ha.headers[0].fileSymbol = full
	ha.headers[0].fileName = [32]byte{}
	ha.headers[0].offset = zero
	ha.headers[0].size = zero

	_, err = f.Write((*(*[unsafe.Sizeof(haMode)]byte)(unsafe.Pointer(ha)))[:unsafe.Sizeof(haMode)])
	if err != nil {
		return nil, err
	}

	var idleHeaders []*header

	for k := range ha.headers {
		if k == 0 {
			continue
		}
		idleHeaders = append(idleHeaders, &ha.headers[k])
	}

	return &Unite{
		file:        &syncFile{File: f},
		headerMap:   map[[8]byte]*header{full: &ha.headers[0]},
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
	var m *meta
	hMap := map[[8]byte]*header{}

	var ha headerArea
	for {

		ha = *(*headerArea)(unsafe.Pointer(&haR[0]))

		for k := range ha.headers {
			h := ha.headers[k]

			if h.fileSymbol == zero {
				idleHeader = append(idleHeader, &h)
			} else {
				hMap[h.fileSymbol] = &h

				if h.fileSymbol == full {

					next := h.originData
					if next != zero {
						b := make([]byte, unsafe.Sizeof(mMode))
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
		headerArea:  &ha,
		idleMetas:   m,
		idleHeaders: idleHeader,
	}, nil
}
