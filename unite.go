package unite

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"unsafe"
)

const (
	METASIZE = 1024 * 1024 * 4
	//MAXMETA  = 1024
	MAGIC = "unit"
)

var (
	NotFindFileError = errors.New("not find file")
	FileTypeError    = errors.New(" the file is not a unite file")
	DamagedError     = errors.New("damaged file")
	AlreadyExists    = errors.New("file already exists")
)

type header struct {
	end        bool
	seq        [8]byte
	offset     [8]byte
	fileSymbol [8]byte
	size       [8]byte
	next       [8]byte
	previous   [8]byte
	originData [8]byte
}

var hMode header
var mMode meta

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

type uniteFile struct {
	mu      sync.RWMutex
	file    *os.File
	headers []header
}

type file struct {
	file    *os.File
	head    *meta
	current *meta
	cursor  int64
}

func (f *file) Write(b []byte) (n int, err error) {

	if UnBigEndian(f.current.effective) == UnBigEndian(f.current.end)-UnBigEndian(f.current.start) {
		// meta full
		for {
			if UnBigEndian(f.current.next) == 0 {

				err = f.createNewMeta()
				if err != nil {
					return 0, err
				}
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
		bb := make([]byte, metaRemain)
		nn := copy(bb, b)
		n += nn
		b = b[nn:]
		_, err = f.file.WriteAt(bb[:nn], offset)
		if err != nil {
			return 0, err
		}

		f.current.effective = BigEndian(UnBigEndian(f.current.effective) + int64(nn))

		// update meta.effective
		_, err = f.file.WriteAt(f.current.effective[:],
			UnBigEndian(f.current.offset)+int64(unsafe.Offsetof(f.current.effective)))
		if err != nil {
			return 0, err
		}

		if len(b) == 0 {
			return n, nil
		} else {
			err = f.createNewMeta()
			if err != nil {
				return 0, err
			}
		}
	}
}

func (f *file) createNewMeta() error {

	// create new meta
	offset, err := f.file.Seek(0, 2)
	if err != nil {
		return err
	}

	newMeta := meta{
		offset:    BigEndian(offset),
		seq:       BigEndian(UnBigEndian(f.current.seq) + 1),
		start:     zero,
		end:       BigEndian(METASIZE),
		effective: zero,
		next:      zero,
		previous:  f.current.offset,
	}

	_, err = f.file.Write((*(*[unsafe.Sizeof(newMeta)]byte)(unsafe.Pointer(&newMeta)))[:])
	if err != nil {
		return err
	}

	b := BigEndian(offset)
	_, err = f.file.WriteAt(b[:], UnBigEndian(f.current.offset)+int64(unsafe.Offsetof(f.current.next)))
	if err != nil {
		return err
	}

	f.current.next = BigEndian(offset)

	f.current = &newMeta

	return nil
}

func (f *file) nextMeta() error {

	if UnBigEndian(f.current.next) == 0 {
		return io.EOF
	}

	// find next meta
	var nextMeta *meta
	b := make([]byte, unsafe.Sizeof(mMode))
	_, err := f.file.ReadAt(b, UnBigEndian(f.current.next))
	if err != nil {
		return err
	}
	nextMeta = (*meta)(unsafe.Pointer(&b[0]))
	f.current = nextMeta
	return nil
}

func (f *file) Read(b []byte) (n int, err error) {

	for {

		beginAddr := UnBigEndian(f.current.seq) * METASIZE
		currentMetaOffset := f.cursor - beginAddr
		offset := UnBigEndian(f.current.offset)

		metaRemain := UnBigEndian(f.current.effective) - currentMetaOffset

		var bb []byte
		if metaRemain > int64(len(b)) {
			bb = make([]byte, len(b))
		} else {
			bb = make([]byte, metaRemain)
		}

		nn, err := f.file.ReadAt(bb, offset+int64(unsafe.Offsetof(f.current.data))+currentMetaOffset)
		if err != nil {
			return nn, err
		}

		copy(b, bb)
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

var zero = BigEndian(0)

func BigEndian(i int64) [8]byte {
	b := [8]byte{}
	binary.BigEndian.PutUint64(b[:], uint64(i))
	return b
}

func UnBigEndian(b [8]byte) int64 {
	return int64(binary.BigEndian.Uint64(b[:]))
}

func (u *uniteFile) Open(name string) (*file, error) {

	u.mu.RLock()
	defer u.mu.RUnlock()

	sha := sha256.New()
	sha.Write([]byte(name))
	symbol := sha.Sum(nil)[:8]

	for _, v := range u.headers {
		if bytes.Compare(v.fileSymbol[:], symbol) == 0 {

			var hMeta meta
			b := make([]byte, unsafe.Sizeof(mMode))
			_, err := u.file.ReadAt(b, UnBigEndian(v.originData))
			if err != nil {
				return nil, err
			}
			hMeta = *(*meta)(unsafe.Pointer(&b[0]))

			return &file{
				file:    u.file,
				head:    &hMeta,
				current: &hMeta,
				cursor:  0,
			}, nil
		}
	}

	return nil, NotFindFileError
}

func (u *uniteFile) Create(name string) (*file, error) {

	u.mu.Lock()
	defer u.mu.Unlock()

	sha := sha256.New()
	sha.Write([]byte(name))
	symbol := sha.Sum(nil)[:8]

	for _, v := range u.headers {
		if bytes.Compare(v.fileSymbol[:], symbol) == 0 {
			return nil, AlreadyExists
		}
	}

	tail := u.headers[len(u.headers)-1]

	if tail.end {

		offset, err := u.file.Seek(0, 2)
		if err != nil {
			return nil, err
		}

		sy := [8]byte{}
		copy(sy[:], symbol)
		h := header{
			offset:     BigEndian(offset),
			seq:        BigEndian(UnBigEndian(tail.seq) + 1),
			fileSymbol: sy,
			originData: BigEndian(offset + int64(unsafe.Sizeof(hMode))),
			size:       zero,
			next:       zero,
			previous:   zero,
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

		u.headers = append(u.headers, h)

		return &file{
			file:    u.file,
			head:    &m,
			current: &m,
			cursor:  0,
		}, nil

	} else {
		return nil, errors.New("unknown error")
	}
}

func (u *uniteFile) Close() error {
	return u.file.Close()
}

func CreateUniteFile(path string) (*uniteFile, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	_, err = f.Write([]byte(MAGIC))
	if err != nil {
		return nil, err
	}

	h := header{
		offset:     BigEndian(int64(len([]byte(MAGIC)))),
		seq:        zero,
		fileSymbol: [8]byte{},
		originData: zero,
		size:       zero,
		next:       zero,
		previous:   zero,
		end:        true,
	}

	_, err = f.Write((*(*[unsafe.Sizeof(h)]byte)(unsafe.Pointer(&h)))[:unsafe.Sizeof(hMode)])
	if err != nil {
		return nil, err
	}

	return &uniteFile{
		headers: append([]header{}, h),
		file:    f,
	}, nil
}

func OpenUniteFile(path string) (*uniteFile, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0666)
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

	var hs []header
	for {
		h := *(*header)(unsafe.Pointer(&hd[0]))
		hs = append(hs, h)
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

	return &uniteFile{
		file:    f,
		headers: hs,
	}, nil
}
