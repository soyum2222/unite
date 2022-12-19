package unite

import (
	"os"
	"sync"
)

type syncFile struct {
	sync.Mutex
	*os.File
}

func (f *syncFile) Write(b []byte) (n int, err error) {
	f.Lock()
	defer f.Unlock()
	return f.File.Write(b)
}

func (f *syncFile) Read(b []byte) (n int, err error) {
	f.Lock()
	defer f.Unlock()
	return f.File.Read(b)
}

func (f *syncFile) WriteAt(b []byte, off int64) (n int, err error) {
	f.Lock()
	defer f.Unlock()
	return f.File.WriteAt(b, off)
}

func (f *syncFile) ReadAt(b []byte, off int64) (n int, err error) {
	f.Lock()
	defer f.Unlock()
	return f.File.ReadAt(b, off)
}

func (f *syncFile) Close() error {
	f.Lock()
	defer f.Unlock()
	return f.File.Close()
}
