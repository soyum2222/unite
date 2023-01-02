package unite

import (
	"fmt"
	"io"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestCreateUniteFile(t *testing.T) {

	file, err := CreateUniteFile("./test.unite")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	_ = file.Close()
}

func TestOpenUniteFile(t *testing.T) {
	file, err := OpenUniteFile("./test.unite")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	_ = file.Close()
}

func TestCreateFile(t *testing.T) {

	unite, err := CreateUniteFile("./test.unite")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	f, err := unite.Create("unite.txt")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	_ = f.Close()
	return
}

func TestOpenFile(t *testing.T) {

	TestCreateUniteFile(t)
	TestCreateFile(t)

	unite, err := OpenUniteFile("./test.unite")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	file, err := unite.Open("unite.txt")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	_ = file.Close()
}

func TestFile_Write(t *testing.T) {

	TestCreateUniteFile(t)

	unite, err := OpenUniteFile("./test.unite")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	file, err := unite.Create("unite.txt")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	_, err = file.Write([]byte("hello world"))
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	_ = file.Close()
}

func TestFile_Read(t *testing.T) {

	TestFile_Write(t)

	unite, err := OpenUniteFile("./test.unite")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	file, err := unite.Open("unite.txt")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	b := make([]byte, 1024)

	n, err := file.Read(b)
	if err != nil && err != io.EOF {
		fmt.Println(err)
		t.Fail()
		return
	}

	if string(b[:n]) != "hello world" {
		fmt.Println("read error")
		t.Fail()
		return
	}
}

func TestRemove(t *testing.T) {

	TestCreateUniteFile(t)

	TestFile_Write(t)

	unite, err := OpenUniteFile("./test.unite")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	err = unite.Remove("unite.txt")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	file, err := unite.Create("foo.txt")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	b := make([]byte, 1<<30)

	for k := range b {
		b[k] = byte(k)
	}

	_, err = file.Write(b)
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	err = unite.Remove("foo.txt")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	_ = unite.Close()

	unite, err = OpenUniteFile("./test.unite")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	file, err = unite.Create("foo.txt")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	_ = unite.Close()
}

func TestWriteHuge(t *testing.T) {

	TestCreateUniteFile(t)

	unite, err := OpenUniteFile("./test.unite")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	file, err := unite.Create("unite.data")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	b := make([]byte, 1<<30)

	for k := range b {
		b[k] = byte(k)
	}

	n, err := file.Write(b)
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	if n != 1<<30 {
		fmt.Println("write error")
		t.Fail()
		return
	}

	_ = unite.Close()
}

func TestReadHuge(t *testing.T) {

	TestWriteHuge(t)

	unite, err := OpenUniteFile("./test.unite")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	file, err := unite.Open("unite.data")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	b := make([]byte, 1<<30)

	n, err := file.Read(b)
	if err != nil && err != io.EOF {
		fmt.Println(err)
		t.Fail()
		return
	}

	if n != 1<<30 {
		fmt.Println("read error")
		t.Fail()
		return
	}

	for k := range b {
		if b[k] != byte(k) {
			fmt.Println("read error")
			t.Fail()
			return
		}
	}

	_ = file.Close()
}

func TestWriteMultiple(t *testing.T) {

	TestCreateUniteFile(t)

	unite, err := OpenUniteFile("./test.unite")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	file, err := unite.Create("unite.data")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	_, err = file.Write([]byte("hello"))
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	_, err = file.Write([]byte(" "))
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	_, err = file.Write([]byte("world"))
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	_ = unite.Close()
}

func TestReadMultiple(t *testing.T) {

	TestWriteMultiple(t)

	unite, err := OpenUniteFile("./test.unite")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	file, err := unite.Open("unite.data")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	b := make([]byte, 5)
	n, err := file.Read(b)
	if err != nil && err != io.EOF {
		fmt.Println(err)
		t.Fail()
		return
	}

	if string(b[:n]) != "hello" {
		fmt.Println("read error")
		t.Fail()
		return
	}

	b = make([]byte, 1)
	n, err = file.Read(b)
	if err != nil && err != io.EOF {
		fmt.Println(err)
		t.Fail()
		return
	}

	if string(b[:n]) != " " {
		fmt.Println("read error")
		t.Fail()
		return
	}

	b = make([]byte, 5)
	n, err = file.Read(b)
	if err != nil && err != io.EOF {
		fmt.Println(err)
		t.Fail()
		return
	}

	if string(b[:n]) != "world" {
		fmt.Println("read error")
		t.Fail()
		return
	}

	_ = file.Close()
}

func TestGetFileName(t *testing.T) {

	TestCreateUniteFile(t)

	unite, err := OpenUniteFile("./test.unite")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	file, err := unite.Create("unite.data")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	err = file.Close()
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	file, err = unite.Create("unite1.data")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	err = file.Close()
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	list := unite.FileList()
	if len(list) != 2 {
		fmt.Println("file list error")
		t.Fail()
		return
	}

	if list[0] != "unite.data" {
		fmt.Println("file list error")
		t.Fail()
		return
	}

	if list[1] != "unite1.data" {
		fmt.Println("file list error")
		t.Fail()
		return
	}

	_ = unite.Close()
}

func TestMetaFull(t *testing.T) {

	TestCreateUniteFile(t)
	unite, err := OpenUniteFile("./test.unite")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	f, err := unite.Create("foo")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	b := make([]byte, METASIZE)

	_, err = f.Write(b)
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	_, err = f.Write([]byte("hello"))
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	_ = f.Close()
	_ = unite.Close()

	unite, err = OpenUniteFile("./test.unite")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	f, err = unite.Open("foo")
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	b = make([]byte, METASIZE+5)
	_, err = f.Read(b)
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	if string(b[METASIZE:]) != "hello" {
		fmt.Println("read error")
		t.Fail()
		return
	}

	_ = unite.Close()
}

func TestCreateHugeFile(t *testing.T) {

	u, err := CreateUniteFile("./test.unite")
	if err != nil {
		t.Fail()
		panic(err)
	}

	for i := 0; i < 10000; i++ {

		// get a rand number
		rand.Seed(time.Now().UnixNano())
		r := rand.Intn(10000)
		b := make([]byte, r)

		f, err := u.Create(fmt.Sprintf("%d", i))
		if err != nil {
			t.Fail()
			panic(err)
		}

		_, err = f.Write(b)
		if err != nil {
			t.Fail()
			panic(err)
		}
	}

	_ = u.Close()

	u, err = OpenUniteFile("./test.unite")
	if err != nil {
		t.Fail()
		panic(err)
	}

	names := u.FileList()

	sort.Slice(names, func(i, j int) bool {
		return names[i] < names[j]
	})

	if len(names) != 10000 {
		t.Fail()
		return
	}
}
