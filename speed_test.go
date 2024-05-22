package unite

import (
	"crypto/rand"
	"io"
	"testing"
	"time"
)

func writeSpeedStatistic(writer io.Writer, size int64) (int64, error) {

	b := make([]byte, size)

	_, err := rand.Read(b)
	if err != nil {
		return 0, err
	}

	start := time.Now()

	_, err = writer.Write(b)
	if err != nil {
		return 0, err
	}

	end := time.Now()

	cost := end.UnixNano() - start.UnixNano()

	return cost, nil
}

func TestCrossSpeedStatistic(t *testing.T) {
	unite, err := CreateUniteFile("./test.unite")
	if err != nil {
		panic(err)
	}

	totalCost := int64(0)
	size := int64(1 << 10)
	totalSize := int64(1 << 30)

	count := totalSize / size

	var files [2]*file

	file1, err := unite.Create("test1")
	if err != nil {
		panic(err)
	}

	file2, err := unite.Create("test2")
	if err != nil {
		panic(err)
	}

	files[0] = file1
	files[1] = file2

	for i := int64(0); i < count*2; i++ {
		cost, err := writeSpeedStatistic(files[i%2], size)
		if err != nil {
			panic(err)
		}
		totalCost += cost
	}

	t.Logf("write speed: %f MB/s write count %d ", float64(totalSize*2)/(float64(totalCost)/1e9)/1e6, count)
}

func TestSpeed(t *testing.T) {

	unite, err := CreateUniteFile("./test.unite")
	if err != nil {
		panic(err)
	}

	file, err := unite.Create("speed.test")
	if err != nil {
		panic(err)
	}

	//file, err := os.Create("speed.test")
	//if err != nil {
	//	panic(err)
	//}

	totalCost := int64(0)
	size := int64(1 << 10)
	totalSize := int64(1 << 30)

	count := totalSize / size

	for i := int64(0); i < count; i++ {
		cost, err := writeSpeedStatistic(file, size)
		if err != nil {
			panic(err)
		}
		totalCost += cost
	}

	t.Logf("write speed: %f MB/s write count %d ", float64(totalSize)/(float64(totalCost)/1e9)/1e6, count)
}
