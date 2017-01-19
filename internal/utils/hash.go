package utils

const (
	HUSTDB_TABLE_SIZE = 1024
)

func LocateHashRegion(key string) int {
	return NgxHashKey(key) % 1024
}

func NgxHashKey(key string) int {
	val := 0
	for _, c := range []byte(key) {
		val = ngxHash(val, c)
	}
	return val
}

func ngxHash(key int, c byte) int {
	return key + int(c)
}
