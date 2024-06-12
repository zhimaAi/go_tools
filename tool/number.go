package tool

import "strconv"

func U64ToString(i uint64) string {
	return strconv.FormatUint(i, 10)
}

func U32ToString(u32 uint32) string {
	return U64ToString(uint64(u32))
}

func Int64ToString(i int64) string {
	return strconv.FormatInt(i, 10)
}

func Int2String(i int) string {
	return strconv.Itoa(i)
}

func Float64ToString(v float64) string {
	return strconv.FormatFloat(v, 'E', -1, 64)
}

func Float32ToString(v float32) string {
	return strconv.FormatFloat(float64(v), 'E', -1, 32)
}

func Uint2String(i uint) string {
	return Int2String(int(i))
}

func U8ToString(i uint8) string {
	return Int2String(int(i))
}
