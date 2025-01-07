package tool

import (
	"strings"
)

func String2IntArray(ids string) []int {
	if ids == "" {
		return make([]int, 0)
	}
	sl := strings.Split(ids, ",")
	arr := make([]int, len(sl))
	for k, v := range sl {
		arr[k] = StringToInt(v)
	}
	return arr
}

func InArray[Val comparable](a Val, b []Val) bool {
	for i := range b {
		if b[i] == a {
			return true
		}
	}
	return false
}

func InArrayInt(n int, h []int) bool {
	for _, v := range h {
		if v == n {
			return true
		}
	}
	return false
}

func InArrayString(n string, h []string) bool {
	for _, v := range h {
		if v == n {
			return true
		}
	}
	return false
}

func Array2String(sl []string, glue string) (ss string) {
	return strings.Join(sl, glue)
}
