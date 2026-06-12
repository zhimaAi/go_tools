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

func InArray[T comparable](needle T, haystack []T) bool {
	for idx := range haystack {
		if haystack[idx] == needle {
			return true
		}
	}
	return false
}

func InArrayInt(needle int, haystack []int) bool {
	return InArray(needle, haystack)
}

func InArrayString(needle string, haystack []string) bool {
	return InArray(needle, haystack)
}

func Array2String(sl []string, glue string) (ss string) {
	return strings.Join(sl, glue)
}
