package tool

import (
	"strconv"
	"strings"
)

func String2Int64(str string) int64 {
	i, _ := strconv.ParseInt(str, 10, 64)
	return i
}

func String2Int(s string) (int, error) {
	return strconv.Atoi(s)
}

func StringToInt(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func StringToUint(s string) uint {
	n, _ := strconv.Atoi(s)
	return uint(n)
}

func String2Float64(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

func StringToFloat64(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

func GetBytes(bytes interface{}) []byte {
	bs, _ := bytes.([]byte)
	return bs
}

func Bytes2String(bytes interface{}) string {
	return string(GetBytes(bytes))
}

func String2Bytes(s string) []byte {
	return []byte(s)
}

func Bytes2Float64(bytes interface{}) (float64, error) {
	return String2Float64(Bytes2String(bytes))
}

func Bytes2Int(bytes interface{}) (int, error) {
	return String2Int(Bytes2String(bytes))
}

func GetStringUnique(ss string, glue string) (ns string) {
	ms := map[string]struct{}{}
	ls := strings.Split(ss, glue)
	for _, s := range ls {
		if _, ok := ms[s]; ok {
			continue
		}
		ms[s] = struct{}{}
		if ns == "" {
			ns = s
		} else {
			ns += glue + s
		}
	}
	return ns
}

func Intval(s string) int {
	return StringToInt(s)
}

func Intval2(s string) string {
	return Int2String(Intval(s))
}

func IsEmpty(s string) bool {
	return s == ""
}

func IsZero(n string) bool {
	return n == "0"
}

func IsNumeric(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}
