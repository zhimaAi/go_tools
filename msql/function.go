package msql

import "strings"

// ToField 清理字段名或表名两侧的空白和引号。
func ToField(s string) string {
	return strings.Trim(strings.TrimSpace(s), "`'\"\t ")
}

// ToString 将字符串清理后格式化为 SQL 字符串字面量。
//
// 该函数会先复用 ToField 去掉首尾空白和包裹引号，再按 SQL 字符串字面量规则包裹单引号并转义内部单引号。
func ToString(s string) string {
	return quoteSQLValueString(ToField(s))
}

// InArray 判断 needle 是否存在于 haystack 中。
// haystack 为 nil 或空切片时返回 false。
func InArray[T comparable](needle T, haystack []T) bool {
	for idx := range haystack {
		if haystack[idx] == needle {
			return true
		}
	}
	return false
}
