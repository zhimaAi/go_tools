package msql

import (
	"strings"

	"github.com/zhimaAi/go_tools/tool"
)

func TrimAdd(a ...string) (s string) {
	n := len(a)
	if n == 0 {
		return
	}
	if a[0] != "" {
		s = strings.TrimSpace(a[0])
		s = strings.Trim(s, "`'\"\t ")
		if n == 1 {
			return
		}
		s = strings.Trim(s, a[1])
	}
	if n == 2 {
		return
	}
	return a[2] + s + a[2]
}

func ToField(s string) string {
	return TrimAdd(s, "", "")
}

func ToString(s string) string {
	return TrimAdd(s, "", "'")
}

func ToLike(s string) string {
	return ToString(TrimAdd(s, "", "%"))
}

func Assemble(s string) string {
	l := strings.Split(s, ",")
	for i, v := range l {
		l[i] = ToString(v)
	}
	return strings.Join(l, ",")
}

func GetLimitPage(query Params) (limit, page int) {
	limit = tool.Intval(query["limit"])
	if limit < 1 {
		limit = 15
	}
	page = tool.Intval(query["page"])
	if page < 1 {
		page = 1
	}
	return
}
