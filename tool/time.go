package tool

import (
	"strings"
	"time"
)

var TimeMap = map[string]string{
	"Y": "2006", "m": "01", "d": "02",
	"y": "06", "n": "1", "j": "2",
	"H": "15", "i": "04", "s": "05",
}

func Date(argv ...interface{}) string {
	var format string
	var timestamp int64
	var date time.Time
	argc := len(argv)
	if argc > 0 {
		format, _ = argv[0].(string)
		if argc > 1 {
			if v, ok := argv[1].(int); ok {
				timestamp = int64(v)
			} else if v, ok := argv[1].(uint); ok {
				timestamp = int64(v)
			} else if v, ok := argv[1].(string); ok {
				timestamp = String2Int64(v)
			} else {
				timestamp, _ = argv[1].(int64)
			}
		}
	}
	if format == "" {
		format = "Y-m-d H:i:s"
	}
	if timestamp > 0 {
		date = time.Unix(timestamp, 0)
	} else {
		date = time.Now()
	}
	for k, v := range TimeMap {
		format = strings.Replace(format, k, v, -1)
	}
	return date.Format(format)
}

func StrToTime(format string, str string) (int64, error) {
	if format == "" {
		format = "Y-m-d H:i:s"
	}
	for k, v := range TimeMap {
		format = strings.Replace(format, k, v, -1)
	}
	t, err := time.ParseInLocation(format, str, time.Local)
	if err != nil {
		return 0, err
	} else {
		return t.Unix(), nil
	}
}

func Time2Int() int {
	return int(time.Now().Unix())
}

func Time2Uint() uint {
	return uint(time.Now().Unix())
}

func Time2String() string {
	return Int64ToString(time.Now().Unix())
}

func Microtime() float64 {
	return float64(time.Now().UnixNano()/1e6) / 1e3
}

func GetYmd(timestamp uint) uint {
	return StringToUint(Date("Ymd", timestamp))
}

func GetTimestamp(ymd uint) uint {
	if ymd < 1 {
		ymd = GetYmd(0)
	}
	timestamp, err := StrToTime("Ymd", Uint2String(ymd))
	if err == nil && timestamp > 0 {
		return uint(timestamp)
	} else {
		return GetTimestamp(0)
	}
}

func GetYmdBeforeDay(day uint) uint {
	return GetYmd(Time2Uint() - 86400*day)
}

func GetYmdAfterDay(day uint) uint {
	return GetYmd(Time2Uint() + 86400*day)
}

func GetYesterdayYmd() uint {
	return GetYmdBeforeDay(1)
}

func GetYesterdayTime() uint {
	return GetTimestamp(GetYesterdayYmd())
}

func GetYmdSubDay(ymd, day uint) uint {
	return GetYmd(GetTimestamp(ymd) - 86400*day)
}

func GetYmdAddDay(ymd, day uint) uint {
	return GetYmd(GetTimestamp(ymd) + 86400*day)
}

var Months = map[string]int{
	"January":   1,
	"February":  2,
	"March":     3,
	"April":     4,
	"May":       5,
	"June":      6,
	"July":      7,
	"August":    8,
	"September": 9,
	"October":   10,
	"November":  11,
	"December":  12,
}

var Weeks = map[string]int{
	"Sunday":    0,
	"Monday":    1,
	"Tuesday":   2,
	"Wednesday": 3,
	"Thursday":  4,
	"Friday":    5,
	"Saturday":  6,
}

func TimestampConvert(timestamp uint) []int {
	if timestamp < 1 {
		timestamp = Time2Uint()
	}
	date := time.Unix(int64(timestamp), 0)
	y, m := date.Year(), Months[date.Month().String()]
	d, w := date.Day(), Weeks[date.Weekday().String()]
	ret := make([]int, 5)
	ret[4] = y
	ret[3] = y*100 + (m-1)/3*3 + 1
	ret[2] = y*100 + m
	ret[1] = int(GetYmd(timestamp - 86400*uint(w)))
	ret[0] = ret[2]*100 + d
	return ret
}

func IsLeapYear(y int) bool {
	if y%4 == 0 && y%100 != 0 || y%400 == 0 {
		return true
	}
	return false
}

func GetMonthDay(y, m int) int {
	if m == 2 {
		if IsLeapYear(y) {
			return 29
		} else {
			return 28
		}
	}
	if m == 4 || m == 6 || m == 9 || m == 11 {
		return 30
	}
	return 31
}

func GetWeekByDay(ymd string) int {
	timestamp, err := StrToTime("Ymd", ymd)
	if err == nil && timestamp > 0 {
		date := time.Unix(timestamp, 0)
		return Weeks[date.Weekday().String()]
	} else {
		return Weeks[time.Now().Weekday().String()]
	}
}
