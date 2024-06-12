package tool

import (
	"errors"
	"github.com/skip2/go-qrcode"
	"math/rand"
	"net"
	"regexp"
	"strings"
	"time"
)

func QRCode(s string) (string, error) {
	smd5 := MD5(s)
	path := "static/qrcode/" + smd5[:2] + "/"
	if err := MkDirAll(path); err != nil {
		return "", err
	}
	file := path + smd5[2:] + ".png"
	if !IsFile(file) {
		err := qrcode.WriteFile(s, qrcode.Medium, 256, file)
		if err != nil {
			return "", err
		}
	}
	if IsFile(file) {
		return "/" + file, nil
	}
	return "", errors.New("qrcode generation failed")
}

func IsMobile(useragent string) bool {
	useragent = strings.ToLower(useragent)
	keywords := []string{"android", "iphone", "ipod", "ipad", "windows phone", "mqqbrowser"}
	for _, v := range keywords {
		if strings.Contains(useragent, v) {
			return true
		}
	}
	pattern1 := `/(android|bb\d+|meego).+mobile|avantgo|bada/|blackberry|blazer|compal|elaine|fennec|hiptop|iemobile|ip(hone|od)|iris|kindle|lge |maemo|midp|mmp|mobile.+firefox|netfront|opera m(ob|in)i|palm( os)?|phone|p(ixi|re)/|plucker|pocket|psp|series(4|6)0|symbian|treo|up\.(browser|link)|vodafone|wap|windows (ce|phone)|xda|xiino/i`
	if matched1, err := regexp.MatchString(pattern1, useragent); err == nil && matched1 {
		return true
	} else {
		pattern2 := `/1207|6310|6590|3gso|4thp|50[1-6]i|770s|802s|a wa|abac|ac(er|oo|s\-)|ai(ko|rn)|al(av|ca|co)|amoi|an(ex|ny|yw)|aptu|ar(ch|go)|as(te|us)|attw|au(di|\-m|r |s )|avan|be(ck|ll|nq)|bi(lb|rd)|bl(ac|az)|br(e|v)w|bumb|bw\-(n|u)|c55/|capi|ccwa|cdm\-|cell|chtm|cldc|cmd\-|co(mp|nd)|craw|da(it|ll|ng)|dbte|dc\-s|devi|dica|dmob|do(c|p)o|ds(12|\-d)|el(49|ai)|em(l2|ul)|er(ic|k0)|esl8|ez([4-7]0|os|wa|ze)|fetc|fly(\-|_)|g1 u|g560|gene|gf\-5|g\-mo|go(\.w|od)|gr(ad|un)|haie|hcit|hd\-(m|p|t)|hei\-|hi(pt|ta)|hp( i|ip)|hs\-c|ht(c(\-| |_|a|g|p|s|t)|tp)|hu(aw|tc)|i\-(20|go|ma)|i230|iac( |\-|/)|ibro|idea|ig01|ikom|im1k|inno|ipaq|iris|ja(t|v)a|jbro|jemu|jigs|kddi|keji|kgt( |/)|klon|kpt |kwc\-|kyo(c|k)|le(no|xi)|lg( g|/(k|l|u)|50|54|\-[a-w])|libw|lynx|m1\-w|m3ga|m50/|ma(te|ui|xo)|mc(01|21|ca)|m\-cr|me(rc|ri)|mi(o8|oa|ts)|mmef|mo(01|02|bi|de|do|t(\-| |o|v)|zz)|mt(50|p1|v )|mwbp|mywa|n10[0-2]|n20[2-3]|n30(0|2)|n50(0|2|5)|n7(0(0|1)|10)|ne((c|m)\-|on|tf|wf|wg|wt)|nok(6|i)|nzph|o2im|op(ti|wv)|oran|owg1|p800|pan(a|d|t)|pdxg|pg(13|\-([1-8]|c))|phil|pire|pl(ay|uc)|pn\-2|po(ck|rt|se)|prox|psio|pt\-g|qa\-a|qc(07|12|21|32|60|\-[2-7]|i\-)|qtek|r380|r600|raks|rim9|ro(ve|zo)|s55/|sa(ge|ma|mm|ms|ny|va)|sc(01|h\-|oo|p\-)|sdk/|se(c(\-|0|1)|47|mc|nd|ri)|sgh\-|shar|sie(\-|m)|sk\-0|sl(45|id)|sm(al|ar|b3|it|t5)|so(ft|ny)|sp(01|h\-|v\-|v )|sy(01|mb)|t2(18|50)|t6(00|10|18)|ta(gt|lk)|tcl\-|tdg\-|tel(i|m)|tim\-|t\-mo|to(pl|sh)|ts(70|m\-|m3|m5)|tx\-9|up(\.b|g1|si)|utst|v400|v750|veri|vi(rg|te)|vk(40|5[0-3]|\-v)|vm40|voda|vulc|vx(52|53|60|61|70|80|81|83|85|98)|w3c(\-| )|webc|whit|wi(g |nc|nw)|wmlb|wonu|x700|yas\-|your|zeto|zte\-/i`
		if matched2, err := regexp.MatchString(pattern2, useragent[:4]); err == nil && matched2 {
			return true
		}
	}
	return false
}

func IsMobileNumber(mobile string) bool {
	if ok, err := regexp.MatchString(`^1\d{10}$`, mobile); err == nil && ok {
		return true
	}
	return false
}

func FindInSet(s, sl string) bool {
	ss := strings.Split(sl, ",")
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

func IsIP(ip string) bool {
	if i := strings.LastIndex(ip, ":"); i >= 0 {
		ip = ip[:i]
	}
	pattern := `^(25[0-5]|2[0-4]\d|[0-1]\d{2}|[1-9]?\d)\.(25[0-5]|2[0-4]\d|[0-1]\d{2}|[1-9]?\d)\.(25[0-5]|2[0-4]\d|[0-1]\d{2}|[1-9]?\d)\.(25[0-5]|2[0-4]\d|[0-1]\d{2}|[1-9]?\d)$`
	if ok, err := regexp.MatchString(pattern, ip); err == nil && ok {
		return true
	}
	return false
}

func GetMacList() (maclist string) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return
	}
	for _, v := range interfaces {
		if v.Flags&net.FlagUp == 0 {
			continue
		}
		if v.Flags&net.FlagLoopback != 0 {
			continue
		}
		mac := strings.ToUpper(v.HardwareAddr.String())
		mac = strings.ReplaceAll(mac, ":", "")
		if len(mac) != 12 {
			continue
		}
		if maclist == "" {
			maclist = mac
		} else {
			maclist += "," + mac
		}
	}
	return
}

func Random(n int) (s string) {
	const l = "0123456789" +
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
		"abcdefghijklmnopqrstuvwxyz"
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < n; i++ {
		idx := r.Intn(62)
		s += string(l[idx])
	}
	return
}
