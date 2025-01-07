package tool

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"github.com/axgle/mahonia"
	"github.com/htruong/go-md2"
	"golang.org/x/crypto/md4"
	"strings"
)

func Convert(str string, src string, tag string) string {
	str = mahonia.NewDecoder(src).ConvertString(str)
	_, bytes, _ := mahonia.NewDecoder(tag).Translate([]byte(str), true)
	return string(bytes)
}

func MD5(text string) string {
	ctx := md5.New()
	ctx.Write(String2Bytes(text))
	return hex.EncodeToString(ctx.Sum(nil))
}

func MD4(text string) string {
	ctx := md4.New()
	ctx.Write(String2Bytes(text))
	return hex.EncodeToString(ctx.Sum(nil))
}

func MD2(text string) string {
	ctx := md2.New()
	ctx.Write(String2Bytes(text))
	return hex.EncodeToString(ctx.Sum(nil))
}

func MyMD5(str string) string {
	bytes := []byte(str)
	length := len(bytes)
	mod := length & 0x3f
	mod = 56 - mod
	if mod <= 0 {
		mod += 64
	}
	patch := make([]byte, mod+8)
	patch[0] = 0x80
	length <<= 3
	for i := 0; i < 8; i++ {
		patch[mod+i] = byte(length & 0xff)
		length >>= 8
		if length <= 0 {
			break
		}
	}
	bytes = append(bytes, patch...)
	n := len(bytes) >> 6
	ti := [64]uint32{
		3614090360, 3905402710, 606105819, 3250441966, 4118548399, 1200080426, 2821735955, 4249261313,
		1770035416, 2336552879, 4294925233, 2304563134, 1804603682, 4254626195, 2792965006, 1236535329,
		4129170786, 3225465664, 643717713, 3921069994, 3593408605, 38016083, 3634488961, 3889429448,
		568446438, 3275163606, 4107603335, 1163531501, 2850285829, 4243563512, 1735328473, 2368359562,
		4294588738, 2272392833, 1839030562, 4259657740, 2763975236, 1272893353, 4139469664, 3200236656,
		681279174, 3936430074, 3572445317, 76029189, 3654602809, 3873151461, 530742520, 3299628645,
		4096336452, 1126891415, 2878612391, 4237533241, 1700485571, 2399980690, 4293915773, 2240044497,
		1873313359, 4264355552, 2734768916, 1309151649, 4149444226, 3174756917, 718787259, 3951481745,
	} //int((1 << 32) * math.Abs(math.Sin(float64(i)))) 1<=i<=64
	s := [64]uint8{
		7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22,
		5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20,
		4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
		6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
	}
	var A, B, C, D uint32
	A, B, C, D = 0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476
	for i := 0; i < n; i++ {
		M := bytes[i<<6 : (i+1)<<6]
		m := make([]uint32, 16)
		for j := 0; j < 16; j++ {
			idx := j << 2
			m[j] = uint32(M[idx]) + uint32(M[idx+1])<<8 + uint32(M[idx+2])<<16 + uint32(M[idx+3])<<24
		}
		a, b, c, d := A, B, C, D
		for j := 0; j < 64; j++ {
			var fun, idx uint32
			if j < 16 {
				fun, idx = (b&c)|((^b)&d), uint32(j)
			} else {
				if j < 32 {
					fun, idx = (b&d)|(c&(^d)), uint32((1+5*j)%16)
				} else {
					if j < 48 {
						fun, idx = b^c^d, uint32((5+3*j)%16)
					} else {
						fun, idx = c^(b|(^d)), uint32((7*j)%16)
					}
				}
			}
			a = a + fun + ti[j] + m[idx]
			a = a<<s[j] + a>>(32-s[j]) + b
			a, b, c, d = d, a, b, c
		}
		A, B, C, D = A+a, B+b, C+c, D+d
	}
	temp := make([]uint8, 16)
	for i := 0; i < 4; i++ {
		temp[i] = uint8((A >> (i * 8)) & 0xff)
		temp[i+4] = uint8((B >> (i * 8)) & 0xff)
		temp[i+8] = uint8((C >> (i * 8)) & 0xff)
		temp[i+12] = uint8((D >> (i * 8)) & 0xff)
	}
	result := make([]byte, 32)
	h := [16]byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'}
	for k, v := range temp {
		result[k*2+1] = h[v&0x0f]
		result[k*2] = h[v&0xf0>>4]
	}
	return string(result)
}

func JsonDecode(s string, v interface{}) error {
	return json.Unmarshal(String2Bytes(s), v)
}

func JsonDecodeUseNumber(s string, v interface{}) error {
	d := json.NewDecoder(strings.NewReader(s))
	d.UseNumber()
	return d.Decode(v)
}

func JsonEncode(v interface{}) (string, error) {
	if bytes, err := json.Marshal(v); err != nil {
		return "", err
	} else {
		return Bytes2String(bytes), nil
	}
}

func JsonEncodeNoError(v any) string {
	s, _ := JsonEncode(v)
	return s
}

func JsonEncodeIndent(v interface{}, prefix, indent string) (string, error) {
	if bytes, err := json.MarshalIndent(v, prefix, indent); err != nil {
		return "", err
	} else {
		return Bytes2String(bytes), nil
	}
}

func Base64Encode(src string) string {
	return base64.StdEncoding.EncodeToString(String2Bytes(src))
}

func Base64Decode(src string) (string, error) {
	ret, err := base64.StdEncoding.DecodeString(src)
	if err != nil {
		return "", err
	} else {
		return Bytes2String(ret), nil
	}
}

func Base64DecodeNoError(src string) string {
	s, _ := Base64Decode(src)
	return s
}
