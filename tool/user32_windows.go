package tool

import (
	"errors"
	"syscall"
	"unsafe"
)

func StringPtr(s string) uintptr {
	ptr, err := syscall.UTF16PtrFromString(s)
	if err != nil {
		return 0
	}
	return uintptr(unsafe.Pointer(ptr))
}

// MessageBox 确定_1 取消_2 中止(A)_3 重试(R)_4 忽略(I)_5 是(Y)_6 否(N)_7 重试(T)_10 继续(C)_11
func MessageBox(title, content string, style int) (int, error) {
	TitleEmpty := errors.New("消息弹框标题不能为空")
	ContentEmpty := errors.New("消息弹框内容不能为空")
	StyleError := errors.New("消息弹框样式类型错误")
	Successfully := "The operation completed successfully."
	if title == "" {
		return 0, TitleEmpty
	}
	if content == "" {
		return 0, ContentEmpty
	}
	styles := []uintptr{0, 1, 2, 3, 4, 5, 6, 16, 32, 48, 64, 128, 256, 512, 768}
	if style < 0 || style >= len(styles) {
		return 0, StyleError
	}
	MessageBox := syscall.NewLazyDLL("user32.dll").NewProc("MessageBoxW")
	n, _, err := MessageBox.Call(0, StringPtr(content), StringPtr(title), styles[style])
	if err != nil && err.Error() != Successfully {
		return 0, err
	}
	return int(n), nil
}

func GetWindowSize(a ...int) (width, height int) {
	proc := syscall.NewLazyDLL("user32.dll").NewProc("GetSystemMetrics")
	w, _, _ := proc.Call(0)
	h, _, _ := proc.Call(1)
	width, height = int(w), int(h)
	if width < 1 {
		if len(a) >= 1 && a[0] > 0 {
			width = a[0]
		} else {
			width = 1920
		}
	}
	if height < 1 {
		if len(a) >= 2 && a[1] > 0 {
			height = a[1]
		} else {
			height = 1080
		}
	}
	return
}
