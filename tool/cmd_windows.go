package tool

import (
	"os/exec"
	"strings"
	"syscall"
)

func Command(cmd string) (msg string, err error) {
	arg := []string{"/c"}
	cmd = strings.ReplaceAll(cmd, "\t", " ")
	arr := strings.Split(cmd, " ")
	for _, v := range arr {
		v = strings.TrimSpace(v)
		if v != "" {
			arg = append(arg, v)
		}
	}
	command := exec.Command("cmd", arg...)
	command.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	gbk, err := command.CombinedOutput()
	if len(gbk) == 0 {
		return "", err
	}
	msg = Convert(string(gbk), "gbk", "utf-8")
	return msg, err
}
