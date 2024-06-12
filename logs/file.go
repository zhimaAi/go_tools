package logs

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/hjwlsy/frame/tool"
)

var (
	dir = "logs"
	cmd = false
)

func SetLogsDir(d string) {
	if d != "" {
		dir = d
	}
}

func SetTerminal(show bool) {
	cmd = show
}

func Info(message string, v ...interface{}) {
	write("info", message, v...)
}

func Notice(message string, v ...interface{}) {
	write("notice", message, v...)
}

func Debug(message string, v ...interface{}) {
	write("debug", message, v...)
}

func Alert(message string, v ...interface{}) {
	write("alert", message, v...)
}

func Warning(message string, v ...interface{}) {
	write("warning", message, v...)
}

func Error(message string, v ...interface{}) {
	write("error", message, v...)
}

func Other(name, message string, v ...interface{}) {
	if name == "" {
		name = "default"
	}
	write(name, message, v...)
}

func write(level, message string, v ...interface{}) {
	when := time.Now()
	if len(v) > 0 {
		message = fmt.Sprintf(message, v...)
	}
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		file = "unknown"
		line = 0
	}
	dirs := strings.Split(tool.GetProPath(), `/`)
	project := dirs[len(dirs)-1]
	idx := strings.Index(file, project)
	if len(project) > 0 && idx > 0 && idx+len(project) < len(file) {
		file = file[idx+len(project):]
	}
	message = " [" + file + ":" + tool.Int2String(line) + "] " + message
	message = when.Format("2006-01-02 15:04:05.000") + message
	if cmd {
		fmt.Println("[" + level + "] " + message)
	}
	_ = save(when, level, message)
}

func save(when time.Time, level, message string) error {
	name, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	name = strings.Replace(name, "\\", "/", -1)
	name += when.Format("/2006/01/" + level + "/02.txt")
	return tool.AppendFile(name, message+"\r\n")
}
