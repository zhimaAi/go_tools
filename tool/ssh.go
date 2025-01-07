package tool

import (
	"errors"
	"fmt"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Terminal struct {
	sshClient  *ssh.Client
	sftpClient *sftp.Client
}

func NewSSH(host, username, password string, port int) (*Terminal, error) {
	config := ssh.ClientConfig{
		User:            username,
		Auth:            []ssh.AuthMethod{ssh.Password(password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}
	addr := fmt.Sprintf(`%s:%d`, host, port)
	if client, err := ssh.Dial(`tcp`, addr, &config); err != nil {
		return nil, err
	} else {
		return &Terminal{sshClient: client}, nil
	}
}

func (t *Terminal) Run(shell string) (string, error) {
	if t.sshClient == nil {
		return ``, errors.New(`ssh client is nil`)
	}
	session, err := t.sshClient.NewSession()
	if err != nil {
		return ``, err
	}
	defer func(session *ssh.Session) {
		_ = session.Close()
	}(session)
	buf, err := session.CombinedOutput(shell)
	return string(buf), err
}

func (t *Terminal) Close() {
	if t.sftpClient != nil {
		_ = t.sftpClient.Close()
	}
	if t.sshClient != nil {
		_ = t.sshClient.Close()
	}
}

func (t *Terminal) GetSftp() (*sftp.Client, error) {
	if t.sftpClient != nil {
		return t.sftpClient, nil
	}
	if t.sshClient == nil {
		return nil, errors.New(`ssh client is nil`)
	}
	sftpClient, err := sftp.NewClient(t.sshClient)
	if err != nil {
		return nil, err
	}
	t.sftpClient = sftpClient
	return t.sftpClient, nil
}

func (t *Terminal) Download(remotePath, localPath string) (int64, error) {
	sftpClient, err := t.GetSftp()
	if err != nil {
		return 0, err
	}
	remoteFile, err := sftpClient.Open(remotePath)
	if err != nil {
		return 0, err
	}
	defer func(remoteFile *sftp.File) {
		_ = remoteFile.Close()
	}(remoteFile)
	localFile, err := os.Create(localPath)
	if err != nil {
		return 0, err
	}
	defer func(localFile *os.File) {
		_ = localFile.Close()
	}(localFile)
	return io.Copy(localFile, remoteFile)
}

func (t *Terminal) DownloadDir(remoteDir, localDir string, allowExts []string, recurve bool) ([]error, error) {
	sftpClient, err := t.GetSftp()
	if err != nil {
		return nil, err
	}
	remoteDir = strings.TrimRight(strings.ReplaceAll(remoteDir, "\\", `/`), `/`) + `/`
	localDir = strings.TrimRight(strings.ReplaceAll(localDir, "\\", `/`), `/`) + `/`
	files, err := sftpClient.ReadDir(remoteDir)
	if err != nil {
		return nil, err
	}
	if err = MkDirAll(localDir); err != nil {
		return nil, err
	}
	errList := make([]error, 0)
	for _, file := range files {
		if file.IsDir() {
			if recurve {
				errs, err := t.DownloadDir(remoteDir+file.Name(), localDir+file.Name(), allowExts, recurve)
				if err != nil {
					return nil, err
				}
				errList = append(errList, errs...)
			}
		} else if file.Mode().IsRegular() {
			if len(allowExts) > 0 {
				ext := strings.ToLower(filepath.Ext(file.Name()))
				if !InArrayString(ext, allowExts) {
					continue
				}
			}
			_, err = t.Download(remoteDir+file.Name(), localDir+file.Name())
			if err != nil {
				errList = append(errList, fmt.Errorf(`file:%s,err:%s`, remoteDir+file.Name(), err.Error()))
			}
		} else {
			//todo ...
		}
	}
	return errList, nil
}
