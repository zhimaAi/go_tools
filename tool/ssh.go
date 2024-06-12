package tool

import (
	"golang.org/x/crypto/ssh"
	"time"
)

type terminal struct {
	host     string
	username string
	password string
	client   *ssh.Client
}

func NewSSH(host, username, password string, port int) (*terminal, error) {
	host = host + ":" + Int2String(port)
	t := terminal{host, username, password, nil}
	config := ssh.ClientConfig{
		User:            t.username,
		Auth:            []ssh.AuthMethod{ssh.Password(t.password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}
	if client, err := ssh.Dial("tcp", t.host, &config); err != nil {
		return nil, err
	} else {
		t.client = client
		return &t, nil
	}
}

func (t *terminal) Run(shell string) (string, error) {
	if session, err := t.client.NewSession(); err != nil {
		return "", err
	} else {
		defer func(session *ssh.Session) {
			_ = session.Close()
		}(session)
		buf, err := session.CombinedOutput(shell)
		return string(buf), err
	}
}

func (t *terminal) Close() {
	_ = t.client.Close()
}
