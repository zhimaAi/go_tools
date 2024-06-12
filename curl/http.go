package curl

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"encoding/xml"
	"gopkg.in/yaml.v2"
	"io"
	"log"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

var defaultSetting = Setting{
	UserAgent:        "httpServer",
	ConnectTimeout:   60 * time.Second,
	ReadWriteTimeout: 60 * time.Second,
	Gzip:             true,
	DumpBody:         true,
}

var defaultCookieJar http.CookieJar
var settingMutex sync.Mutex

func createDefaultCookie() {
	settingMutex.Lock()
	defer settingMutex.Unlock()
	defaultCookieJar, _ = cookiejar.New(nil)
}

func SetDefaultSetting(setting Setting) {
	settingMutex.Lock()
	defer settingMutex.Unlock()
	defaultSetting = setting
}

func NewRequest(rawurl, method string) *Request {
	var resp http.Response
	u, err := url.Parse(rawurl)
	if err != nil {
		log.Println("http:", err)
	}
	req := http.Request{
		URL:        u,
		Method:     method,
		Header:     make(http.Header),
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
	}
	return &Request{
		url:      rawurl,
		req:      &req,
		params:   map[string][]string{},
		files:    map[string]string{},
		filename: map[string]string{},
		setting:  defaultSetting,
		resp:     &resp,
	}
}

func Get(url string) *Request {
	return NewRequest(url, "GET")
}

func Post(url string) *Request {
	return NewRequest(url, "POST")
}

func Put(url string) *Request {
	return NewRequest(url, "PUT")
}

func Delete(url string) *Request {
	return NewRequest(url, "DELETE")
}

func Head(url string) *Request {
	return NewRequest(url, "HEAD")
}

type Setting struct {
	ShowDebug        bool
	UserAgent        string
	ConnectTimeout   time.Duration
	ReadWriteTimeout time.Duration
	TLSClientConfig  *tls.Config
	Proxy            func(*http.Request) (*url.URL, error)
	Transport        http.RoundTripper
	CheckRedirect    func(req *http.Request, via []*http.Request) error
	EnableCookie     bool
	Gzip             bool
	DumpBody         bool
	Retries          int
}

type Request struct {
	url      string
	req      *http.Request
	params   map[string][]string
	files    map[string]string
	filename map[string]string
	setting  Setting
	resp     *http.Response
	body     []byte
	dump     []byte
}

func (b *Request) GetRequest() *http.Request {
	return b.req
}

func (b *Request) Setting(setting Setting) *Request {
	b.setting = setting
	return b
}

func (b *Request) SetBasicAuth(username, password string) *Request {
	b.req.SetBasicAuth(username, password)
	return b
}

func (b *Request) SetEnableCookie(enable bool) *Request {
	b.setting.EnableCookie = enable
	return b
}

func (b *Request) SetUserAgent(useragent string) *Request {
	b.setting.UserAgent = useragent
	return b
}

func (b *Request) Debug(isdebug bool) *Request {
	b.setting.ShowDebug = isdebug
	return b
}

func (b *Request) Retries(times int) *Request {
	b.setting.Retries = times
	return b
}

func (b *Request) DumpBody(isdump bool) *Request {
	b.setting.DumpBody = isdump
	return b
}

func (b *Request) DumpRequest() []byte {
	return b.dump
}

func (b *Request) SetTimeout(connectTimeout, readWriteTimeout time.Duration) *Request {
	b.setting.ConnectTimeout = connectTimeout
	b.setting.ReadWriteTimeout = readWriteTimeout
	return b
}

func (b *Request) SetTLSClientConfig(config *tls.Config) *Request {
	b.setting.TLSClientConfig = config
	return b
}

func (b *Request) Header(key, value string) *Request {
	b.req.Header.Set(key, value)
	return b
}

func (b *Request) SetHost(host string) *Request {
	b.req.Host = host
	return b
}

func (b *Request) SetProtocolVersion(vers string) *Request {
	if len(vers) == 0 {
		vers = "HTTP/1.1"
	}
	major, minor, ok := http.ParseHTTPVersion(vers)
	if ok {
		b.req.Proto = vers
		b.req.ProtoMajor = major
		b.req.ProtoMinor = minor
	}
	return b
}

func (b *Request) SetCookie(cookie *http.Cookie) *Request {
	b.req.Header.Add("Cookie", cookie.String())
	return b
}

func (b *Request) SetTransport(transport http.RoundTripper) *Request {
	b.setting.Transport = transport
	return b
}

func (b *Request) SetProxy(proxy func(*http.Request) (*url.URL, error)) *Request {
	b.setting.Proxy = proxy
	return b
}

func (b *Request) SetCheckRedirect(redirect func(req *http.Request, via []*http.Request) error) *Request {
	b.setting.CheckRedirect = redirect
	return b
}

func (b *Request) Param(key, value string) *Request {
	if param, ok := b.params[key]; ok {
		b.params[key] = append(param, value)
	} else {
		b.params[key] = []string{value}
	}
	return b
}

func (b *Request) PostFile(formname, filename string, argv ...string) *Request {
	b.files[formname] = filename
	if len(argv) > 0 && len(argv[0]) > 0 {
		b.filename[formname] = argv[0]
	} else {
		b.filename[formname] = filename
	}
	return b
}

func (b *Request) Body(data interface{}) *Request {
	switch t := data.(type) {
	case string:
		bf := bytes.NewBufferString(t)
		b.req.Body = io.NopCloser(bf)
		b.req.ContentLength = int64(len(t))
	case []byte:
		bf := bytes.NewBuffer(t)
		b.req.Body = io.NopCloser(bf)
		b.req.ContentLength = int64(len(t))
	}
	return b
}

func (b *Request) XMLBody(obj interface{}) (*Request, error) {
	if b.req.Body == nil && obj != nil {
		byts, err := xml.Marshal(obj)
		if err != nil {
			return b, err
		}
		b.req.Body = io.NopCloser(bytes.NewReader(byts))
		b.req.ContentLength = int64(len(byts))
		b.req.Header.Set("Content-Type", "application/xml")
	}
	return b, nil
}

func (b *Request) YAMLBody(obj interface{}) (*Request, error) {
	if b.req.Body == nil && obj != nil {
		byts, err := yaml.Marshal(obj)
		if err != nil {
			return b, err
		}
		b.req.Body = io.NopCloser(bytes.NewReader(byts))
		b.req.ContentLength = int64(len(byts))
		b.req.Header.Set("Content-Type", "application/x+yaml")
	}
	return b, nil
}

func (b *Request) JSONBody(obj interface{}) (*Request, error) {
	if b.req.Body == nil && obj != nil {
		byts, err := json.Marshal(obj)
		if err != nil {
			return b, err
		}
		b.req.Body = io.NopCloser(bytes.NewReader(byts))
		b.req.ContentLength = int64(len(byts))
		b.req.Header.Set("Content-Type", "application/json")
	}
	return b, nil
}

func (b *Request) buildURL(paramBody string) {
	if b.req.Method == "GET" && len(paramBody) > 0 {
		if strings.Contains(b.url, "?") {
			b.url += "&" + paramBody
		} else {
			b.url = b.url + "?" + paramBody
		}
		return
	}
	if (b.req.Method == "POST" || b.req.Method == "PUT" ||
		b.req.Method == "PATCH" || b.req.Method == "DELETE") &&
		b.req.Body == nil {
		if len(b.files) > 0 {
			pr, pw := io.Pipe()
			bodyWriter := multipart.NewWriter(pw)
			go func() {
				for formname, filename := range b.files {
					fileWriter, err := bodyWriter.CreateFormFile(formname, b.filename[formname])
					if err != nil {
						log.Println("http:", err)
					}
					fh, err := os.Open(filename)
					if err != nil {
						log.Println("http:", err)
					}
					_, err = io.Copy(fileWriter, fh)
					_ = fh.Close()
					if err != nil {
						log.Println("http:", err)
					}
				}
				for k, v := range b.params {
					for _, vv := range v {
						_ = bodyWriter.WriteField(k, vv)
					}
				}
				_ = bodyWriter.Close()
				_ = pw.Close()
			}()
			b.Header("Content-Type", bodyWriter.FormDataContentType())
			b.req.Body = io.NopCloser(pr)
			return
		}
		if len(paramBody) > 0 {
			b.Header("Content-Type", "application/x-www-form-urlencoded")
			b.Body(paramBody)
		}
	}
}

func (b *Request) getResponse() (*http.Response, error) {
	if b.resp.StatusCode != 0 {
		return b.resp, nil
	}
	resp, err := b.DoRequest()
	if err != nil {
		return nil, err
	}
	b.resp = resp
	return resp, nil
}

func (b *Request) DoRequest() (resp *http.Response, err error) {
	var paramBody string
	if len(b.params) > 0 {
		var buf bytes.Buffer
		for k, v := range b.params {
			for _, vv := range v {
				buf.WriteString(url.QueryEscape(k))
				buf.WriteByte('=')
				buf.WriteString(url.QueryEscape(vv))
				buf.WriteByte('&')
			}
		}
		paramBody = buf.String()
		paramBody = paramBody[0 : len(paramBody)-1]
	}
	b.buildURL(paramBody)
	urlParsed, err := url.Parse(b.url)
	if err != nil {
		return nil, err
	}
	b.req.URL = urlParsed
	trans := b.setting.Transport
	if trans == nil {
		trans = &http.Transport{
			TLSClientConfig:     b.setting.TLSClientConfig,
			Proxy:               b.setting.Proxy,
			MaxIdleConnsPerHost: 100,
			DisableKeepAlives:   true,
		}
	} else {
		if t, ok := trans.(*http.Transport); ok {
			if t.TLSClientConfig == nil {
				t.TLSClientConfig = b.setting.TLSClientConfig
			}
			if t.Proxy == nil {
				t.Proxy = b.setting.Proxy
			}
		}
	}
	var jar http.CookieJar
	if b.setting.EnableCookie {
		if defaultCookieJar == nil {
			createDefaultCookie()
		}
		jar = defaultCookieJar
	}
	client := &http.Client{
		Transport: trans,
		Jar:       jar,
	}
	if b.setting.UserAgent != "" && b.req.Header.Get("User-Agent") == "" {
		b.req.Header.Set("User-Agent", b.setting.UserAgent)
	}
	if b.setting.CheckRedirect != nil {
		client.CheckRedirect = b.setting.CheckRedirect
	}
	if b.setting.ShowDebug {
		dump, err := httputil.DumpRequest(b.req, b.setting.DumpBody)
		if err != nil {
			log.Println(err.Error())
		}
		b.dump = dump
	}
	for i := 0; b.setting.Retries == -1 || i <= b.setting.Retries; i++ {
		resp, err = client.Do(b.req)
		if err == nil {
			break
		}
	}
	return resp, err
}

func (b *Request) String() (string, error) {
	data, err := b.Bytes()
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (b *Request) Bytes() ([]byte, error) {
	if b.body != nil {
		return b.body, nil
	}
	resp, err := b.getResponse()
	if err != nil {
		return nil, err
	}
	if resp.Body == nil {
		return nil, nil
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)
	if b.setting.Gzip && resp.Header.Get("Content-Encoding") == "gzip" {
		reader, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		b.body, err = io.ReadAll(reader)
		return b.body, err
	}
	b.body, err = io.ReadAll(resp.Body)
	return b.body, err
}

func (b *Request) ToFile(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)
	resp, err := b.getResponse()
	if err != nil {
		return err
	}
	if resp.Body == nil {
		return nil
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)
	_, err = io.Copy(f, resp.Body)
	return err
}

func (b *Request) ToJSON(v interface{}) error {
	data, err := b.Bytes()
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func (b *Request) ToXML(v interface{}) error {
	data, err := b.Bytes()
	if err != nil {
		return err
	}
	return xml.Unmarshal(data, v)
}

func (b *Request) ToYAML(v interface{}) error {
	data, err := b.Bytes()
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, v)
}

func (b *Request) Response() (*http.Response, error) {
	return b.getResponse()
}

func TimeoutDialer(cTimeout time.Duration, rwTimeout time.Duration) func(net, addr string) (c net.Conn, err error) {
	return func(netw, addr string) (net.Conn, error) {
		conn, err := net.DialTimeout(netw, addr, cTimeout)
		if err != nil {
			return nil, err
		}
		err = conn.SetDeadline(time.Now().Add(rwTimeout))
		return conn, err
	}
}
