package mq

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/zhimaAi/go_tools/logs"
)

const nsqDefaultHost = `127.0.0.1`
const producerDefaultPort = 4150
const producerDefaultWorkNum = 1
const producerDefaultJobsLen = 1000
const consumerDefaultPort = 4161

type job struct {
	topic   string
	message string
	delay   time.Duration
}

type ProducerHandle struct {
	addr    string
	host    string
	port    uint
	workNum uint
	workWg  *sync.WaitGroup
	opLock  *sync.Mutex
	running bool
	jobs    chan *job
	isStop  bool
}

func NewProducerHandle() *ProducerHandle {
	return &ProducerHandle{
		addr:    ``,
		host:    nsqDefaultHost,
		port:    producerDefaultPort,
		workNum: producerDefaultWorkNum,
		workWg:  &sync.WaitGroup{},
		opLock:  &sync.Mutex{},
		running: false,
		jobs:    make(chan *job, producerDefaultJobsLen),
		isStop:  false,
	}
}

func (handle *ProducerHandle) SetAddr(addr string) *ProducerHandle {
	if !handle.running && len(strings.TrimSpace(addr)) > 0 {
		handle.addr = strings.TrimSpace(addr)
	}
	return handle
}

func (handle *ProducerHandle) SetHostAndPort(host string, port uint) *ProducerHandle {
	if !handle.running && len(strings.TrimSpace(host)) > 0 {
		handle.host = strings.TrimSpace(host)
	}
	if !handle.running && port > 0 && port < 65536 {
		handle.port = port
	}
	return handle
}

func (handle *ProducerHandle) SetWorkNum(workNum uint) *ProducerHandle {
	if !handle.running && workNum > 0 {
		handle.workNum = workNum
	}
	return handle
}

func (handle *ProducerHandle) getConn() (*nsq.Producer, error) {
	if len(handle.addr) == 0 {
		handle.addr = fmt.Sprintf(`%s:%d`, handle.host, handle.port)
	}
	conn, err := nsq.NewProducer(handle.addr, nsq.NewConfig())
	if err != nil {
		return nil, err
	}
	conn.SetLoggerLevel(nsq.LogLevelError)
	if err = conn.Ping(); err != nil {
		return nil, err
	}
	return conn, nil
}

func (handle *ProducerHandle) publish(conn *nsq.Producer, data *job) error {
	if data.delay > 0 {
		delay := min(data.delay, time.Hour) //nsq延时时间0-3600,000ms
		return conn.DeferredPublish(data.topic, delay, []byte(data.message))
	}
	return conn.Publish(data.topic, []byte(data.message))
}

func (handle *ProducerHandle) autoRun() {
	handle.opLock.Lock()
	defer handle.opLock.Unlock()
	if !handle.running {
		handle.running = true
		for i := 0; i < int(handle.workNum); i++ {
			handle.workWg.Add(1)
			go doWork(handle)
		}
	}
}

func doWork(handle *ProducerHandle) {
	defer handle.workWg.Done()
	var conn *nsq.Producer
	var err error
	defer func(conn *nsq.Producer) {
		if conn != nil {
			conn.Stop()
		}
	}(conn)
	for data := range handle.jobs {
		//获取conn
		for conn == nil {
			if handle.isStop {
				_ = handle.AddJobs(data.topic, data.message, data.delay) //重入jobs
				return                                                   //直接退出
			}
			conn, err = handle.getConn()
			if err != nil {
				logs.Error(`producer conn error:%s`, err.Error())
				time.Sleep(time.Second)
			}
		}
		//生产data
		if err = handle.publish(conn, data); err != nil {
			logs.Error(`producer publish error:%s`, err.Error())
			//重试一次
			if err = handle.publish(conn, data); err != nil {
				logs.Error(`producer retry publish error:%s`, err.Error())
				_ = handle.AddJobs(data.topic, data.message, data.delay) //重入jobs
				conn.Stop()                                              //关闭连接
				conn = nil                                               //清理连接
			}
		}
	}
}

func (handle *ProducerHandle) AddJobs(topic, message string, delay ...time.Duration) error {
	if handle.isStop {
		return errors.New(`已退出,禁止生产数据`)
	}
	if !handle.running {
		handle.autoRun() //自动启动work
	}
	topic = strings.TrimSpace(topic)
	if len(topic) == 0 || len(message) == 0 {
		return errors.New(`topic与message不能为空`)
	}
	data := &job{topic: topic, message: message, delay: 0}
	if len(delay) > 0 && delay[0] > 0 {
		data.delay = delay[0]
	}
	defer func() {
		if r := recover(); r != nil {
			logs.Other(`recover`, `topic:%s,message:%s,delay:%v,recover:%#v`,
				data.topic, data.message, data.delay, r)
		}
	}()
	handle.jobs <- data
	return nil
}

func (handle *ProducerHandle) Stop() {
	handle.opLock.Lock()
	defer handle.opLock.Unlock()
	if handle.isStop {
		return
	}
	logs.Info(`正在退出...`)
	handle.isStop = true
	time.Sleep(time.Second)
	close(handle.jobs)
	logs.Info(`jobs已关闭...`)
	go func(handle *ProducerHandle) {
		for len(handle.jobs) > 0 {
			logs.Info(`(%d)休眠中...`, len(handle.jobs))
			time.Sleep(time.Second * 1)
		}
	}(handle)
	handle.workWg.Wait()
	logs.Info(`jobs任务量:%d`, len(handle.jobs))
	for data := range handle.jobs {
		logs.Warning(`未生产的数据:topic:%s,message:%s,delay:%v`, data.topic, data.message, data.delay)
	}
	logs.Info(`退出已完成`)
}

type ConsumerHandle struct {
	addr     string
	host     string
	port     uint
	workWg   *sync.WaitGroup
	opLock   *sync.Mutex
	stopLock *sync.Mutex
	run      int
	conns    []*nsq.Consumer
	isStop   bool
}

func NewConsumerHandle() *ConsumerHandle {
	return &ConsumerHandle{
		addr:     ``,
		host:     nsqDefaultHost,
		port:     consumerDefaultPort,
		workWg:   &sync.WaitGroup{},
		opLock:   &sync.Mutex{},
		stopLock: &sync.Mutex{},
		run:      0,
		conns:    make([]*nsq.Consumer, 0),
		isStop:   false,
	}
}

func (handle *ConsumerHandle) SetAddr(addr string) *ConsumerHandle {
	if len(strings.TrimSpace(addr)) > 0 {
		handle.addr = strings.TrimSpace(addr)
	}
	return handle
}

func (handle *ConsumerHandle) SetHostAndPort(host string, port uint) *ConsumerHandle {
	if len(strings.TrimSpace(host)) > 0 {
		handle.host = strings.TrimSpace(host)
	}
	if port > 0 && port < 65536 {
		handle.port = port
	}
	return handle
}

func (handle *ConsumerHandle) setRun(diff int) {
	handle.opLock.Lock()
	defer handle.opLock.Unlock()
	handle.run += diff
}

func (handle *ConsumerHandle) PushZero(addr, topic string) error {
	conn, err := NewProducer(PConfig{Server: addr, Topic: topic})
	if err != nil {
		return err
	}
	defer conn.Stop()
	return conn.Publish(`0`)
}

func (handle *ConsumerHandle) Run(topic string, channel string, workNum uint, callback func(msg string, args ...string) error) error {
	handle.opLock.Lock()
	defer handle.opLock.Unlock()
	if handle.isStop {
		return errors.New(`已退出,禁止创建消费者`)
	}
	topic = strings.TrimSpace(topic)
	channel = strings.TrimSpace(channel)
	workNum = max(1, workNum)
	if len(topic) == 0 || len(channel) == 0 || callback == nil {
		return errors.New(`topic,channel,callback不能为空`)
	}
	for i := 0; i < int(workNum); i++ {
		conn, err := nsq.NewConsumer(topic, channel, nsq.NewConfig())
		if err != nil {
			return err
		}
		conn.SetLoggerLevel(nsq.LogLevelError)
		conn.AddHandler(&Handle{handle: func(msg string, args ...string) error {
			if msg == `0` {
				return nil
			}
			handle.workWg.Add(1)
			defer handle.workWg.Done()
			handle.setRun(1)
			defer handle.setRun(-1)
			return callback(msg, args...)
		}, topic: topic, channel: channel})
		if len(handle.addr) == 0 {
			handle.addr = fmt.Sprintf(`%s:%d`, handle.host, handle.port)
		}
		err = conn.ConnectToNSQLookupd(handle.addr)
		if err != nil {
			return err
		}
		handle.conns = append(handle.conns, conn)
	}
	return nil
}

func (handle *ConsumerHandle) Stop() {
	handle.stopLock.Lock()
	defer handle.stopLock.Unlock()
	if handle.isStop {
		return
	}
	logs.Info(`正在退出...`)
	handle.isStop = true
	for i := range handle.conns {
		handle.conns[i].Stop()
	}
	logs.Info(`conns已关闭...`)
	go func(handle *ConsumerHandle) {
		for handle.run > 0 {
			logs.Info(`(%d)休眠中...`, handle.run)
			time.Sleep(time.Second * 1)
		}
	}(handle)
	handle.workWg.Wait()
	logs.Info(`run任务数:%d`, handle.run)
	logs.Info(`退出已完成`)
}
