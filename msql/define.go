package msql

import (
	"database/sql"
	"errors"
	"sync"
	"time"
)

// 数据库驱动和默认别名常量。
const (
	// DriverMysql 表示 MySQL 驱动名，可用于 RegisterDataBase 的 driverName 参数。
	DriverMysql = "mysql"
	// DriverPostgres 表示 PostgreSQL 驱动名，可用于 RegisterDataBase 的 driverName 参数。
	DriverPostgres = "postgres"
	// DefaultAlias 表示未指定 name 时使用的默认数据库别名。
	DefaultAlias = "default"
)

// Params 表示一行查询结果，key 为字段名，value 为字段值字符串。
type Params map[string]string

// Column 表示按某个字段聚合后的查询结果集合。
type Column map[string]Params

// Datas 表示写入或更新数据，key 为字段名，value 为字段值。
type Datas map[string]any

// 事务状态错误。
var (
	// TxE0 表示事务尚未开始或已经结束。
	TxE0 = errors.New("transaction not begin")
	// TxE1 表示事务已经开始，不能重复开启。
	TxE1 = errors.New("transaction already begin")
)

var errEmptyTableName = errors.New("the table name cannot be empty")

// SQL 渲染和日志常量。
const (
	// paramSeat 是包内构造 SQL 时使用的临时占位符，执行前会转换为具体驱动的占位符。
	paramSeat = "__msql_param_seat__"
	// sqlLogQueryMaxRunes 控制调试日志中 SQL 文本的最大 rune 数，0 表示不截断。
	sqlLogQueryMaxRunes = 0
	// sqlLogArgMaxRunes 控制调试日志中单个参数的最大 rune 数。
	sqlLogArgMaxRunes = 32
)

// Builder 保存一次表级链式 SQL 构造和执行过程中的临时状态。
//
// Builder 不是数据库连接或连接池；数据库连接由 RegisterDataBase 注册并由包内的 dataBase 管理。
// Builder 应通过 Model 创建。零值 Builder 或空表名 Builder 可以调用链式方法补齐配置，
// 但执行查询、写入或表结构检查前必须设置有效表名。
//
// Builder 会在链式调用过程中修改自身状态，且执行方法会重置部分临时条件；它不应被多个 goroutine
// 并发复用，也不应在使用后复制。需要并发构造 SQL 时，应为每条调用链单独创建 Builder。
type Builder struct {
	name        string
	field       []string
	fieldArgs   []any
	table       string
	alias       string
	join        []string
	joinArgs    []any
	where       []string
	whereor     []string
	whereArgs   []any
	whereorArgs []any
	group       []string
	having      []string
	havingArgs  []any
	order       []string
	lastid      int64
	affect      int64
	lastsql     string
	limit       int
	offset      int
	istx        bool
	tx          *sql.Tx
}

// dataBase 保存单个已注册数据库连接及其连接池配置。
type dataBase struct {
	mu     sync.RWMutex
	name   string
	conn   string
	driver string
	life   time.Duration
	open   int
	idle   int
	db     *sql.DB
	dev    bool
}
