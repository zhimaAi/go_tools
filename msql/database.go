package msql

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

const (
	// DriverMysql 表示 MySQL 驱动名，可用于 RegisterDataBase 的 driverName 参数。
	DriverMysql = "mysql"
	// DriverPostgres 表示 PostgreSQL 驱动名，可用于 RegisterDataBase 的 driverName 参数。
	DriverPostgres = "postgres"
	// DefaultAlias 表示未指定 name 时使用的默认数据库别名。
	DefaultAlias = "default"

	paramSeat           = "__msql_param_seat__"
	sqlLogQueryMaxRunes = 0
	sqlLogArgMaxRunes   = 32
)

// Params 表示一行查询结果，key 为字段名，value 为字段值字符串。
type Params map[string]string

// Column 表示按某个字段聚合后的查询结果集合。
type Column map[string]Params

// Datas 表示写入或更新数据，key 为字段名，value 为字段值。
type Datas map[string]any

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

var (
	dataBases   = make(map[string]*dataBase)
	dataBasesMu sync.RWMutex
)

// RegisterDataBase 注册数据库连接。
//
// name 为空时使用 default 作为别名；driverName 为空时默认使用 DriverMysql。
// 同一别名只能注册一次，进程退出或不再使用时可调用 CloseAllRegDataBase 关闭连接。
//
// 示例：
//
//	err := msql.RegisterDataBase("default", "user:pass@tcp(127.0.0.1:3306)/demo")
//	err = msql.RegisterDataBase("pg", conn, msql.DriverPostgres)
func RegisterDataBase(name, conn string, driverName ...string) error {
	var emptyName bool
	if name == "" {
		emptyName = true
	}
	name = registeredAliasName(name)
	if isDataBaseRegistered(name) {
		return duplicateAliasError(emptyName)
	}
	if conn == "" {
		return errors.New("the database connection parameter cannot be empty")
	}
	alias := &dataBase{
		name: name,
		conn: conn,
		life: time.Second * 10,
		open: 50,
		idle: 25,
		db:   nil,
	}
	if err := sqlOpen(alias, driverName...); err != nil {
		return err
	}
	if !insertDataBaseAlias(name, alias) {
		_ = closeAliasDB(alias)
		return duplicateAliasError(emptyName)
	}
	return nil
}

// CloseAllRegDataBase 关闭所有已注册数据库连接，并清空注册表。
//
// 如果多个连接关闭失败，会将错误合并后返回。
func CloseAllRegDataBase() error {
	var errs []error
	for name, alias := range takeDataBaseAliases() {
		if err := closeAliasDB(alias); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

// SetConnMaxLifetime 设置指定数据库连接的最大生命周期。
//
// name 为空时使用 default 连接。
func SetConnMaxLifetime(name string, d time.Duration) error {
	return useDataBaseAlias(name, func(alias *dataBase) {
		setAliasConnMaxLifetime(alias, d)
	})
}

// SetMaxOpenConns 设置指定数据库连接的最大打开连接数。
//
// name 为空时使用 default 连接。
func SetMaxOpenConns(name string, n int) error {
	return useDataBaseAlias(name, func(alias *dataBase) {
		setAliasMaxOpenConns(alias, n)
	})
}

// SetMaxIdleConns 设置指定数据库连接的最大空闲连接数。
//
// name 为空时使用 default 连接。
func SetMaxIdleConns(name string, n int) error {
	return useDataBaseAlias(name, func(alias *dataBase) {
		setAliasMaxIdleConns(alias, n)
	})
}

// SetDebug 开启或关闭指定连接的 SQL 调试日志。
//
// 调试日志会单行输出 SQL 和参数，参数中的特殊字符会转义，过长参数会截断。
func SetDebug(name string, dev bool) error {
	return useDataBaseAlias(name, func(alias *dataBase) {
		setAliasDebug(alias, dev)
	})
}

// GetDB 返回已注册连接对应的 *sql.DB。
//
// name 为空时使用 default 连接。返回的 *sql.DB 由注册表持有，通常不应由调用方单独关闭。
func GetDB(name string) (*sql.DB, error) {
	if alias, ok := lookupDataBase(name); ok && alias != nil {
		return aliasDB(alias), nil
	}
	return nil, errors.New("the database alias does not exist")
}

// Begin 基于指定连接开启原生 database/sql 事务。
//
// name 为空时使用 default 连接。链式模型事务通常使用 Model(...).Begin()。
func Begin(name string) (*sql.Tx, error) {
	alias, err := getDB(name)
	if err != nil {
		return nil, err
	}
	return alias.db.Begin()
}

// RawValues 执行原始查询 SQL，并将结果按 []Params 返回。
//
// tx 不为空时使用传入事务执行；args 会透传给 database/sql 做参数绑定。
// 调用方需要自行保证 query 中的表名、字段名和 SQL 片段可信。
//
// 示例：
//
//	rows, err := msql.RawValues("", "select id,name from users where id=?", nil, 1)
func RawValues(name, query string, tx *sql.Tx, args ...any) ([]Params, error) {
	db, err := getExecDB(name, query, tx, args)
	if err != nil {
		return nil, err
	}
	var rows *sql.Rows
	if tx == nil {
		rows, err = db.Query(query, args...)
	} else {
		rows, err = tx.Query(query, args...)
	}
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)
	cols, err2 := rows.Columns()
	if err2 != nil {
		return nil, err2
	}
	list := make([]Params, 0)
	for rows.Next() {
		row := make([]any, len(cols))
		for i := range row {
			row[i] = &sql.NullString{}
		}
		if err := rows.Scan(row...); err != nil {
			return nil, err
		}
		item := Params{}
		for i, v := range row {
			if s, ok := v.(*sql.NullString); ok {
				item[cols[i]] = s.String
			} else {
				item[cols[i]] = ""
			}
		}
		list = append(list, item)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return list, nil
}

// RawExec 执行原始写入 SQL，并返回 sql.Result。
//
// tx 不为空时使用传入事务执行；args 会透传给 database/sql 做参数绑定。
// 调用方需要自行保证 query 中的表名、字段名和 SQL 片段可信。
//
// 示例：
//
//	ret, err := msql.RawExec("", "update users set name=? where id=?", nil, "tom", 1)
func RawExec(name, query string, tx *sql.Tx, args ...any) (sql.Result, error) {
	db, err := getExecDB(name, query, tx, args)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return db.Exec(query, args...)
	}
	return tx.Exec(query, args...)
}
