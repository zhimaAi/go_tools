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
// name 为空时使用 default 连接。链式 Builder 事务通常使用 Model(...).Begin()。
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

// 已注册数据库别名表及其并发保护。
var (
	// dataBases 保存当前进程内注册的数据库别名。
	dataBases = make(map[string]*dataBase)
	// dataBasesMu 保护 dataBases 的并发读写。
	dataBasesMu sync.RWMutex
)
