package msql

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"time"
)

type Params map[string]string
type Column map[string]Params
type Datas map[string]interface{}

type dataBase struct {
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
	dataBases = make(map[string]*dataBase)
)

func sqlOpen(alias *dataBase, driverName ...string) error {
	driver := `mysql` //默认值
	if len(driverName) > 0 && len(driverName[0]) > 0 {
		driver = driverName[0]
	}
	alias.driver = driver //保留db类型
	db, err := sql.Open(driver, alias.conn)
	if err != nil {
		return err
	}
	if err := db.Ping(); err != nil {
		return err
	}
	db.SetConnMaxLifetime(alias.life)
	db.SetMaxOpenConns(alias.open)
	db.SetMaxIdleConns(alias.idle)
	alias.db = db
	return nil
}

func getSeatStr(name string, index int) string {
	if dataBases[name].driver == "postgres" {
		return fmt.Sprintf(`$%d`, index+1)
	}
	return "?"
}

func RegisterDataBase(name, conn string, driverName ...string) error {
	if name == "" {
		if _, ok := dataBases["default"]; ok {
			return errors.New("the database alias cannot be empty")
		}
		name = "default"
	} else {
		if _, ok := dataBases[name]; ok {
			return errors.New("the database alias already exists")
		}
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
	dataBases[name] = alias
	return nil
}

func SetConnMaxLifetime(name string, d time.Duration) error {
	if name == "" {
		name = "default"
	}
	if alias, ok := dataBases[name]; ok {
		alias.life = d
		alias.db.SetConnMaxLifetime(d)
		return nil
	} else {
		return errors.New("the database alias does not exist")
	}
}

func SetMaxOpenConns(name string, n int) error {
	if name == "" {
		name = "default"
	}
	if alias, ok := dataBases[name]; ok {
		alias.open = n
		alias.db.SetMaxOpenConns(n)
		return nil
	} else {
		return errors.New("the database alias does not exist")
	}
}

func SetMaxIdleConns(name string, n int) error {
	if name == "" {
		name = "default"
	}
	if alias, ok := dataBases[name]; ok {
		alias.idle = n
		alias.db.SetMaxIdleConns(n)
		return nil
	} else {
		return errors.New("the database alias does not exist")
	}
}

func SetDebug(name string, dev bool) error {
	if name == "" {
		name = "default"
	}
	if alias, ok := dataBases[name]; ok {
		alias.dev = dev
		return nil
	} else {
		return errors.New("the database alias does not exist")
	}
}

func getDB(name string) (alias *dataBase, err error) {
	if name == "" {
		name = "default"
	}
	if alias, ok := dataBases[name]; ok {
		return alias, nil
	} else {
		return nil, errors.New("the database alias does not exist")
	}
}

func Begin(name string) (*sql.Tx, error) {
	alias, err := getDB(name)
	if err != nil {
		return nil, err
	}
	return alias.db.Begin()
}

func RawValues(name, query string, tx *sql.Tx, args ...interface{}) ([]Params, error) {
	alias, err := getDB(name)
	if err != nil {
		return nil, err
	}
	if alias.dev {
		s := "[sql][" + alias.name + "][" + time.Now().Format("2006-01-02 15:04:05.000") + "][" + query + "]"
		fmt.Println(s, args)
	}
	var rows *sql.Rows
	if tx == nil {
		rows, err = alias.db.Query(query, args...)
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
		row := make([]interface{}, len(cols))
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
	return list, nil
}

func RawExec(name, query string, tx *sql.Tx, args ...interface{}) (sql.Result, error) {
	alias, err := getDB(name)
	if err != nil {
		return nil, err
	}
	if alias.dev {
		s := "[sql][" + alias.name + "][" + time.Now().Format("2006-01-02 15:04:05.000") + "][" + query + "]"
		fmt.Println(s, args)
	}
	if tx == nil {
		return alias.db.Exec(query, args...)
	} else {
		return tx.Exec(query, args...)
	}
}
