package msql

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Model 创建一个表模型，用于链式构造和执行 SQL。
//
// table 为表名，name 为可选数据库别名；name 为空时使用 default 连接。
//
// 示例：
//
//	list, err := msql.Model("users").
//	    Where("status", "=", "enabled").
//	    Order("id desc").
//	    Select()
func Model(table string, name ...string) *db {
	dbName := DefaultAlias
	if len(name) > 0 && name[0] != "" {
		dbName = name[0]
	}
	m := &db{table: table, name: dbName}
	return m
}

// Reset 清空当前模型上的查询条件、字段、排序、分页等临时状态。
//
// Select、Find、Insert、Update、Delete 等执行方法通常会在执行后自动调用 Reset。
func (m *db) Reset() {
	m.field = nil
	m.fieldArgs = nil
	m.alias = ""
	m.join = nil
	m.joinArgs = nil
	m.where = nil
	m.whereor = nil
	m.whereArgs = nil
	m.whereorArgs = nil
	m.group = nil
	m.order = nil
	m.having = nil
	m.havingArgs = nil
	m.limit = 0
	m.offset = 0
}

// db 保存一次链式 SQL 构造过程中的临时状态。
//
// 类型本身不导出，但 Model 会返回 *db，调用方通过其导出方法完成查询和写入。
type db struct {
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

// Name 切换当前模型使用的数据库别名。
//
// 如果模型已经开启事务，Name 不会切换连接，避免事务跨连接执行。
func (m *db) Name(name string) *db {
	if m.istx || name == "" {
		return m
	}
	m.name = name
	return m
}

// Field 添加 select 字段或表达式。
//
// args 用于绑定字段表达式中的占位符，常见于字段子查询。
// PostgreSQL raw 字段表达式需要使用 $1、$2 等占位符；? 会按 SQL 原文保留。
//
// 示例：
//
//	msql.Model("users").Field("id").Field("name")
//	msql.Model("users").Field("(select count(*) from orders where user_id=users.id and status=?) order_count", "paid")
//	msql.Model("users", "pg").Field("(select count(*) from orders where user_id=users.id and status=$1) order_count", "paid")
func (m *db) Field(field string, args ...any) *db {
	if field == "" {
		return m
	}
	if m.field == nil {
		m.field = []string{}
	}
	m.field = append(m.field, field)
	m.fieldArgs = append(m.fieldArgs, args...)
	return m
}

// Table 修改当前模型的表名。
func (m *db) Table(table string) *db {
	if table == "" {
		return m
	}
	m.table = table
	return m
}

// Alias 设置当前表别名。
func (m *db) Alias(alias string) *db {
	if alias == "" {
		return m
	}
	m.alias = alias
	return m
}

// Join 添加 join 子句。
//
// cate 为空时默认使用 left；args 用于绑定 join SQL 中的占位符，常见于嵌套子查询。
// PostgreSQL raw join 片段需要使用 $1、$2 等占位符；? 会按 SQL 原文保留。
//
// 示例：
//
//	subSQL, subArgs := msql.Model("orders").Where("status", "=", "paid").BuildSqlPro()
//	sql, args := msql.Model("users u").
//	    Join("("+subSQL+") o", "o.user_id=u.id", "left", subArgs...).
//	    Where("u.id", "=", "1").
//	    BuildSqlPro()
//	sql, args = msql.Model("users u", "pg").
//	    Join("orders o", "o.user_id=u.id and o.status=$1", "left", "paid").
//	    Where("u.id", "=", "1").
//	    BuildSqlPro()
func (m *db) Join(join, condition, cate string, args ...any) *db {
	if join == "" || condition == "" {
		return m
	}
	if m.join == nil {
		m.join = []string{}
	}
	if cate == "" {
		cate = "left"
	}
	joins := cate + " join " + join + " on " + condition
	m.join = append(m.join, joins)
	m.joinArgs = append(m.joinArgs, args...)
	return m
}

// Where 添加 AND 查询条件。
//
// 支持以下常用形式：
//
//	Where("id", "1") 等价于 id = ?
//	Where("id", "=", "1")
//	Where("status", "in", "open,done")
//	Where("name", "like", "tom")
//	Where("age", "between", "18,30")
//
// 三段式条件会使用参数绑定；单参数原始 SQL 条件会原样拼接，调用方需保证可信。
// PostgreSQL 三段式条件由包内自动渲染占位符；单参数原始 SQL 条件不接收绑定参数。
func (m *db) Where(a ...string) *db {
	where, args := toWhere(a)
	if where == "" {
		return m
	}
	if m.where == nil {
		m.where = []string{}
	}
	m.where = append(m.where, where)
	m.whereArgs = append(m.whereArgs, args...)
	return m
}

// WhereIn 添加字段 IN 条件，values 会按绑定参数传递。
//
// 适合需要传入非字符串值，或值中可能包含逗号的场景；空 values 不会追加条件。
//
// 示例：
//
//	msql.Model("users").WhereIn("id", 1, 2, 3)
//	msql.Model("users").WhereIn("email", "a,b@example.com", "c@example.com")
func (m *db) WhereIn(field string, values ...any) *db {
	if field == "" || len(values) == 0 {
		return m
	}
	seats := make([]string, len(values))
	for i := range values {
		seats[i] = paramSeat
	}
	if m.where == nil {
		m.where = []string{}
	}
	m.where = append(m.where, field+" in("+strings.Join(seats, ",")+")")
	m.whereArgs = append(m.whereArgs, values...)
	return m
}

// WhereBetween 添加字段 BETWEEN 条件，start 和 end 会按绑定参数传递。
//
// 示例：
//
//	msql.Model("users").WhereBetween("created_at", "2026-06-01", "2026-06-30")
//	msql.Model("orders").WhereBetween("amount", 100, 500)
func (m *db) WhereBetween(field string, start, end any) *db {
	if field == "" {
		return m
	}
	if m.where == nil {
		m.where = []string{}
	}
	m.where = append(m.where, field+" between "+paramSeat+" and "+paramSeat)
	m.whereArgs = append(m.whereArgs, start, end)
	return m
}

// Where2 批量添加 AND 查询条件。
//
// 每个子切片的含义与 Where 的参数一致。
func (m *db) Where2(l [][]string) *db {
	if l == nil || len(l) == 0 {
		return m
	}
	for _, a := range l {
		m.Where(a...)
	}
	return m
}

// WhereOr 添加 OR 查询条件。
//
// 参数规则与 Where 一致；多个 WhereOr 之间使用 OR 连接。
// PostgreSQL 三段式条件由包内自动渲染占位符；单参数原始 SQL 条件不接收绑定参数。
func (m *db) WhereOr(a ...string) *db {
	whereor, args := toWhere(a)
	if whereor == "" {
		return m
	}
	if m.whereor == nil {
		m.whereor = []string{}
	}
	m.whereor = append(m.whereor, whereor)
	m.whereorArgs = append(m.whereorArgs, args...)
	return m
}

// WhereOr2 批量添加 OR 查询条件。
//
// 每个子切片的含义与 WhereOr 的参数一致。
func (m *db) WhereOr2(l [][]string) *db {
	if l == nil || len(l) == 0 {
		return m
	}
	for _, a := range l {
		m.WhereOr(a...)
	}
	return m
}

// Group 添加 group by 字段或表达式。
func (m *db) Group(group string) *db {
	if group == "" {
		return m
	}
	if m.group == nil {
		m.group = []string{}
	}
	m.group = append(m.group, group)
	return m
}

// Order 添加 order by 字段或表达式。
//
// Order 会原样拼接传入内容，调用方需保证排序字段和方向可信。
func (m *db) Order(order string) *db {
	if order == "" {
		return m
	}
	if m.order == nil {
		m.order = []string{}
	}
	m.order = append(m.order, order)
	return m
}

// Having 添加 having 条件。
//
// args 用于绑定 having 条件中的占位符；having 片段本身会原样拼接，调用方需保证可信。
// PostgreSQL raw having 片段需要使用 $1、$2 等占位符；? 会按 SQL 原文保留。
//
// 示例：
//
//	total, err := msql.Model("orders").Group("user_id").Having("sum(amount)>?", 100).Count()
//	total, err = msql.Model("orders", "pg").Group("user_id").Having("sum(amount)>$1", 100).Count()
func (m *db) Having(having string, args ...any) *db {
	if having == "" {
		return m
	}
	if m.having == nil {
		m.having = []string{}
	}
	m.having = append(m.having, having)
	m.havingArgs = append(m.havingArgs, args...)
	return m
}

// Limit 设置 limit 或 offset + limit。
//
// 传一个参数表示 Limit(limit)，传两个参数表示 Limit(offset, limit)。
func (m *db) Limit(a ...int) *db {
	if len(a) == 0 {
		return m
	}
	if len(a) == 1 {
		m.offset = 0
		m.limit = a[0]
	} else {
		m.offset = a[0]
		m.limit = a[1]
	}
	return m
}

// GetLastInsertId 返回最近一次 Insert 得到的记录 ID。
//
// MySQL 返回数据库生成的自增 ID；PostgreSQL 返回 Insert returning 参数指定的 ID 字段值。
func (m *db) GetLastInsertId() int64 {
	return m.lastid
}

// GetRowsAffected 返回最近一次 Update、Update2 或 Delete 影响的行数。
func (m *db) GetRowsAffected() int64 {
	return m.affect
}

// GetLastSql 返回最近一次执行的 SQL 调试字符串。
func (m *db) GetLastSql() string {
	return m.lastsql
}

// GetAsField 从字段表达式中提取返回结果使用的字段名或别名。
//
// 示例：
//
//	GetAsField("count(*) total") 返回 "total"
//	GetAsField("sum(score) AS total") 返回 "total"
//	GetAsField("u.name") 返回 "name"
func GetAsField(field string) string {
	fields := strings.Fields(field)
	if len(fields) == 0 {
		return ""
	}
	if len(fields) >= 3 && strings.EqualFold(fields[len(fields)-2], "as") {
		return normalizeResultField(fields[len(fields)-1])
	}
	return normalizeResultField(fields[len(fields)-1])
}

// ToString 将字符串清理后格式化为 SQL 字符串字面量。
//
// 该函数会先复用 ToField 去掉首尾空白和包裹引号，再按 SQL 字符串字面量规则包裹单引号并转义内部单引号。
func ToString(s string) string {
	return quoteSQLValueString(ToField(s))
}

// ToField 清理字段名或表名两侧的空白和引号。
func ToField(s string) string {
	return strings.Trim(strings.TrimSpace(s), "`'\"\t ")
}

// BuildSql 返回带参数渲染结果的调试 SQL 字符串。
//
// 该方法只适合日志或调试展示，不建议将返回值作为可执行 SQL 继续传递。
//
// Deprecated: 请使用 BuildSqlPro 获取可执行 SQL 和绑定参数。
func (m *db) BuildSql() string {
	return renderDebugParamSeats(m.buildSql(), m.getQueryArgs(true))
}

// BuildSqlPro 返回可执行 SQL 和绑定参数。
//
// 示例：
//
//	sql, args := msql.Model("users").Where("id", "=", "1").BuildSqlPro()
//	rows, err := msql.RawValues("", sql, nil, args...)
func (m *db) BuildSqlPro() (string, []any) {
	rawQuery := m.buildSql()
	args := m.getQueryArgs(true)
	return renderParamSeats(m.name, rawQuery, 0), args
}

// Count 统计当前条件下的记录数。
//
// field 为空时使用 *；存在 group by 时会自动包一层子查询统计分组数量。
// field 仅接收字段或表达式文本，不支持占位符绑定；Count 也不会使用 Field(..., args...)
// 中的字段表达式参数。需要绑定条件时请使用 Where、Join 或 Having。
func (m *db) Count(field ...string) (int, error) {
	defer m.Reset()
	if field == nil || len(field) == 0 {
		field = []string{"*"}
	}
	rawQuery := m.buildCount(field[0])
	vs, e := m.queryValues(rawQuery, false)
	if e != nil || len(vs) < 1 {
		return 0, e
	}
	total, _ := strconv.Atoi(vs[0][`total`])
	return total, e
}

// Select 查询多行数据。
//
// 返回值中的每一行都是 Params，字段值统一以字符串表示。
//
// 示例：
//
//	list, err := msql.Model("users").Where("status", "=", "enabled").Select()
func (m *db) Select() (list []Params, err error) {
	defer m.Reset()
	rawQuery := m.buildSql()
	list, err = m.queryValues(rawQuery, true)
	if err != nil {
		list = []Params{}
	}
	return
}

// Find 查询单行数据。
//
// Find 会自动追加 Limit(1)，没有数据时返回空 Params 和 nil error。
//
// 示例：
//
//	user, err := msql.Model("users").Where("id", "=", "1").Find()
func (m *db) Find() (Params, error) {
	defer m.Reset()
	m.Limit(1)
	rawQuery := m.buildSql()
	list, err := m.queryValues(rawQuery, true)
	if err != nil {
		return Params{}, err
	}
	if len(list) < 1 {
		return Params{}, nil
	}
	return list[0], nil
}

// Value 查询单个字段值。
//
// Value 会自动限制一行；如果返回多个字段会返回错误。
// field 仅接收字段或表达式文本，不支持占位符绑定；Value 会清空之前 Field(..., args...)
// 中的字段表达式参数，再使用传入 field 构造单字段查询。
//
// 示例：
//
//	name, err := msql.Model("users").Where("id", "=", "1").Value("name")
func (m *db) Value(field string) (string, error) {
	defer m.Reset()
	m.field = nil
	m.fieldArgs = nil
	m.Field(field)
	m.Limit(1)
	rawQuery := m.buildSql()
	list, err := m.queryValues(rawQuery, true)
	if err != nil {
		return "", err
	}
	if len(list) < 1 {
		return "", nil
	}
	field = GetAsField(field)
	if v, ok := list[0][field]; ok {
		return v, nil
	}
	if len(list[0]) != 1 {
		return "", errors.New("return multiple fields")
	}
	for _, v := range list[0] {
		return v, nil
	}
	return "", nil
}

// Sum 查询字段求和结果。
//
// field 仅接收字段或表达式文本，不支持占位符绑定；底层会调用 Value。
func (m *db) Sum(field string) (string, error) {
	field = "sum(" + field + ")"
	return m.Value(field)
}

// Min 查询字段最小值。
//
// field 仅接收字段或表达式文本，不支持占位符绑定；底层会调用 Value。
func (m *db) Min(field string) (string, error) {
	field = "min(" + field + ")"
	return m.Value(field)
}

// Max 查询字段最大值。
//
// field 仅接收字段或表达式文本，不支持占位符绑定；底层会调用 Value。
func (m *db) Max(field string) (string, error) {
	field = "max(" + field + ")"
	return m.Value(field)
}

// Avg 查询字段平均值。
//
// field 仅接收字段或表达式文本，不支持占位符绑定；底层会调用 Value。
func (m *db) Avg(field string) (string, error) {
	field = "avg(" + field + ")"
	return m.Value(field)
}

// Paginate 按页查询数据，并返回总记录数。
//
// page 小于 1 时按 1 处理；limit 小于 1 时按 15 处理。
//
// 示例：
//
//	list, total, err := msql.Model("users").Where("status", "=", "enabled").Paginate(1, 20)
func (m *db) Paginate(page, limit int) (list []Params, total int, err error) {
	defer m.Reset()
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = 15
	}
	m.Limit((page-1)*limit, limit)
	total, err = m.pageCount()
	if err != nil {
		list = []Params{}
		return
	}
	rawQuery := m.buildSql()
	list, err = m.queryValues(rawQuery, true)
	if err != nil {
		list = []Params{}
	}
	return
}

// ColumnArr 查询单列数据，并按顺序返回字符串切片。
//
// 示例：
//
//	ids, err := msql.Model("users").ColumnArr("id")
func (m *db) ColumnArr(field string) (array []string, err error) {
	defer m.Reset()
	m.Field(field)
	array = []string{}
	rawQuery := m.buildSql()
	l, e := m.queryValues(rawQuery, true)
	if e != nil {
		return array, e
	}
	if len(l) < 1 {
		return array, nil
	}
	field = GetAsField(field)
	for _, row := range l {
		array = append(array, row[field])
	}
	return array, nil
}

// ColumnObj 查询两列数据，并返回 key 到 field 的映射。
//
// field 是值字段，key 是键字段。
//
// 示例：
//
//	names, err := msql.Model("users").ColumnObj("name", "id")
func (m *db) ColumnObj(field, key string) (object Params, err error) {
	defer m.Reset()
	m.Field(field)
	m.Field(key)
	object = Params{}
	rawQuery := m.buildSql()
	l, e := m.queryValues(rawQuery, true)
	if e != nil {
		return object, e
	}
	if len(l) < 1 {
		return object, nil
	}
	field = GetAsField(field)
	key = GetAsField(key)
	for _, row := range l {
		object[row[key]] = row[field]
	}
	return object, nil
}

// ColumnMap 查询多行数据，并按 key 字段值映射到整行数据。
//
// 示例：
//
//	users, err := msql.Model("users").ColumnMap("id,name", "id")
func (m *db) ColumnMap(field, key string) (list Column, err error) {
	defer m.Reset()
	m.Field(field)
	m.Field(key)
	list = Column{}
	rawQuery := m.buildSql()
	l, e := m.queryValues(rawQuery, true)
	if e != nil {
		return list, e
	}
	if len(l) < 1 {
		return list, nil
	}
	key = GetAsField(key)
	for _, row := range l {
		list[row[key]] = row
	}
	return list, nil
}

// Insert 插入一行数据，并返回记录 ID。
//
// data 的 key 是字段名，value 是字段值；MySQL 返回数据库生成的自增 ID。
// PostgreSQL 可通过 returning 指定 ID 字段名，并返回该字段对应的数值。
//
// 示例：
//
//	id, err := msql.Model("users").Insert(msql.Datas{"name": "tom", "status": "enabled"})
//	id, err = msql.Model("users", "pg").Insert(msql.Datas{"name": "tom"}, "id")
func (m *db) Insert(data Datas, returning ...string) (int64, error) {
	if len(data) < 1 {
		return 0, errors.New("insert data cannot be null")
	}
	m.lastid = 0
	defer m.Reset()
	fields := make([]string, len(data))
	seats := make([]string, len(data))
	values := make([]any, len(data))
	for index, k := range sortedDataKeys(data) {
		fields[index] = ToField(k)
		seats[index] = getSeatStr(m.name, index)
		values[index] = data[k]
	}
	query := "insert into " + ToField(m.table) + " (" + strings.Join(fields, ", ") +
		") values (" + strings.Join(seats, ", ") + ")"
	if len(returning) > 0 { // 兼容postgres
		query += fmt.Sprintf(` RETURNING %s`, strings.Join(returning, `,`))
	}
	m.lastsql = renderDebugParamSeats(query, values)
	if len(returning) > 0 { // 兼容postgres
		if vs, err := RawValues(m.name, query, m.tx, values...); err == nil {
			if len(vs) > 0 {
				m.lastid, _ = strconv.ParseInt(vs[0][returning[0]], 10, 64)
			}
			return m.lastid, nil
		} else {
			return 0, err
		}
	}
	if ret, err := RawExec(m.name, query, m.tx, values...); err == nil {
		if isPostgres(m.name) {
			return 0, nil
		}
		id, err := ret.LastInsertId()
		if err != nil {
			return 0, err
		}
		m.lastid = id
		return id, nil
	} else {
		return 0, err
	}
}

// Update 按当前 where 条件更新数据，并返回影响行数。
//
// Update 要求必须存在 where 条件，避免误更新整表。
//
// 示例：
//
//	rows, err := msql.Model("users").
//	    Where("id", "=", "1").
//	    Update(msql.Datas{"name": "tom"})
func (m *db) Update(data Datas) (int64, error) {
	if len(data) < 1 {
		return 0, errors.New("update data cannot be null")
	}
	where := m.getWhere()
	if where == "" {
		return 0, errors.New("where condition cannot be null")
	}
	defer m.Reset()
	fields := make([]string, len(data))
	values := make([]any, len(data))
	for index, k := range sortedDataKeys(data) {
		fields[index] = ToField(k) + " = " + getSeatStr(m.name, index)
		values[index] = data[k]
	}
	query := "update " + ToField(m.table) + " set " +
		strings.Join(fields, ", ") + " " + where
	query = renderParamSeats(m.name, query, 0)
	whereArgs := m.getWhereArgs()
	args := append(values, whereArgs...)
	return m.execRowsAffected(query, args)
}

// Update2 使用原始 set SQL 片段按当前 where 条件更新数据，并返回影响行数。
//
// sqlraw 会原样拼接到 set 后面，调用方需保证内容可信；args 会在 where 条件参数之前传入。
// PostgreSQL raw 片段需要使用 $1、$2 等占位符；? 会按 SQL 原文保留，可用于 JSONB 运算符。
// Update2 同样要求必须存在 where 条件。
//
// 示例：
//
//	rows, err := msql.Model("users").Where("id", "=", "1").Update2("login_count=login_count+1")
//	rows, err = msql.Model("users").Where("id", "=", "1").Update2("score=?", 100)
//	rows, err = msql.Model("users", "pg").Where("id", "=", "1").Update2("score=$1", 100)
func (m *db) Update2(sqlraw string, args ...any) (int64, error) {
	if sqlraw == "" {
		return 0, errors.New("update data cannot be null")
	}
	where := m.getWhere()
	if where == "" {
		return 0, errors.New("where condition cannot be null")
	}
	defer m.Reset()
	query := "update " + ToField(m.table) + " set " + sqlraw + " " + where
	query = renderParamSeats(m.name, query, 0)
	whereArgs := m.getWhereArgs()
	execArgs := make([]any, 0, len(args)+len(whereArgs))
	execArgs = append(execArgs, args...)
	execArgs = append(execArgs, whereArgs...)
	return m.execRowsAffected(query, execArgs)
}

// Delete 按当前 where 条件删除数据，并返回影响行数。
//
// Delete 要求必须存在 where 条件，避免误删除整表。
//
// 示例：
//
//	rows, err := msql.Model("users").Where("id", "=", "1").Delete()
func (m *db) Delete() (int64, error) {
	where := m.getWhere()
	if where == "" {
		return 0, errors.New("where condition cannot be null")
	}
	defer m.Reset()
	query := "delete from " + ToField(m.table) + " " + where
	query = renderParamSeats(m.name, query, 0)
	args := m.getWhereArgs()
	return m.execRowsAffected(query, args)
}

// TableExists 判断当前模型指定的表是否存在。
func (m *db) TableExists() (bool, error) {
	tableName := ToField(m.table)
	if tableName == "" {
		return false, errors.New("the table name cannot be empty")
	}

	var (
		query  string
		args   []any
		exists func([]Params) bool
	)
	switch {
	case isPostgres(m.name):
		query = "select table_name from information_schema.tables where table_schema = current_schema() and table_name = " + getSeatStr(m.name, 0) + " limit 1"
		args = []any{tableName}
		exists = func(vs []Params) bool {
			return len(vs) == 1
		}
	default:
		query = "show tables like " + quoteSQLValueString(tableName)
		exists = func(vs []Params) bool {
			if len(vs) == 1 && len(vs[0]) == 1 {
				for _, t := range vs[0] {
					if t == tableName {
						return true
					}
				}
			}
			return false
		}
	}

	vs, err := m.rawValues(query, args)
	if err != nil {
		return false, err
	}
	return exists(vs), nil
}

// FieldExists 判断当前表中指定字段是否存在。
func (m *db) FieldExists(field string) (bool, error) {
	table := ToField(m.table)
	if table == "" {
		return false, errors.New("the table name cannot be empty")
	}
	field = ToField(field)
	if field == "" {
		return false, errors.New("the field name cannot be empty")
	}

	var (
		query  string
		args   []any
		exists func([]Params) bool
	)
	switch {
	case isPostgres(m.name):
		query = "select column_name from information_schema.columns where table_schema = current_schema() and table_name = " + getSeatStr(m.name, 0) + " and column_name = " + getSeatStr(m.name, 1) + " limit 1"
		args = []any{table, field}
		exists = func(vs []Params) bool {
			return len(vs) == 1
		}
	default:
		query = "describe " + table + " " + field
		exists = func(vs []Params) bool {
			return len(vs) == 1 && ToField(vs[0]["Field"]) == field
		}
	}

	vs, err := m.rawValues(query, args)
	if err != nil {
		return false, err
	}
	return exists(vs), nil
}

// IndexExists 判断当前表中指定索引是否存在。
func (m *db) IndexExists(keyname string) (bool, error) {
	table := ToField(m.table)
	if table == "" {
		return false, errors.New("the table name cannot be empty")
	}
	if keyname == "" {
		return false, errors.New("the index name cannot be empty")
	}

	var (
		query  string
		args   []any
		exists func([]Params) bool
	)
	switch {
	case isPostgres(m.name):
		query = "select indexname from pg_indexes where schemaname = current_schema() and tablename = " + getSeatStr(m.name, 0) + " and indexname = " + getSeatStr(m.name, 1) + " limit 1"
		args = []any{table, keyname}
		exists = func(vs []Params) bool {
			return len(vs) == 1
		}
	default:
		query = "show index from " + table
		exists = func(vs []Params) bool {
			for _, v := range vs {
				if v["Key_name"] == keyname {
					return true
				}
			}
			return false
		}
	}

	vs, err := m.rawValues(query, args)
	if err != nil {
		return false, err
	}
	return exists(vs), nil
}

// GetFields 查询当前表的字段名列表。
func (m *db) GetFields() ([]string, error) {
	table := ToField(m.table)
	if table == "" {
		return nil, errors.New("the table name cannot be empty")
	}

	var (
		query     string
		args      []any
		fieldKeys []string
	)
	switch {
	case isPostgres(m.name):
		query = "select column_name as Field from information_schema.columns where table_schema = current_schema() and table_name = " + getSeatStr(m.name, 0) + " order by ordinal_position"
		args = []any{table}
		fieldKeys = []string{"field", "Field"}
	default:
		query = "describe " + table
		fieldKeys = []string{"Field"}
	}

	vs, err := m.rawValues(query, args)
	if err != nil {
		return nil, err
	}
	fields := make([]string, len(vs))
	for k, v := range vs {
		for _, key := range fieldKeys {
			fields[k] = v[key]
			if fields[k] != "" {
				break
			}
		}
	}
	return fields, nil
}

var (
	// TxE0 表示事务尚未开始或已经结束。
	TxE0 = errors.New("transaction not begin")
	// TxE1 表示事务已经开始，不能重复开启。
	TxE1 = errors.New("transaction already begin")
)

// Begin 在当前模型上开启事务。
//
// 开启事务后，当前模型的后续查询和写入会使用同一个事务连接，直到 Commit 或 Rollback。
//
// 示例：
//
//	m := msql.Model("users")
//	if err := m.Begin(); err != nil { return err }
//	_, err := m.Where("id", "=", "1").Update(msql.Datas{"name": "tom"})
//	if err != nil { _ = m.Rollback(); return err }
//	err = m.Commit()
func (m *db) Begin() error {
	if m.istx {
		return TxE1
	}
	tx, err := Begin(m.name)
	if err == nil {
		m.istx, m.tx = true, tx
		logSQLTxBoundary(m.name, m.table, "BEGIN")
	}
	return err
}

// Commit 提交当前模型上的事务。
func (m *db) Commit() error {
	if !m.istx || m.tx == nil {
		return TxE0
	}
	logSQLTxBoundary(m.name, m.table, "COMMIT")
	err := m.tx.Commit()
	m.istx, m.tx = false, nil
	if errors.Is(err, sql.ErrTxDone) {
		return TxE0
	}
	return err
}

// Rollback 回滚当前模型上的事务。
func (m *db) Rollback() error {
	if !m.istx || m.tx == nil {
		return TxE0
	}
	logSQLTxBoundary(m.name, m.table, "ROLLBACK")
	err := m.tx.Rollback()
	m.istx, m.tx = false, nil
	if errors.Is(err, sql.ErrTxDone) {
		return TxE0
	}
	return err
}
