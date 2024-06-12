package msql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/zhimaAi/go_tools/tool"
)

func Model(table string, name ...string) *db {
	if len(name) < 1 {
		name = append(name, "default")
	}
	m := &db{table: table, name: name[0]}
	return m
}

func (m *db) Reset() {
	m.field = nil
	m.alias = ""
	m.join = nil
	m.where = nil
	m.whereor = nil
	m.group = nil
	m.order = nil
	m.having = nil
	m.limit = 0
	m.offset = 0
}

type db struct {
	name    string
	field   []string
	table   string
	alias   string
	join    []string
	where   []string
	whereor []string
	group   []string
	having  []string
	order   []string
	lastid  int64
	affect  int64
	lastsql string
	limit   int
	offset  int
	istx    bool
	tx      *sql.Tx
}

func (m *db) Name(name string) *db {
	if m.istx || name == "" {
		return m
	}
	m.name = name
	return m
}

func (m *db) Field(field string) *db {
	if field == "" {
		return m
	}
	if m.field == nil {
		m.field = []string{}
	}
	m.field = append(m.field, field)
	return m
}

func (m *db) Table(table string) *db {
	if table == "" {
		return m
	}
	m.table = table
	return m
}

func (m *db) Alias(alias string) *db {
	if alias == "" {
		return m
	}
	m.alias = alias
	return m
}

func (m *db) Join(join, condition, cate string) *db {
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
	return m
}

func toWhere(a []string) string {
	if a == nil || len(a) < 1 || a[0] == "" {
		return ""
	}
	if len(a) == 1 {
		return a[0]
	}
	if len(a) == 2 {
		if a[0] == "" {
			return a[1]
		}
		a = append(a, ToString(a[1]))
		a[1] = "="
	} else if len(a) == 3 {
		if strings.EqualFold(a[1], "in") ||
			strings.EqualFold(a[1], "not in") {
			a[1] = " " + a[1]
			a[2] = "(" + Assemble(a[2]) + ")"
		} else if strings.EqualFold(a[1], "like") ||
			strings.EqualFold(a[1], "not like") {
			a[1] = " " + a[1] + " "
			a[2] = ToLike(a[2])
		} else if strings.EqualFold(a[1], "between") ||
			strings.EqualFold(a[1], "not between") {
			bs := strings.Split(a[2], ",")
			if len(bs) != 2 {
				return ""
			}
			a[1] = " " + a[1] + " "
			a[2] = ToString(bs[0]) + " and " + ToString(bs[1])
		} else if tool.InArrayString(a[1],
			[]string{">", ">=", "<", "<=", "!=", "<>", "="}) {
			a[2] = ToString(a[2])
		} else if strings.EqualFold(a[0], "find_in_set") {
			if a[1] == "" || a[2] == "" {
				return ""
			}
			a[2] = "find_in_set(" + a[1] + "," + a[2] + ")"
			a[0], a[1] = "", ""
		} else {
			return ""
		}
	} else {
		return ""
	}
	fs := strings.Split(a[0], "|")
	if len(fs) > 1 {
		for i, f := range fs {
			fs[i] = f + a[1] + a[2]
		}
		return "(" + tool.Array2String(fs, " or ") + ")"
	} else {
		return a[0] + a[1] + a[2]
	}
}

func (m *db) Where(a ...string) *db {
	where := toWhere(a)
	if where == "" {
		return m
	}
	if m.where == nil {
		m.where = []string{}
	}
	m.where = append(m.where, where)
	return m
}

func (m *db) Where2(l [][]string) *db {
	if l == nil || len(l) == 0 {
		return m
	}
	for _, a := range l {
		m.Where(a...)
	}
	return m
}

func (m *db) WhereOr(a ...string) *db {
	whereor := toWhere(a)
	if whereor == "" {
		return m
	}
	if m.whereor == nil {
		m.whereor = []string{}
	}
	m.whereor = append(m.whereor, whereor)
	return m
}

func (m *db) WhereOr2(l [][]string) *db {
	if l == nil || len(l) == 0 {
		return m
	}
	for _, a := range l {
		m.WhereOr(a...)
	}
	return m
}

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

func (m *db) Having(having string) *db {
	if having == "" {
		return m
	}
	if m.having == nil {
		m.having = []string{}
	}
	m.having = append(m.having, having)
	return m
}

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

func (m *db) GetLastInsertId() int64 {
	return m.lastid
}

func (m *db) GetRowsAffected() int64 {
	return m.affect
}

func (m *db) GetLastSql() string {
	return m.lastsql
}

func GetAsField(field string) string {
	if field == "" {
		return ""
	}
	l := strings.Split(field, " ")
	if len(l) < 1 {
		return ""
	}
	return l[len(l)-1]
}

func (m *db) getFields() string {
	if len(m.field) == 0 {
		return "*"
	}
	return strings.Join(m.field, ",")
}

func (m *db) getWhere() string {
	wh := tool.Array2String(m.where, " and ")
	or := tool.Array2String(m.whereor, " or ")
	if wh == "" {
		if or == "" {
			return ""
		} else {
			return "where " + or
		}
	} else {
		if or == "" {
			return "where " + wh
		} else {
			return "where " + wh + " or " + or
		}
	}
}

func (m *db) getLimit() string {
	if m.limit < 1 {
		return ""
	}
	s := "limit " + tool.Int2String(m.limit)
	if m.offset > 0 {
		s += " offset " + tool.Int2String(m.offset)
	}
	return s
}

func (m *db) getGroups() string {
	groups := tool.Array2String(m.group, ",")
	if groups == "" {
		return ""
	}
	return "group by " + groups
}

func (m *db) getHavings() string {
	havings := tool.Array2String(m.having, " and ")
	if havings == "" {
		return ""
	}
	return "having " + havings
}

func (m *db) getOrders() string {
	orders := tool.Array2String(m.order, ",")
	if orders == "" {
		return ""
	}
	return "order by " + orders
}

func (m *db) BuildSql() string {
	sl := []string{
		"select",
		m.getFields(),
		"from",
		ToField(m.table),
		m.alias,
		tool.Array2String(m.join, " "),
		m.getWhere(),
		m.getGroups(),
		m.getHavings(),
		m.getOrders(),
		m.getLimit(),
	}
	return tool.Array2String(sl, " ")
}

func (m *db) buildCount(field string) string {
	if field == "" {
		field = "*"
	}
	group := m.getGroups()
	sl := []string{
		"select",
		"count(" + field + ") total",
		"from",
		ToField(m.table),
		m.alias,
		tool.Array2String(m.join, " "),
		m.getWhere(),
		group,
		m.getHavings(),
	}
	query := tool.Array2String(sl, " ")
	if group != "" {
		//goland:noinspection Annotator
		query = "select count(*) total from (" + query + ") gc"
	}
	query += " limit 1"
	return query
}

func (m *db) pageCount() int {
	query := m.buildCount("*")
	m.lastsql = query
	vs, e := RawValues(m.name, query, m.tx)
	if e != nil || len(vs) < 1 {
		return 0
	}
	return tool.Intval(vs[0]["total"])
}

func (m *db) Count(field ...string) (int, error) {
	defer m.Reset()
	if field == nil || len(field) == 0 {
		field = []string{"*"}
	}
	query := m.buildCount(field[0])
	m.lastsql = query
	vs, e := RawValues(m.name, query, m.tx)
	if e != nil || len(vs) < 1 {
		return 0, e
	}
	return tool.Intval(vs[0]["total"]), e
}

func (m *db) Select() (list []Params, err error) {
	defer m.Reset()
	query := m.BuildSql()
	m.lastsql = query
	list, err = RawValues(m.name, query, m.tx)
	if err != nil {
		list = []Params{}
	}
	return
}

func (m *db) Find() (Params, error) {
	defer m.Reset()
	m.Limit(1)
	query := m.BuildSql()
	m.lastsql = query
	list, err := RawValues(m.name, query, m.tx)
	if err != nil {
		return Params{}, err
	}
	if len(list) < 1 {
		return Params{}, nil
	}
	return list[0], nil
}

func (m *db) Value(field string) (string, error) {
	defer m.Reset()
	m.field = nil
	m.Field(field)
	m.Limit(1)
	query := m.BuildSql()
	m.lastsql = query
	list, err := RawValues(m.name, query, m.tx)
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

func (m *db) Sum(field string) (string, error) {
	field = "sum(" + field + ")"
	return m.Value(field)
}

func (m *db) Min(field string) (string, error) {
	field = "min(" + field + ")"
	return m.Value(field)
}

func (m *db) Max(field string) (string, error) {
	field = "max(" + field + ")"
	return m.Value(field)
}

func (m *db) Avg(field string) (string, error) {
	field = "avg(" + field + ")"
	return m.Value(field)
}

func (m *db) Paginate(page, limit int) (list []Params, total int, err error) {
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = 15
	}
	m.Limit((page-1)*limit, limit)
	total = m.pageCount()
	defer m.Reset()
	query := m.BuildSql()
	m.lastsql = query
	list, err = RawValues(m.name, query, m.tx)
	if err != nil {
		list = []Params{}
	}
	return
}

func (m *db) ColumnArr(field string) (array []string, err error) {
	defer m.Reset()
	m.Field(field)
	array = []string{}
	query := m.BuildSql()
	m.lastsql = query
	l, e := RawValues(m.name, query, m.tx)
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

func (m *db) ColumnObj(field, key string) (object Params, err error) {
	defer m.Reset()
	m.Field(field)
	m.Field(key)
	object = Params{}
	query := m.BuildSql()
	m.lastsql = query
	l, e := RawValues(m.name, query, m.tx)
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

func (m *db) ColumnMap(field, key string) (list Column, err error) {
	defer m.Reset()
	m.Field(field)
	m.Field(key)
	list = Column{}
	query := m.BuildSql()
	m.lastsql = query
	l, e := RawValues(m.name, query, m.tx)
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

func (m *db) Insert(data Datas, returning ...string) (int64, error) {
	if len(data) < 1 {
		return 0, errors.New("insert data cannot be null")
	}
	defer m.Reset()
	fields := make([]string, len(data))
	seats := make([]string, len(data))
	values := make([]interface{}, len(data))
	index := 0
	for k, v := range data {
		fields[index] = ToField(k)
		seats[index] = getSeatStr(m.name, index)
		values[index] = v
		index++
	}
	//goland:noinspection Annotator
	query := "insert into " + ToField(m.table) + " (" + tool.Array2String(fields, ", ") +
		") values (" + tool.Array2String(seats, ", ") + ")"
	if len(returning) > 0 { //兼容postgres
		query += fmt.Sprintf(` RETURNING %s`, strings.Join(returning, `,`))
	}
	m.lastsql = query + "---" + fmt.Sprint(values)
	if len(returning) > 0 { //兼容postgres
		if vs, err := RawValues(m.name, query, m.tx, values...); err == nil {
			if len(vs) > 0 {
				m.lastid = tool.String2Int64(vs[0][returning[0]])
			}
			return m.lastid, nil
		} else {
			return 0, err
		}
	}
	if ret, err := RawExec(m.name, query, m.tx, values...); err == nil {
		if id, err := ret.LastInsertId(); err == nil {
			m.lastid = id
		}
		return m.lastid, nil
	} else {
		return 0, err
	}
}

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
	values := make([]interface{}, len(data))
	index := 0
	for k, v := range data {
		fields[index] = ToField(k) + " = " + getSeatStr(m.name, index)
		values[index] = v
		index++
	}
	query := "update " + ToField(m.table) + " set " +
		tool.Array2String(fields, ", ") + " " + where
	m.lastsql = query + "---" + fmt.Sprint(values)
	if ret, err := RawExec(m.name, query, m.tx, values...); err == nil {
		if id, err := ret.RowsAffected(); err == nil {
			m.affect = id
		}
		return m.affect, nil
	} else {
		return 0, err
	}
}

func (m *db) Update2(sqlraw string) (int64, error) {
	if sqlraw == "" {
		return 0, errors.New("update data cannot be null")
	}
	where := m.getWhere()
	if where == "" {
		return 0, errors.New("where condition cannot be null")
	}
	defer m.Reset()
	query := "update " + ToField(m.table) + " set " + sqlraw + " " + where
	m.lastsql = query
	if ret, err := RawExec(m.name, query, m.tx); err == nil {
		if id, err := ret.RowsAffected(); err == nil {
			m.affect = id
		}
		return m.affect, nil
	} else {
		return 0, err
	}
}

func (m *db) Delete() (int64, error) {
	where := m.getWhere()
	if where == "" {
		return 0, errors.New("where condition cannot be null")
	}
	defer m.Reset()
	//goland:noinspection Annotator
	query := "delete from " + ToField(m.table) + " " + where
	m.lastsql = query
	if ret, err := RawExec(m.name, query, m.tx); err == nil {
		if id, err := ret.RowsAffected(); err == nil {
			m.affect = id
		}
		return m.affect, nil
	} else {
		return 0, err
	}
}

func (m *db) TableExists() (bool, error) {
	table := ToString(m.table)
	if table == "''" {
		return false, errors.New("the table name cannot be empty")
	}
	query := "show tables like " + table
	m.lastsql = query
	vs, err := RawValues(m.name, query, m.tx)
	if err != nil {
		return false, err
	}
	if len(vs) == 1 && len(vs[0]) == 1 {
		for _, t := range vs[0] {
			if ToString(t) == table {
				return true, nil
			}
		}
	}
	return false, nil
}

func (m *db) FieldExists(field string) (bool, error) {
	table := ToField(m.table)
	if table == "``" {
		return false, errors.New("the table name cannot be empty")
	}
	field = ToField(field)
	if field == "``" {
		return false, errors.New("the field name cannot be empty")
	}
	query := "describe " + table + " " + field
	m.lastsql = query
	vs, err := RawValues(m.name, query, m.tx)
	if err != nil {
		return false, err
	}
	if len(vs) == 1 && ToField(vs[0]["Field"]) == field {
		return true, nil
	}
	return false, nil
}

func (m *db) IndexExists(keyname string) (bool, error) {
	table := ToField(m.table)
	if table == "``" {
		return false, errors.New("the table name cannot be empty")
	}
	if keyname == "" {
		return false, errors.New("the index name cannot be empty")
	}
	query := "show index from " + table
	m.lastsql = query
	vs, err := RawValues(m.name, query, m.tx)
	if err != nil {
		return false, err
	}
	for _, v := range vs {
		if v["Key_name"] == keyname {
			return true, nil
		}
	}
	return false, nil
}

func (m *db) GetFields() ([]string, error) {
	table := ToField(m.table)
	if table == "``" {
		return nil, errors.New("the table name cannot be empty")
	}
	query := "describe " + table
	m.lastsql = query
	vs, err := RawValues(m.name, query, m.tx)
	if err != nil {
		return nil, err
	}
	fields := make([]string, len(vs))
	for k, v := range vs {
		fields[k] = v["Field"]
	}
	return fields, nil
}

var (
	TxE0 = errors.New("transaction not begin")
	TxE1 = errors.New("transaction already begin")
)

func (m *db) Begin() error {
	if m.istx {
		return TxE1
	}
	tx, err := Begin(m.name)
	if err == nil {
		m.istx, m.tx = true, tx
	}
	return err
}

func (m *db) Commit() error {
	if !m.istx {
		return TxE0
	}
	err := m.tx.Commit()
	if err == nil {
		m.istx, m.tx = false, nil
	} else if errors.Is(err, sql.ErrTxDone) {
		return TxE0
	}
	return err
}

func (m *db) Rollback() error {
	if !m.istx {
		return TxE0
	}
	err := m.tx.Rollback()
	if err == nil {
		m.istx, m.tx = false, nil
	} else if errors.Is(err, sql.ErrTxDone) {
		return TxE0
	}
	return err
}
