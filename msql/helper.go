package msql

import (
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// getDB 获取已注册的数据库配置。
//
// name 为空时使用 default 连接。
func getDB(name string) (alias *dataBase, err error) {
	if alias, ok := lookupDataBase(name); ok && alias != nil {
		return alias, nil
	}
	return nil, errors.New("the database alias does not exist")
}

// getExecDB 获取执行 SQL 所需的连接池，并按别名配置输出调试日志。
//
// RawValues 和 RawExec 都需要在执行前完成别名查找、连接空值检查和 debug 日志输出，
// 该方法用于保持这两条执行路径的前置逻辑一致。
func getExecDB(name, query string, tx *sql.Tx, args []any) (*sql.DB, error) {
	alias, err := getDB(name)
	if err != nil {
		return nil, err
	}
	aliasName, db, dev := aliasSnapshot(alias)
	if db == nil {
		return nil, errors.New("the database connection does not exist")
	}
	if dev {
		fmt.Println(formatSQLLog(aliasName, time.Now(), tx != nil, query, args))
	}
	return db, nil
}

// queryValues 执行由 Builder 构造出的查询 SQL，并同步记录调试 SQL。
//
// rawQuery 保留内部占位符，方法内部会按当前数据库驱动渲染为可执行 SQL；
// withField 用于控制 count 场景是否跳过字段表达式参数。
func (m *Builder) queryValues(rawQuery string, withField bool) ([]Params, error) {
	query := renderParamSeats(m.name, rawQuery, 0)
	args := m.getQueryArgs(withField)
	m.lastsql = renderDebugParamSeats(rawQuery, args)
	return RawValues(m.name, query, m.tx, args...)
}

// rawValues 执行已经构造完成的原始查询 SQL，并同步记录调试 SQL。
//
// 例如 TableExists、FieldExists 这类元信息查询会自行按数据库类型生成 SQL，
// 不再经过 buildSql，因此直接使用该方法执行。
func (m *Builder) rawValues(query string, args []any) ([]Params, error) {
	m.lastsql = renderDebugParamSeats(query, args)
	return RawValues(m.name, query, m.tx, args...)
}

// execRowsAffected 执行写入 SQL，并把影响行数保存到当前 Builder。
//
// Update、Update2 和 Delete 都只关心 RawExec 的 RowsAffected 结果，
// 该方法用于统一 lastsql 记录、执行和 affect 更新逻辑。
func (m *Builder) execRowsAffected(query string, args []any) (int64, error) {
	m.affect = 0
	m.lastsql = renderDebugParamSeats(query, args)
	ret, err := RawExec(m.name, query, m.tx, args...)
	if err != nil {
		return 0, err
	}
	rows, err := ret.RowsAffected()
	if err != nil {
		return 0, err
	}
	m.affect = rows
	return rows, nil
}

// getFields 生成 select 字段列表。
//
// 未指定字段时返回 *。
func (m *Builder) getFields() string {
	if len(m.field) == 0 {
		return "*"
	}
	return strings.Join(m.field, ",")
}

// tableName 返回当前 Builder 的有效表名。
//
// 表名会复用 ToField 的清理规则；空 Builder、零值 Builder 或仅包含空白/引号的表名都会返回错误。
func (m *Builder) tableName() (string, error) {
	if m == nil {
		return "", errEmptyTableName
	}
	table := ToField(m.table)
	if table == "" {
		return "", errEmptyTableName
	}
	return table, nil
}

// getQueryArgs 按最终 SQL 出现顺序返回查询绑定参数。
//
// withField 为 false 时不包含字段表达式参数，主要用于 count 查询。
func (m *Builder) getQueryArgs(withField bool) []any {
	capacity := len(m.joinArgs) + len(m.whereArgs) + len(m.whereorArgs) + len(m.havingArgs)
	if withField {
		capacity += len(m.fieldArgs)
	}
	args := make([]any, 0, capacity)
	if withField {
		args = append(args, m.fieldArgs...)
	}
	args = append(args, m.joinArgs...)
	args = append(args, m.whereArgs...)
	args = append(args, m.whereorArgs...)
	args = append(args, m.havingArgs...)
	return args
}

// getLimit 生成 limit 和 offset 子句。
func (m *Builder) getLimit() string {
	if m.limit < 1 {
		return ""
	}
	s := "limit " + strconv.Itoa(m.limit)
	if m.offset > 0 {
		s += " offset " + strconv.Itoa(m.offset)
	}
	return s
}

// getGroups 生成 group by 子句。
func (m *Builder) getGroups() string {
	groups := strings.Join(m.group, ",")
	if groups == "" {
		return ""
	}
	return "group by " + groups
}

// getHavings 生成 having 子句。
func (m *Builder) getHavings() string {
	havings := strings.Join(m.having, " and ")
	if havings == "" {
		return ""
	}
	return "having " + havings
}

// getOrders 生成 order by 子句。
func (m *Builder) getOrders() string {
	orders := strings.Join(m.order, ",")
	if orders == "" {
		return ""
	}
	return "order by " + orders
}

// joinSQLParts 过滤空 SQL 片段后用单空格拼接，避免构造出的 SQL 出现多余空格。
func joinSQLParts(parts ...string) string {
	sl := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		sl = append(sl, part)
	}
	return strings.Join(sl, " ")
}

// buildSql 生成未渲染占位符的 select SQL。
//
// 返回值仍包含内部占位符，执行前需要通过 renderParamSeats 转换。
func (m *Builder) buildSql() (string, error) {
	table, err := m.tableName()
	if err != nil {
		return "", err
	}
	return joinSQLParts(
		"select",
		m.getFields(),
		"from",
		table,
		m.alias,
		joinSQLParts(m.join...),
		m.getWhere(),
		m.getGroups(),
		m.getHavings(),
		m.getOrders(),
		m.getLimit(),
	), nil
}

// buildCount 生成当前条件对应的 count SQL。
//
// 存在 group by 时会包装为子查询后再统计总数。
func (m *Builder) buildCount(field string) (string, error) {
	table, err := m.tableName()
	if err != nil {
		return "", err
	}
	if field == "" {
		field = "*"
	}
	group := m.getGroups()
	query := joinSQLParts(
		"select",
		"count("+field+") total",
		"from",
		table,
		m.alias,
		joinSQLParts(m.join...),
		m.getWhere(),
		group,
		m.getHavings(),
	)
	if group != "" {
		//noinspection SqlDialectInspection
		query = "select count(*) total from (" + query + ") gc"
	}
	query += " limit 1"
	return query, nil
}

// pageCount 查询分页场景下的总记录数。
//
// count 查询失败时保留错误返回给 Paginate，避免把数据库错误误表现为 total=0。
func (m *Builder) pageCount() (int, error) {
	rawQuery, err := m.buildCount("*")
	if err != nil {
		return 0, err
	}
	vs, e := m.queryValues(rawQuery, false)
	if e != nil {
		return 0, e
	}
	if len(vs) < 1 {
		return 0, nil
	}
	total, _ := strconv.Atoi(vs[0][`total`])
	return total, nil
}

// normalizeResultField 规范化结果字段名，兼容表别名限定字段和反引号包裹字段。
func normalizeResultField(field string) string {
	field = ToField(field)
	if field == "" || strings.ContainsAny(field, "()") {
		return field
	}
	if index := strings.LastIndex(field, "."); index >= 0 && index < len(field)-1 {
		return ToField(field[index+1:])
	}
	return field
}

// sortedDataKeys 返回 Datas 中按字典序排列的字段名。
//
// Insert 和 Update 使用稳定字段顺序拼接 SQL，便于日志比对、测试断言和数据库执行计划复用。
func sortedDataKeys(data Datas) []string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// sqlOpen 根据数据库配置打开连接并初始化连接池参数。
func sqlOpen(alias *dataBase, driverName ...string) error {
	var driver = DriverMysql
	if len(driverName) > 0 && len(driverName[0]) > 0 {
		driver = driverName[0]
	}
	alias.driver = driver
	db, err := sql.Open(driver, alias.conn)
	if err != nil {
		return err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return err
	}
	db.SetConnMaxLifetime(alias.life)
	db.SetMaxOpenConns(alias.open)
	db.SetMaxIdleConns(alias.idle)
	alias.db = db
	return nil
}

// lookupDataBase 返回指定别名当前注册的数据库配置。
func lookupDataBase(name string) (*dataBase, bool) {
	if name == "" {
		name = DefaultAlias
	}
	dataBasesMu.RLock()
	alias, ok := dataBases[name]
	dataBasesMu.RUnlock()
	return alias, ok
}

// duplicateAliasError 返回数据库别名重复时的兼容错误文案。
func duplicateAliasError(emptyName bool) error {
	if emptyName {
		return errors.New("the database alias cannot be empty")
	}
	return errors.New("the database alias already exists")
}

// registeredAliasName 规范化数据库别名。
func registeredAliasName(name string) string {
	if name == "" {
		return DefaultAlias
	}
	return name
}

// isDataBaseRegistered 判断数据库别名是否已经注册。
func isDataBaseRegistered(name string) bool {
	_, ok := lookupDataBase(name)
	return ok
}

// useDataBaseAlias 统一处理需要读取已注册数据库别名后再修改配置的操作。
//
// 例如 SetDebug、SetMaxOpenConns 这类入口只关心“别名存在后执行一次设置”，
// 具体设置逻辑通过 use 回调传入。
func useDataBaseAlias(name string, use func(*dataBase)) error {
	if alias, ok := lookupDataBase(name); ok && alias != nil {
		use(alias)
		return nil
	}
	return errors.New("the database alias does not exist")
}

// closeAliasDB 关闭别名持有的连接池；别名为空或连接为空时视为已关闭。
func closeAliasDB(alias *dataBase) error {
	if alias == nil {
		return nil
	}
	alias.mu.RLock()
	db := alias.db
	alias.mu.RUnlock()
	if db == nil {
		return nil
	}
	return db.Close()
}

// aliasSnapshot 一次性读取执行 SQL 所需的连接、别名和调试开关。
//
// 这样 RawValues/RawExec 在实际执行 SQL 前不需要长时间持有别名锁。
func aliasSnapshot(alias *dataBase) (aliasName string, db *sql.DB, dev bool) {
	alias.mu.RLock()
	defer alias.mu.RUnlock()
	return alias.name, alias.db, alias.dev
}

// aliasDB 返回别名当前持有的原生 *sql.DB。
func aliasDB(alias *dataBase) *sql.DB {
	alias.mu.RLock()
	defer alias.mu.RUnlock()
	return alias.db
}

// setAliasConnMaxLifetime 同步更新记录值和原生连接池配置。
func setAliasConnMaxLifetime(alias *dataBase, d time.Duration) {
	alias.mu.Lock()
	alias.life = d
	db := alias.db
	alias.mu.Unlock()
	if db != nil {
		db.SetConnMaxLifetime(d)
	}
}

// setAliasMaxOpenConns 同步更新最大打开连接数。
func setAliasMaxOpenConns(alias *dataBase, n int) {
	alias.mu.Lock()
	alias.open = n
	db := alias.db
	alias.mu.Unlock()
	if db != nil {
		db.SetMaxOpenConns(n)
	}
}

// setAliasMaxIdleConns 同步更新最大空闲连接数。
func setAliasMaxIdleConns(alias *dataBase, n int) {
	alias.mu.Lock()
	alias.idle = n
	db := alias.db
	alias.mu.Unlock()
	if db != nil {
		db.SetMaxIdleConns(n)
	}
}

// setAliasDebug 设置别名级 SQL 调试日志开关。
func setAliasDebug(alias *dataBase, dev bool) {
	alias.mu.Lock()
	alias.dev = dev
	alias.mu.Unlock()
}

// insertDataBaseAlias 在全局注册表中原子插入别名。
//
// 返回 false 表示别名已被其它调用注册，调用方应关闭新建连接并返回重复错误。
func insertDataBaseAlias(name string, alias *dataBase) bool {
	dataBasesMu.Lock()
	defer dataBasesMu.Unlock()
	if _, ok := dataBases[name]; ok {
		return false
	}
	dataBases[name] = alias
	return true
}

// takeDataBaseAliases 取走当前全部注册连接，并把全局注册表重置为空。
//
// CloseAllRegDataBase 先取走 map 再逐个关闭连接，避免关闭数据库连接时长期占用全局锁。
func takeDataBaseAliases() map[string]*dataBase {
	dataBasesMu.Lock()
	defer dataBasesMu.Unlock()
	aliases := dataBases
	dataBases = make(map[string]*dataBase)
	return aliases
}

// isPostgresDriver 判断别名是否使用 PostgreSQL 驱动。
func isPostgresDriver(alias *dataBase) bool {
	alias.mu.RLock()
	defer alias.mu.RUnlock()
	return alias.driver == DriverPostgres
}

// getSeatStr 根据连接驱动返回当前参数位置的占位符。
//
// MySQL 使用 ?，PostgreSQL 使用 $1、$2 形式。
func getSeatStr(name string, index int) string {
	alias, ok := lookupDataBase(name)
	if ok && alias != nil {
		alias.mu.RLock()
		driver := alias.driver
		alias.mu.RUnlock()
		if driver == DriverPostgres {
			return fmt.Sprintf(`$%d`, index+1)
		}
	}
	return "?"
}

// renderParamSeats 将 SQL 中的占位符渲染成当前数据库驱动可执行的占位符。
//
// MySQL 只会替换包内临时占位符；PostgreSQL 会按最终 SQL 出现顺序重新编号包内临时占位符和已有 $n 占位符。
// PostgreSQL raw 片段中的 $n 仅表示一个待绑定参数位置，不表示参数复用；每出现一个 $n 就必须有一个对应参数。
// 普通 ? 在 PostgreSQL 下会保留原文，避免误改 JSONB 等 SQL 运算符。
func renderParamSeats(name, query string, start int) string {
	if !strings.Contains(query, paramSeat) {
		if isPostgres(name) {
			return renderPostgresParamSeats(query, start)
		}
		return query
	}
	if isPostgres(name) {
		return renderPostgresParamSeats(query, start)
	}
	index := start
	for strings.Contains(query, paramSeat) {
		query = strings.Replace(query, paramSeat, getSeatStr(name, index), 1)
		index++
	}
	return query
}

// renderDebugParamSeats 将 SQL 中的占位符按参数渲染为调试展示字符串。
//
// 包内临时占位符、MySQL ? 和 PostgreSQL $n 都会按出现顺序消耗 args；
// 字符串、标识符、注释和 PostgreSQL dollar-quoted 字符串里的占位符文本会保留原文。
// 返回值仅用于日志或调试，不用于执行 SQL。
func renderDebugParamSeats(query string, args []any) string {
	var builder strings.Builder
	builder.Grow(len(query))
	argIndex := 0
	for i := 0; i < len(query); {
		if next, ok := copySQLIgnoredFragment(&builder, query, i, true, true); ok {
			i = next
			continue
		}
		if strings.HasPrefix(query[i:], paramSeat) {
			builder.WriteString(formatSQLPlaceholder(paramSeat, args, &argIndex))
			i += len(paramSeat)
			continue
		}
		if query[i] == '?' {
			builder.WriteString(formatSQLPlaceholder("?", args, &argIndex))
			i++
			continue
		}
		if end, ok := postgresParamSeatEnd(query, i); ok {
			builder.WriteString(formatSQLPlaceholder(query[i:end], args, &argIndex))
			i = end
			continue
		}
		builder.WriteByte(query[i])
		i++
	}
	return builder.String()
}

// isPostgres 判断指定数据库别名是否使用 PostgreSQL 驱动。
func isPostgres(name string) bool {
	alias, ok := lookupDataBase(name)
	return ok && alias != nil && isPostgresDriver(alias)
}

// renderPostgresParamSeats 将 SQL 中的内部占位符或旧 $n 占位符重新编号为 PostgreSQL 占位符。
//
// 嵌套子查询已经带有 $1、$2 时，会按最终 SQL 出现顺序重新编号，避免与外层参数冲突。
// $n 仅表示一个待绑定参数位置，不表示参数复用；每出现一个 $n 就必须有一个对应参数。
func renderPostgresParamSeats(query string, start int) string {
	var builder strings.Builder
	builder.Grow(len(query))
	index := start
	for i := 0; i < len(query); {
		if next, ok := copySQLIgnoredFragment(&builder, query, i, hasPostgresEscapeStringPrefix(query, i), false); ok {
			i = next
			continue
		}
		if strings.HasPrefix(query[i:], paramSeat) {
			index++
			builder.WriteString(fmt.Sprintf("$%d", index))
			i += len(paramSeat)
			continue
		}
		if end, ok := postgresParamSeatEnd(query, i); ok {
			index++
			builder.WriteString(fmt.Sprintf("$%d", index))
			i = end
			continue
		}
		builder.WriteByte(query[i])
		i++
	}
	return builder.String()
}

// copySQLIgnoredFragment 复制不应解析占位符的 SQL 片段。
//
// 字符串、标识符、注释和 PostgreSQL dollar-quoted 字符串里的占位符都应保留原文。
func copySQLIgnoredFragment(builder *strings.Builder, query string, start int, escapeBackslash, copyBacktick bool) (int, bool) {
	switch query[start] {
	case '\'':
		return copySQLSingleQuoted(builder, query, start, escapeBackslash), true
	case '"':
		return copySQLDoubleQuoted(builder, query, start), true
	case '`':
		if copyBacktick {
			return copySQLBacktickQuoted(builder, query, start), true
		}
	case '-':
		if start+1 < len(query) && query[start+1] == '-' {
			return copySQLLineComment(builder, query, start), true
		}
	case '/':
		if start+1 < len(query) && query[start+1] == '*' {
			return copySQLBlockComment(builder, query, start), true
		}
	case '$':
		if tag, ok := postgresDollarQuoteTag(query, start); ok {
			return copyPostgresDollarQuoted(builder, query, start, tag), true
		}
	}
	return start, false
}

// copySQLSingleQuoted 复制 SQL 单引号字符串，并跳过字符串内可能出现的占位符文本。
func copySQLSingleQuoted(builder *strings.Builder, query string, start int, escapeBackslash bool) int {
	builder.WriteByte(query[start])
	for i := start + 1; i < len(query); i++ {
		builder.WriteByte(query[i])
		if escapeBackslash && query[i] == '\\' && i+1 < len(query) {
			builder.WriteByte(query[i+1])
			i++
			continue
		}
		if query[i] != '\'' {
			continue
		}
		if i+1 < len(query) && query[i+1] == '\'' {
			builder.WriteByte(query[i+1])
			i++
			continue
		}
		return i + 1
	}
	return len(query)
}

// hasPostgresEscapeStringPrefix 判断单引号字符串是否带有 PostgreSQL E 前缀。
func hasPostgresEscapeStringPrefix(query string, quoteIndex int) bool {
	if quoteIndex < 1 {
		return false
	}
	prefixIndex := quoteIndex - 1
	if query[prefixIndex] != 'E' && query[prefixIndex] != 'e' {
		return false
	}
	return prefixIndex == 0 || !isSQLIdentifierPart(query[prefixIndex-1])
}

// copySQLDoubleQuoted 复制 SQL 双引号标识符，并跳过标识符内可能出现的占位符文本。
func copySQLDoubleQuoted(builder *strings.Builder, query string, start int) int {
	builder.WriteByte(query[start])
	for i := start + 1; i < len(query); i++ {
		builder.WriteByte(query[i])
		if query[i] != '"' {
			continue
		}
		if i+1 < len(query) && query[i+1] == '"' {
			builder.WriteByte(query[i+1])
			i++
			continue
		}
		return i + 1
	}
	return len(query)
}

// copySQLBacktickQuoted 复制 MySQL 反引号标识符，并跳过其中的占位符文本。
func copySQLBacktickQuoted(builder *strings.Builder, query string, start int) int {
	builder.WriteByte(query[start])
	for i := start + 1; i < len(query); i++ {
		builder.WriteByte(query[i])
		if query[i] != '`' {
			continue
		}
		if i+1 < len(query) && query[i+1] == '`' {
			builder.WriteByte(query[i+1])
			i++
			continue
		}
		return i + 1
	}
	return len(query)
}

// copySQLLineComment 复制 SQL 行注释，并跳过注释内可能出现的占位符文本。
func copySQLLineComment(builder *strings.Builder, query string, start int) int {
	for i := start; i < len(query); i++ {
		builder.WriteByte(query[i])
		if query[i] == '\n' {
			return i + 1
		}
	}
	return len(query)
}

// copySQLBlockComment 复制 SQL 块注释，并跳过注释内可能出现的占位符文本。
func copySQLBlockComment(builder *strings.Builder, query string, start int) int {
	for i := start; i < len(query); i++ {
		builder.WriteByte(query[i])
		if query[i] == '*' && i+1 < len(query) && query[i+1] == '/' {
			builder.WriteByte(query[i+1])
			return i + 2
		}
	}
	return len(query)
}

// postgresDollarQuoteTag 返回 PostgreSQL dollar-quoted 字符串的完整起始标签。
func postgresDollarQuoteTag(query string, start int) (string, bool) {
	if query[start] != '$' || start+1 >= len(query) {
		return "", false
	}
	if query[start+1] == '$' {
		return "$$", true
	}
	if !isSQLIdentifierStart(query[start+1]) {
		return "", false
	}
	for i := start + 2; i < len(query); i++ {
		if query[i] == '$' {
			return query[start : i+1], true
		}
		if !isSQLIdentifierPart(query[i]) {
			return "", false
		}
	}
	return "", false
}

// copyPostgresDollarQuoted 复制 PostgreSQL dollar-quoted 字符串。
func copyPostgresDollarQuoted(builder *strings.Builder, query string, start int, tag string) int {
	searchStart := start + len(tag)
	end := strings.Index(query[searchStart:], tag)
	if end < 0 {
		builder.WriteString(query[start:])
		return len(query)
	}
	next := searchStart + end + len(tag)
	builder.WriteString(query[start:next])
	return next
}

// formatSQLPlaceholder 用当前参数替换调试 SQL 中的单个占位符。
func formatSQLPlaceholder(placeholder string, args []any, index *int) string {
	if *index >= len(args) {
		if placeholder == paramSeat {
			return "?"
		}
		return placeholder
	}
	arg := args[*index]
	*index = *index + 1
	return formatSQLValue(arg)
}

// postgresParamSeatEnd 返回 PostgreSQL $n 占位符的结束位置。
func postgresParamSeatEnd(query string, start int) (int, bool) {
	if query[start] != '$' || start+1 >= len(query) || !isDigit(query[start+1]) {
		return start, false
	}
	end := start + 2
	for end < len(query) && isDigit(query[end]) {
		end++
	}
	return end, true
}

// isDigit 判断字节是否为 ASCII 数字。
func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

// isSQLIdentifierStart 判断字节是否可作为 PostgreSQL dollar quote 标签首字符。
func isSQLIdentifierStart(b byte) bool {
	return b == '_' || (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

// isSQLIdentifierPart 判断字节是否可作为 PostgreSQL dollar quote 标签后续字符。
func isSQLIdentifierPart(b byte) bool {
	return isSQLIdentifierStart(b) || isDigit(b)
}

// formatSQLValue 将参数值格式化为 SQL 调试字符串中的字面量。
func formatSQLValue(arg any) string {
	if arg == nil {
		return "null"
	}
	switch v := arg.(type) {
	case string:
		return quoteSQLValueString(v)
	case []byte:
		return quoteSQLValueString(string(v))
	default:
		return fmt.Sprint(v)
	}
}

// quoteSQLValueString 将字符串格式化为调试 SQL 字符串字面量，不修改原始内容。
func quoteSQLValueString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// formatSQLLog 生成单行 SQL 调试日志。
//
// query 会按 JSON 字符串规则转义，args 会逐个格式化并限制长度。
func formatSQLLog(name string, now time.Time, inTx bool, query string, args []any) string {
	return formatSQLLogWithTable(name, now, inTx, query, "", false, args)
}

// formatSQLTxBoundaryLog 生成事务边界的 SQL 调试日志，并附带当前 Builder 表名。
func formatSQLTxBoundaryLog(name string, now time.Time, query, table string) string {
	return formatSQLLogWithTable(name, now, true, query, table, true, nil)
}

// formatSQLLogWithTable 按统一格式拼接 SQL 调试日志；withTable 仅用于事务边界日志。
func formatSQLLogWithTable(name string, now time.Time, inTx bool, query, table string, withTable bool, args []any) string {
	log := "[sql][" + name + "][" + now.Format("2006-01-02 15:04:05.000") + "]" +
		"[tx=" + formatSQLLogTx(inTx) + "][query=" + quoteSQLLogString(query, sqlLogQueryMaxRunes) + "]"
	if withTable {
		log += "[table=" + quoteSQLLogString(table, sqlLogQueryMaxRunes) + "]"
	}
	return log + "[args=" + formatSQLLogArgs(args) + "]"
}

// formatSQLLogTx 将事务状态格式化为固定宽度的 0/1 标记，便于扫日志。
func formatSQLLogTx(inTx bool) string {
	if inTx {
		return "1"
	}
	return "0"
}

// logSQLTxBoundary 在 debug 开启时输出 BEGIN、COMMIT、ROLLBACK 边界日志。
func logSQLTxBoundary(name, table, query string) {
	alias, err := getDB(name)
	if err != nil {
		return
	}
	aliasName, _, dev := aliasSnapshot(alias)
	if dev {
		fmt.Println(formatSQLTxBoundaryLog(aliasName, time.Now(), query, table))
	}
}

// formatSQLLogArgs 将参数列表格式化为日志中的数组形式。
func formatSQLLogArgs(args []any) string {
	if len(args) == 0 {
		return "[]"
	}
	var builder strings.Builder
	builder.WriteByte('[')
	for i, arg := range args {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(formatSQLLogArg(arg))
	}
	builder.WriteByte(']')
	return builder.String()
}

// formatSQLLogArg 将单个参数格式化为日志可读形式。
func formatSQLLogArg(arg any) string {
	if arg == nil {
		return "null"
	}
	switch v := arg.(type) {
	case string:
		return quoteSQLLogString(v, sqlLogArgMaxRunes)
	case []byte:
		return quoteSQLLogString(string(v), sqlLogArgMaxRunes)
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprint(v)
	case time.Time:
		return quoteSQLLogString(v.Format(time.RFC3339Nano), sqlLogArgMaxRunes)
	default:
		return quoteSQLLogString(fmt.Sprint(v), sqlLogArgMaxRunes)
	}
}

// quoteSQLLogString 将字符串截断后渲染为单行日志字段值。
func quoteSQLLogString(s string, maxRunes int) string {
	s = truncateSQLLogString(s, maxRunes)
	const hex = "0123456789abcdef"
	var builder strings.Builder
	builder.Grow(len(s) + 2)
	builder.WriteByte('"')
	for _, r := range s {
		switch r {
		case '\\':
			builder.WriteString(`\\`)
		case '"':
			builder.WriteString(`\"`)
		case '\n':
			builder.WriteString(`\n`)
		case '\r':
			builder.WriteString(`\r`)
		case '\t':
			builder.WriteString(`\t`)
		case '\b':
			builder.WriteString(`\b`)
		case '\f':
			builder.WriteString(`\f`)
		case '\u2028':
			builder.WriteString(`\u2028`)
		case '\u2029':
			builder.WriteString(`\u2029`)
		default:
			if r < ' ' || r == '\u007f' || (r >= '\u0080' && r <= '\u009f') {
				builder.WriteString(`\u00`)
				builder.WriteByte(hex[int(r)>>4])
				builder.WriteByte(hex[int(r)&0x0f])
				continue
			}
			builder.WriteRune(r)
		}
	}
	builder.WriteByte('"')
	return builder.String()
}

// truncateSQLLogString 按 rune 数截断字符串。
//
// maxRunes 小于等于 0 时不截断。
func truncateSQLLogString(s string, maxRunes int) string {
	if maxRunes <= 0 {
		return s
	}
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "..."
}
