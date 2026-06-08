package msql

import "strings"

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
// 需要在原始 SQL 条件中绑定参数时，请使用 WhereRaw。
// PostgreSQL 三段式条件由包内自动渲染占位符；单参数原始 SQL 条件不接收绑定参数。
func (m *Builder) Where(a ...string) *Builder {
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

// WhereRaw 添加原始 AND 查询条件，并把 args 作为绑定参数传递。
//
// query 会原样拼接到 where 子句中，调用方需保证 SQL 片段可信；args 只用于绑定 query 中的占位符。
// PostgreSQL raw 条件需要使用 $1、$2 等占位符，并会按最终 SQL 出现顺序重新编号；? 会按 SQL 原文保留。
// $n 仅表示一个待绑定参数位置，不表示参数复用；每出现一个 $n 就必须按出现顺序传入一个有实际意义的参数。
//
// 示例：
//
//	msql.Model("users").WhereRaw("password=MD5(concat(?,salt))", "pwd")
//	msql.Model("users", "pg").WhereRaw("password=MD5(concat($1,salt))", "pwd")
func (m *Builder) WhereRaw(query string, args ...any) *Builder {
	if query == "" {
		return m
	}
	if m.where == nil {
		m.where = []string{}
	}
	m.where = append(m.where, query)
	m.whereArgs = append(m.whereArgs, args...)
	return m
}

// WhereOr 添加 OR 查询条件。
//
// 参数规则与 Where 一致；多个 WhereOr 之间使用 OR 连接。
// 需要在原始 SQL 条件中绑定参数时，请使用 WhereOrRaw。
// PostgreSQL 三段式条件由包内自动渲染占位符；单参数原始 SQL 条件不接收绑定参数。
func (m *Builder) WhereOr(a ...string) *Builder {
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

// WhereOrRaw 添加原始 OR 查询条件，并把 args 作为绑定参数传递。
//
// query 会原样拼接到 where 子句中，调用方需保证 SQL 片段可信；args 只用于绑定 query 中的占位符。
// PostgreSQL raw 条件需要使用 $1、$2 等占位符，并会按最终 SQL 出现顺序重新编号；? 会按 SQL 原文保留。
// $n 仅表示一个待绑定参数位置，不表示参数复用；每出现一个 $n 就必须按出现顺序传入一个有实际意义的参数。
func (m *Builder) WhereOrRaw(query string, args ...any) *Builder {
	if query == "" {
		return m
	}
	if m.whereor == nil {
		m.whereor = []string{}
	}
	m.whereor = append(m.whereor, query)
	m.whereorArgs = append(m.whereorArgs, args...)
	return m
}

// WhereIn 添加字段 IN 条件，values 会按绑定参数传递。
//
// 适合需要传入非字符串值，或值中可能包含逗号的场景；空 values 不会追加条件。
// 排除集合时请使用 WhereNotIn。
//
// 示例：
//
//	msql.Model("users").WhereIn("id", 1, 2, 3)
//	msql.Model("users").WhereIn("email", "a,b@example.com", "c@example.com")
func (m *Builder) WhereIn(field string, values ...any) *Builder {
	return m.whereIn(field, "in", values)
}

// WhereNotIn 添加字段 NOT IN 条件，values 会按绑定参数传递。
//
// 空 values 不会追加条件。
//
// 示例：
//
//	msql.Model("users").WhereNotIn("status", "deleted", "disabled")
func (m *Builder) WhereNotIn(field string, values ...any) *Builder {
	return m.whereIn(field, "not in", values)
}

// WhereLike 添加字段 LIKE 条件，value 会自动包裹为 %value% 并按绑定参数传递。
//
// 如果需要自行控制通配符位置，请使用 WhereRaw。
//
// 示例：
//
//	msql.Model("users").WhereLike("name", "tom")
func (m *Builder) WhereLike(field, value string) *Builder {
	return m.whereLike(field, "like", value)
}

// WhereNotLike 添加字段 NOT LIKE 条件，value 会自动包裹为 %value% 并按绑定参数传递。
//
// 如果需要自行控制通配符位置，请使用 WhereRaw。
//
// 示例：
//
//	msql.Model("users").WhereNotLike("name", "test")
func (m *Builder) WhereNotLike(field, value string) *Builder {
	return m.whereLike(field, "not like", value)
}

// WhereBetween 添加字段 BETWEEN 条件，start 和 end 会按绑定参数传递。
//
// 排除区间时请使用 WhereNotBetween。
//
// 示例：
//
//	msql.Model("users").WhereBetween("created_at", "2026-06-01", "2026-06-30")
//	msql.Model("orders").WhereBetween("amount", 100, 500)
func (m *Builder) WhereBetween(field string, start, end any) *Builder {
	return m.whereBetween(field, "between", start, end)
}

// WhereNotBetween 添加字段 NOT BETWEEN 条件，start 和 end 会按绑定参数传递。
//
// 示例：
//
//	msql.Model("orders").WhereNotBetween("amount", 100, 500)
func (m *Builder) WhereNotBetween(field string, start, end any) *Builder {
	return m.whereBetween(field, "not between", start, end)
}

// WhereFindInSet 添加 find_in_set 条件，value 会按绑定参数传递。
//
// field 会原样拼接到 find_in_set 的第二个参数位置，调用方需保证字段表达式可信。
//
// 该方法主要面向 MySQL/MariaDB 的 find_in_set 函数。
//
// 示例：
//
//	msql.Model("users").WhereFindInSet("tags", "vip")
func (m *Builder) WhereFindInSet(field string, value any) *Builder {
	if field == "" {
		return m
	}
	if m.where == nil {
		m.where = []string{}
	}
	m.where = append(m.where, "find_in_set("+paramSeat+","+field+")")
	m.whereArgs = append(m.whereArgs, value)
	return m
}

// Where2 批量添加 AND 查询条件。
//
// 每个子切片的含义与 Where 的参数一致。
func (m *Builder) Where2(l [][]string) *Builder {
	if l == nil || len(l) == 0 {
		return m
	}
	for _, a := range l {
		m.Where(a...)
	}
	return m
}

// WhereOr2 批量添加 OR 查询条件。
//
// 每个子切片的含义与 WhereOr 的参数一致。
func (m *Builder) WhereOr2(l [][]string) *Builder {
	if l == nil || len(l) == 0 {
		return m
	}
	for _, a := range l {
		m.WhereOr(a...)
	}
	return m
}

// toWhere 将 Where/WhereOr 的字符串参数转换为 SQL 条件和绑定参数。
//
// 这是旧字符串式 Where API 的兼容转换逻辑；新的类型化条件优先使用 WhereIn、WhereLike 等方法。
// 三段式条件会转换为占位符表达式，单参数条件会按原始 SQL 片段处理。
func toWhere(a []string) (string, []any) {
	if a == nil || len(a) < 1 || a[0] == "" {
		return "", nil
	}
	if len(a) == 1 {
		return a[0], nil
	}
	field := a[0]
	operator := ""
	value := ""
	var args []any
	switch len(a) {
	case 2:
		if a[0] == "" {
			return a[1], nil
		}
		operator = "="
		value = paramSeat
		args = append(args, toWhereValue(a[1]))
	case 3:
		switch {
		case strings.EqualFold(a[1], "in") ||
			strings.EqualFold(a[1], "not in"):
			values := strings.Split(a[2], ",")
			if len(values) == 0 {
				return "", nil
			}
			seats := make([]string, len(values))
			for i, v := range values {
				seats[i] = paramSeat
				args = append(args, toWhereValue(strings.TrimSpace(v)))
			}
			operator = " " + a[1]
			value = "(" + strings.Join(seats, ",") + ")"
		case strings.EqualFold(a[1], "like") ||
			strings.EqualFold(a[1], "not like"):
			operator = " " + a[1] + " "
			value = paramSeat
			args = append(args, "%"+a[2]+"%")
		case strings.EqualFold(a[1], "between") ||
			strings.EqualFold(a[1], "not between"):
			bs := strings.Split(a[2], ",")
			if len(bs) != 2 {
				return "", nil
			}
			operator = " " + a[1] + " "
			value = paramSeat + " and " + paramSeat
			args = append(args, toWhereValue(strings.TrimSpace(bs[0])), toWhereValue(strings.TrimSpace(bs[1])))
		case InArray(a[1], []string{">", ">=", "<", "<=", "!=", "<>", "="}):
			operator = a[1]
			value = paramSeat
			args = append(args, toWhereValue(a[2]))
		case strings.EqualFold(a[0], "find_in_set"):
			if a[1] == "" || a[2] == "" {
				return "", nil
			}
			return "find_in_set(" + paramSeat + "," + a[2] + ")", []any{toWhereValue(a[1])}
		default:
			return "", nil
		}
	default:
		return "", nil
	}
	fs := strings.Split(field, "|")
	if len(fs) > 1 {
		allArgs := make([]any, 0, len(fs)*len(args))
		for i, f := range fs {
			fs[i] = f + operator + value
			allArgs = append(allArgs, args...)
		}
		return "(" + strings.Join(fs, " or ") + ")", allArgs
	}
	return field + operator + value, args
}

// toWhereValue 保留 where 条件绑定值原文。
func toWhereValue(s string) string {
	return s
}

// getWhere 生成 where 子句。
//
// where 条件使用 and 连接，whereor 条件使用 or 连接；两类条件混合时保持历史拼接方式，不额外添加括号。
func (m *Builder) getWhere() string {
	wh := strings.Join(m.where, " and ")
	or := strings.Join(m.whereor, " or ")
	if wh == "" {
		if or == "" {
			return ""
		}
		return "where " + or
	}
	if or == "" {
		return "where " + wh
	}
	return "where " + wh + " or " + or
}

// getWhereArgs 返回 where 和 whereor 条件的绑定参数。
func (m *Builder) getWhereArgs() []any {
	args := make([]any, 0, len(m.whereArgs)+len(m.whereorArgs))
	args = append(args, m.whereArgs...)
	args = append(args, m.whereorArgs...)
	return args
}

// whereIn 添加 IN 或 NOT IN 条件，并复用同一套占位符和参数追加逻辑。
func (m *Builder) whereIn(field, operator string, values []any) *Builder {
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
	m.where = append(m.where, field+" "+operator+"("+strings.Join(seats, ",")+")")
	m.whereArgs = append(m.whereArgs, values...)
	return m
}

// whereLike 添加 LIKE 或 NOT LIKE 条件，并保持旧 Where(..., "like", ...) 的自动包裹语义。
func (m *Builder) whereLike(field, operator, value string) *Builder {
	if field == "" {
		return m
	}
	if m.where == nil {
		m.where = []string{}
	}
	m.where = append(m.where, field+" "+operator+" "+paramSeat)
	m.whereArgs = append(m.whereArgs, "%"+value+"%")
	return m
}

// whereBetween 添加 BETWEEN 或 NOT BETWEEN 条件。
func (m *Builder) whereBetween(field, operator string, start, end any) *Builder {
	if field == "" {
		return m
	}
	if m.where == nil {
		m.where = []string{}
	}
	m.where = append(m.where, field+" "+operator+" "+paramSeat+" and "+paramSeat)
	m.whereArgs = append(m.whereArgs, start, end)
	return m
}
