// Package msql 提供轻量级 SQL 构造、执行和数据库连接注册能力。
//
// 包内的链式查询会尽量使用参数绑定处理 Where 条件，降低 SQL 注入风险。
//
// WhereIn、WhereNotIn、WhereBetween、WhereNotBetween、WhereLike、WhereNotLike 和 WhereFindInSet
// 适合常见条件的类型化参数绑定；复杂表达式可使用 WhereRaw 或 WhereOrRaw。
//
// 原始 SQL 入口包括 Field、Join、WhereRaw、WhereOrRaw、Having、Order、Update2、RawValues 和 RawExec。
//
// 这些入口仍会保留调用方传入的 SQL 片段；PostgreSQL 下绑定 raw 片段参数时应使用 $1、$2 等占位符，? 会按 SQL 原文保留。
// raw 片段中的 $n 仅表示一个待绑定参数位置，不表示参数复用；每出现一个 $n 就必须按出现顺序传入一个有实际意义的参数。
// 构造可执行 SQL 时包内会按最终 SQL 出现顺序重新编号这些 PostgreSQL 占位符。
// 使用这些入口时应只拼接可信字段名、表名或已审计的 SQL 表达式。
//
// Builder 表示一次表级链式 SQL 构造和执行上下文，不是数据库连接或连接池。通常应通过 Model 创建：
//
//	list, err := msql.Model("users").Where("status", "=", "enabled").Select()
//
// Builder 会在链式调用过程中保存并修改查询条件、分页、事务和最近执行结果等状态；执行方法通常会清空
// 查询条件等临时状态。因此 Builder 不应被多个 goroutine 并发复用，也不应在使用后复制。
// 需要并发构造或执行 SQL 时，应为每条调用链单独创建 Builder。
//
// Model 或 Table 传入空表名时不会立即返回错误；后续需要表名的查询、写入和表结构检查方法会返回空表名错误。
// BuildSqlPro 和 BuildSql 无法返回 error，空表名时会返回空 SQL。
package msql
