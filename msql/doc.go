// Package msql 提供轻量级 SQL 构造、执行和数据库连接注册能力。
//
// 包内的链式查询会尽量使用参数绑定处理 Where 条件，降低 SQL 注入风险。
// Field、Join、Having、Order、Update2、RawValues 和 RawExec 等原始 SQL 入口仍会保留调用方传入的 SQL 片段，
// 这些入口在 PostgreSQL 下绑定 raw 片段参数时应使用 $1、$2 等占位符，? 会按 SQL 原文保留。
// 使用这些入口时应只拼接可信字段名、表名或已审计的 SQL 表达式。
package msql
