# 数据库支持矩阵

## 图例

- ✅：内置实现，并有本地或真实数据库契约测试。
- ⚙️：已实现，但依赖服务端配置、扩展或权限。
- ◐：复用兼容协议，尚未在独立真实环境完成契约验证。
- —：当前驱动不提供该能力。

CRUD 指通过查询编辑器执行数据库原生的创建、读取、更新和删除操作。“表数据”表示对象树
驱动的服务端分页浏览，以及带稳定行标识、乐观并发检查和事务提交的可视化编辑。

## 内置驱动

| 数据库 | 查询方式 | CRUD | 对象树 | 表数据 | Estimated | Analyze | 慢查询 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| PostgreSQL | SQL | ✅ | ✅ | ✅ | ✅ | ✅ | ⚙️ `pg_stat_statements` |
| MySQL 8 | SQL | ✅ | ✅ | ✅ | ✅ | ✅ | ⚙️ `performance_schema` |
| MariaDB | MySQL 协议 / SQL | ◐ | ◐ | ◐ | ◐ | ◐ | ◐ |
| SQLite | SQL | ✅ | ✅ | ✅ | ✅ | — | — |
| MongoDB | JSON 操作信封 | ✅ | ✅ | — | ✅ | ✅ | ⚙️ 数据库 profiler |
| Redis 7 | Redis 命令 | ✅ | ✅ `SCAN` | — | — | — | ⚙️ `SLOWLOG` |
| Valkey | Redis / RESP 命令 | ◐ | ◐ `SCAN` | — | — | — | ◐ `SLOWLOG` |

为控制桌面端依赖和本地 `target/` 体积，内置范围限定为 PostgreSQL、MySQL / MariaDB、
SQLite、MongoDB、Redis / Valkey 五类主流数据库；DuckDB、ClickHouse 不再随应用编译。

### 已完成的真实契约验证

- PostgreSQL 16
- MySQL 8.4
- MongoDB 7
- Redis 7
- SQLite（bundled）

MariaDB 与 MySQL、Valkey 与 Redis 分别共享协议入口，但在声明为完全验证前仍需要独立版本矩阵。

## 能力说明

### PostgreSQL

- 使用 SQLx PostgreSQL 驱动。
- 对象树覆盖 schema、表、视图、列、索引等对象。
- 表数据读取使用服务端精确计数和分页；主键优先，否则选择全部列均非空的唯一索引。
- 非二进制表数据由 PostgreSQL 转为规范文本显示，写回时通过目标列类型显式转换；`bytea`
  保持二进制值。
- 执行计划使用 JSON 格式的 `EXPLAIN` / `EXPLAIN ANALYZE`。
- 慢查询读取 `pg_stat_statements`；服务端必须预加载并创建该扩展，当前账号还需要查询权限。

### MySQL / MariaDB

- 使用 SQLx MySQL 驱动和 MySQL URL。
- MySQL 8.4 已验证对象树、JSON 执行计划、Analyze 和
  `performance_schema.events_statements_summary_by_digest`。
- 表数据编辑排除前缀唯一索引和函数索引，避免把不能唯一定位完整行的索引当作稳定键。
- MariaDB 的协议兼容性较高，但执行计划 JSON、Analyze 和性能表字段可能因版本而不同，
  因此当前标记为兼容预览。

### SQLite

- 使用 bundled SQLite，不依赖系统 SQLite。
- 支持内存数据库与文件数据库、对象树和 `EXPLAIN QUERY PLAN`。
- 表数据保留 SQLite 动态类型中的 `NULL`、文本和 BLOB；部分唯一索引与表达式唯一索引不作为
  稳定行标识。
- SQLite 没有跨会话的原生慢查询统计源；Analyze 模式没有被虚假映射为 Estimated。

## 表数据编辑边界

- 表和视图都可分页浏览；视图始终只读。
- 普通表只有在存在非空主键或所有组成列均非空的唯一键时才可编辑。
- 更新条件包含稳定键和被修改列的原始值；删除条件包含稳定键和结果中所有直接来源列的原始值。
- 一批删除、更新和新增在同一事务执行。任一更新或删除未恰好影响一行时，整批回滚并返回冲突。
- SQL 预览中的占位符、语句顺序和有序参数就是实际提交内容；数据值不会拼接进 SQL。
- 简单单表查询在返回完整稳定键时可编辑，别名列可映射回源列，计算列保持只读。投影查询不允许
  新增行；`SELECT *` 才允许新增。
- JOIN、多数据源、CTE、DISTINCT、聚合、分组、集合操作、派生表、视图和缺少稳定键的结果只读，
  界面显示具体原因。
- 当前版本不提供外键联动编辑、批量粘贴、数据库专用类型控件或可更新视图推断。

### MongoDB

- 使用官方异步 Rust 驱动。
- 查询编辑器接受有类型的 JSON 操作信封，而不是解析 JavaScript shell 代码。
- `find` 和 `aggregate` 支持执行计划；Analyze 对应 `executionStats`。
- 慢查询读取目标数据库的 `system.profile`，需要预先启用 profiler 并授予读取权限。

### Redis / Valkey

- 使用 `redis-rs` 的异步复用连接，仅启用 Tokio 和 Rustls TLS 所需特性；不依赖 OpenSSL，
  也不启用连接管理器、集群、脚本和大整数等默认功能。
- 支持 `redis://`、`rediss://`、`valkey://`、`valkeys://`，查询编辑器接受 Redis 原生命令。
- 对象树使用可分页的 `SCAN`，并批量读取 `TYPE` / `PTTL`；为避免阻塞服务端，查询入口拒绝
  `KEYS`。
- 慢查询读取 `SLOWLOG GET`，账号需要相应 ACL 权限；Redis / Valkey 没有 Estimated 或
  Analyze 执行计划能力。
- Redis 7 已完成真实契约验证；Valkey 复用兼容协议，目前仍标记为兼容预览。

## 扩展路线

扩展优先级同时考虑协议复用程度、Rust 驱动成熟度、诊断能力差异和 CI 可验证性。

| 阶段 | 数据库 | 接入策略 | 关键验证 |
| --- | --- | --- | --- |
| A：兼容性认证 | MariaDB | 复用现有驱动 | 独立版本矩阵、计划/慢查询字段差异 |
| A：SQL 兼容族 | CockroachDB、YugabyteDB、TimescaleDB | 优先复用 PostgreSQL 驱动 | 方言、系统目录、计划和分布式诊断 |
| A：MySQL 兼容族 | TiDB、OceanBase | 优先复用 MySQL 驱动 | URL/TLS、系统表、分布式计划 |
| B：企业关系库 | SQL Server | 独立驱动插件（TDS） | Windows/SQL 登录、Showplan、Query Store |
| B：企业关系库 | Oracle | 独立进程插件 | Oracle Client 分发、许可、执行计划和 AWR 权限 |
| B：搜索分析 | Elasticsearch、OpenSearch | HTTP/JSON 插件 | DSL、mapping、profile API、慢日志 |
| B：宽列数据库 | Cassandra、ScyllaDB | CQL 插件 | 分页、token、trace、集群拓扑 |
| C：图数据库 | Neo4j、Memgraph | Bolt 插件 | Cypher、图结果渲染、PROFILE |
| C：云数仓 | Snowflake、BigQuery、Redshift | 独立插件 | OAuth、费用提示、异步作业、结果分页 |
| C：时序/云 KV | InfluxDB、DynamoDB | 独立插件 | 专用查询语言、分页、容量/费用诊断 |

只有“连接 + CRUD + 对象发现 + 取消/超时 + 能力准确声明 + 真实契约测试”全部满足后，
数据库才应从路线图移入内置支持矩阵。无法提供执行计划或慢查询的数据库必须声明为不支持，
而不是模拟一个含义不同的结果。
