# ADR-0003：自研本地依赖与结果表示

## 状态

Accepted

## 背景

项目的目标是"一个可执行文件，装上就能用"。但实现里有一批依赖违背了这个目标，另有
一批依赖付出了成本却没兑现价值：

- **界面要求 Vulkan**。eframe 默认走 wgpu，文档要求 Linux 用户先装 `libvulkan1` 和
  `mesa-vulkan-drivers`。虚拟机、远程桌面和老显卡上装不上或跑不起来。
- **保存密码要求桌面服务**。`keyring` 在 Linux 上走 D-Bus Secret Service，SSH、容器和
  无桌面环境里直接失败。而且这个抽象建好后从未接进界面，等于只付出了依赖体积。
- **导出文件要求桌面门户**。`rfd` 在 Linux 上需要 `xdg-desktop-portal`；缺失时对话框
  静默返回空，界面显示"已取消导出"，用户无法区分是环境问题还是自己点了取消。
- **加载中文字体引入了 HTTP 客户端**。`egui-system-fonts` 传递依赖 `ehttp`，在一个
  本地数据库工具里出现出网能力。
- **Arrow 的收益从未兑现**。所有关系型列都是 `DataType::Utf8`，全仓库只用到
  `num_rows` / `slice` / `schema` / `get_array_memory_size` 四个 API，界面拿到后立刻
  拍平成 `Vec<Vec<String>>`。为一个纯容器付出了整套 arrow-* 的编译成本。
- **一个 crate 完全没被引用**。`dbc-plugin-protocol`（Protobuf + buf 工具链 + `proto/`）
  只有它自己的测试用到它。

同时明确一点：**`sqlx` 保留**。手写 MySQL / PostgreSQL 的 wire protocol 既不会更快，
也会在认证变体和类型解码的边缘情况上引入难以发现的错误，收益为负。

## 决策

### 1. 渲染改用 glow，去掉 GPU 驱动要求

`eframe` 关闭默认 features，只启用 `glow`、`default_fonts`、`accesskit`、`wayland`、
`x11`。wgpu 与 naga 整条链路移出依赖树。

代价是放弃现代 GPU 后端的渲染上限。对一个以虚拟表格和文本为主的界面，可运行范围比
渲染上限更重要。

### 2. 自研文件选择器、字体发现与凭据存储

三者都用 `std` 实现，替换掉需要外部服务的依赖：

- **文件选择器**（`file_picker.rs`）：`std::fs::read_dir` + egui 模态，支持面包屑、
  扩展名过滤、粘贴绝对路径和覆盖确认，并记住上次目录。失去了系统书签和最近位置。
- **字体发现**（`fonts.rs`）：扫描各平台字体目录，按已知的 CJK 字族名排序挑选。
  `FontData` 带 `index` 字段，因此 `.ttc` 字体集合可以直接加载。找不到时给出明确
  提示，而不是让界面显示成方块。
- **配置与凭据**（`store/`）：配置目录用 `std::env` 自行解析（`XDG_CONFIG_HOME` /
  `HOME` / `APPDATA`），连接配置存人可读的 `settings.json`，密码单独存
  `secrets.bin`。

### 3. 凭据加密用现成的密码学实现，容器格式自研

`secrets.bin` = 魔数 + 盐 + nonce + 密文。主密码经 Argon2id 派生 32 字节密钥，
内容用 ChaCha20-Poly1305 加密，Unix 下权限 `0600`，每次保存换新 nonce。

**不自己实现任何密码学原语**，只引入 `argon2` 和 `chacha20poly1305` 两个纯 Rust 实现。
这是净减少：换掉了 `keyring` 及其带来的 `zbus`、`secret-service`、`aes`、`cbc`、`hkdf`
和 Linux 上的 D-Bus 运行时要求。

主密码忘记后无法找回，只能删除凭据库重来——这是加密存储的固有代价，界面在设置时明说。

### 4. 用自研列式容器替换 Arrow

`dbc-core::result` 定义 `TextColumn`（连续字节 + 偏移表 + NULL 位图）和 `RowBatch`
（`Arc` 共享列 + 行窗口）。

- 分页切片是 O(1)：共享列缓冲、只移动行窗口，不复制单元格数据。
- `estimated_bytes()` 只统计窗口内的字节，比 Arrow 的 `get_array_memory_size()`
  （返回整个数组的内存，与切片无关）更准确，有界缓冲的二分预算因此才真正生效。
- 每列三次分配，取代每个单元格一次 `String` 分配。
- 驱动侧的类型 match 决策表原样保留，只换存储目标；`RowBatchBuilder` 同时消除了三个
  SQL 驱动里各自一份的 `TextBatchBuilder`。

代价：放弃了 Arrow 的类型化列存。当前没有任何列式计算需求，真要做时再引入。

### 5. 删除未接线的插件协议

`dbc-plugin-protocol`、`proto/` 和 buf 配置一并删除，理由见
[ADR-0001](0001-modular-driver-architecture.md) 决策 3。

### 6. crate 从 7 个收敛到 3 个

`dbc-data` 并入 `dbc-core`；`dbc-runtime` 与重写后的 `dbc-storage` 并入 `dbc-desktop`
（`tasks.rs` 与 `store/`）。剩下 `dbc-core` / `dbc-drivers` / `dbc-desktop`，边界对应
"契约 / 驱动 / 应用"三层，不再有只为分层而分层的 crate。

## 后果

### 正面

- Linux 上的 release 二进制只动态链接 `libc`、`libm`、`libgcc_s`；构建期系统依赖只剩
  C 编译工具链（供 bundled SQLite 使用）。
- 保存密码、选择文件、加载字体在 SSH、容器和无桌面环境里行为一致。
- 传递依赖 657 → 581。
- 结果分页不再复制单元格数据。

### 负面

- 文件选择器不如系统原生：没有书签、最近位置和网络位置。
- 找不到已知 CJK 字族时中文仍会显示成方块；内嵌全量 CJK 字体可彻底解决，但会让二进制
  增加约 10 MB，与"不体积庞大"冲突，故不采用。
- 忘记主密码等于丢失已保存的密码。
- 结果集失去类型化列存，未来若要做列式计算需要重新引入。

### 中性

- `sqlx`、`mongodb`、`redis` 三个客户端库保留，仍是依赖体积的主要来源。这是有意的：
  它们承担的是协议正确性，不是可以省掉的便利。

## 备选方案

### 自研 wire protocol 替换 sqlx

未采用。MySQL 的 `caching_sha2_password`、PostgreSQL 的 SCRAM-SHA-256、TLS 升级、
预处理语句的二进制协议和各版本的类型解码差异，都是高风险且难以自测的部分，而收益
（性能）并不存在——瓶颈在网络和数据库，不在客户端解析。

### 保留 keyring，仅在不可用时回退到加密文件

未采用。两套代码路径要各自测试，却只换来桌面环境下"更原生"的观感，同时保留了
`keyring` 的依赖体积。

### 内嵌 CJK 字体

未采用。见"负面"。

### 保留 Arrow 以备将来列式计算

未采用。这是为一个没有出现的需求持续付编译成本；真正需要时再引入的代价，低于一直
背着它的代价。

## 参考

- [ADR-0001：模块化数据库驱动架构](0001-modular-driver-architecture.md)
- [数据库支持矩阵](../database-support.md)
