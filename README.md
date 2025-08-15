# RBDC-DaMeng

RBDC-DaMeng 是一个用于达梦数据库的RBDC（Rust Database Connectivity）驱动实现。

## 项目简介

本项目提供了达梦数据库的Rust语言驱动实现，让Rust开发者能够方便地连接和操作达梦数据库。该驱动基于ODBC接口实现，支持异步操作，并与rbatis ORM框架完全兼容。

## 功能特性

- ✅ 支持达梦数据库的基本连接和操作
- ✅ 提供符合RBDC规范的API接口
- ✅ 支持达梦数据库的主要数据类型（INT、VARCHAR、TEXT、DATETIME、BIGINT等）
- ✅ 提供异步操作支持
- ✅ 支持连接池管理
- ✅ 与rbatis ORM框架完全兼容
- ✅ 支持批量插入、查询、更新、删除操作
- ✅ 支持分页查询
- ✅ 支持事务处理
- ✅ 支持JSON字段的序列化和反序列化

## 依赖要求

- Rust 1.70+
- 达梦数据库 8.0+
- DM8 ODBC Driver

## 快速开始

### 安装

在你的 `Cargo.toml` 中添加依赖：

```toml
[dependencies]
rbdc-dameng = "0.1.0"
rbatis = "4.5"
rbdc = "4.5"
rbdc-pool-fast = "0.1"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

### 基本使用

```rust
use rbatis::RBatis;
use rbdc::pool::ConnectionManager;
use rbdc_dameng::driver::DamengDriver;
use rbdc_pool_fast::FastPool;
use serde::{Deserialize, Serialize};
use rbatis::crud;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BizActivity {
    pub id: i32,
    pub name: Option<String>,
    pub status: Option<i32>,
    pub create_time: Option<rbatis::rbdc::datetime::DateTime>,
}
crud!(BizActivity {});

#[tokio::main]
async fn main() -> Result<(), rbdc::Error> {
    // 连接字符串
    let connection_string = "dameng://SYSDBA:SYSDBA001@192.168.50.96:30236/test?CHARACTER_CODE=PG_UTF8";
    
    // 创建连接池
    let pool = FastPool::new(ConnectionManager::new(DamengDriver {}, connection_string)?)?
    pool.set_max_open_conns(10).await;
    pool.set_max_idle_conns(5).await;
    
    // 创建RBatis实例
    let rb = RBatis::new();
    rb.pool.set(Box::new(pool)).unwrap();
    
    // 插入数据
    let activity = BizActivity {
        id: 1,
        name: Some("测试活动".to_string()),
        status: Some(1),
        create_time: Some(rbatis::rbdc::datetime::DateTime::now()),
    };
    BizActivity::insert(&rb, &activity).await?;
    
    // 查询数据
    let result = BizActivity::select_by_column(&rb, "id", 1).await?;
    println!("查询结果: {:?}", result);
    
    Ok(())
}
```

### 连接字符串格式

支持两种连接字符串格式：

1. **Dameng URL格式（推荐）**：
   ```
   dameng://用户名:密码@主机:端口/数据库名?CHARACTER_CODE=PG_UTF8
   ```

2. **ODBC格式**：
   ```
   odbc://用户名:密码@主机:端口/数据库名?CHARACTER_CODE=PG_UTF8&odbc_driver=DM8 ODBC Driver
   ```

### 高级功能

#### 批量操作

```rust
// 批量插入
let activities = vec![activity1, activity2, activity3];
BizActivity::insert_batch(&rb, &activities, 100).await?;
```

#### 分页查询

```rust
use rbatis::PageRequest;

let page_req = PageRequest::new(1, 10); // 第1页，每页10条
let result = BizActivity::select_page(&rb, &page_req, "active").await?;
```

#### 自定义查询

```rust
use rbatis::{impl_select, impl_update, impl_delete};

impl_select!(BizActivity{select_by_name(name: &str) -> Option => "`where name = #{name} limit 1`"});
impl_update!(BizActivity{update_by_name(name: &str) => "`where name = #{name}`"});
impl_delete!(BizActivity{delete_by_name(name: &str) => "`where name = #{name}`"});
```

## 数据类型支持

| 达梦类型 | Rust类型 | 说明 |
|---------|----------|------|
| INT | i32 | 32位整数 |
| BIGINT | i64 | 64位整数 |
| VARCHAR | String | 变长字符串 |
| TEXT | String | 长文本 |
| DATETIME | DateTime | 日期时间 |
| DECIMAL | rust_decimal::Decimal | 高精度小数 |
| FLOAT | f32 | 单精度浮点数 |
| DOUBLE | f64 | 双精度浮点数 |

## 示例项目

项目包含一个完整的示例，展示了如何使用rbdc-dameng进行各种数据库操作：

```bash
cd example
cargo run
```

示例包括：
- 基本的CRUD操作
- 批量插入
- 分页查询
- JSON字段处理
- 自定义序列化/反序列化

## 配置选项

### 连接池配置

```rust
pool.set_max_open_conns(10).await;  // 最大连接数
pool.set_max_idle_conns(5).await;   // 最大空闲连接数
```

### 字符编码

推荐使用 `CHARACTER_CODE=PG_UTF8` 以确保中文字符正确处理。

## 故障排除

### 常见问题

1. **连接失败**：
   - 检查达梦数据库服务是否启动
   - 确认连接字符串中的主机、端口、用户名、密码是否正确
   - 确保安装了DM8 ODBC Driver

2. **中文乱码**：
   - 在连接字符串中添加 `CHARACTER_CODE=PG_UTF8`
   - 确保数据库字符集配置正确

3. **编译错误**：
   - 确保安装了达梦ODBC驱动
   - 检查系统环境变量配置

## 贡献

欢迎提交Issue和Pull Request来改进这个项目。

## 许可证

本项目采用MIT许可证。

## 相关链接

- [RBDC](https://github.com/rbatis/rbdc) - Rust数据库连接规范
- [RBatis](https://github.com/rbatis/rbatis) - Rust ORM框架
- [达梦数据库官网](https://www.dameng.com/)