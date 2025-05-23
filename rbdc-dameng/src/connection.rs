use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use futures_core::future::BoxFuture;
use log::info;
use odbc_api::buffers::{BufferDesc, TextRowSet};
use odbc_api::Connection as OdbcApiConnection;
use odbc_api::ConnectionOptions;
use odbc_api::{Cursor, Environment, Nullability, ResultSetMetadata};
use once_cell::sync::Lazy;
use rbdc::db::{Connection, ExecResult, Row};
use rbdc::Error;
use rbs::Value;

use crate::encode::{sql_replacen, Encode};
use crate::options::DamengConnectOptions;
use crate::{DamengColumn, DamengData, DamengRow};

static ENV: Lazy<Environment> = Lazy::new(|| Environment::new().unwrap());

#[derive(Clone)]
pub struct DamengConnection {
    pub conn: Arc<Mutex<OdbcApiConnection<'static>>>,
    pub batch_size: usize, // 批量获取数据条数
    pub max_str_len: Option<usize>, // 最大字符串长度
    pub is_trans: Arc<Mutex<bool>>,
    pub sys_info: Option<String>,
}

unsafe impl Send for DamengConnection {}

unsafe impl Sync for DamengConnection {}

impl Connection for DamengConnection {
    fn get_rows(&mut self, sql: &str, params: Vec<Value>) -> BoxFuture<Result<Vec<Box<dyn Row>>, Error>> {
        let oc = self.clone();
        let mut sql = sql.to_string();

        let nz_max_str_len = NonZeroUsize::new(self.max_str_len.unwrap_or(0)).unwrap();

        let task = tokio::task::spawn_blocking(move || {
            if sql.eq("begin") || sql.eq("commit") || sql.eq("rollback") {
                log::warn!("不支持事务相关操作,直接返回");
                return Err(rbdc::Error::from("不支持事务相关操作"));
            }

            let mut results = Vec::new();

            let mut encoded_params: Vec<_> = vec![];
            for x in &params {
                // encoded_params.push(x.encode(0)?) ;
                encoded_params.push(x.clone().encode(0)?);
            }
            log::debug!("encoded_params: {:?}",encoded_params);

            // let sql_before_encode = sql.clone();
            // let encoded_params = ["10", "20"];

            // 执行查询
            // FIXME: 需要检查是否有 sql注入风险 ？
            sql = sql_replacen(sql, params);
            log::debug!("get_rows执行的sql:{}",sql);
            // println!("将要执行的sql:{}", sql);

            // Convert the input strings into parameters suitable to for use with ODBC.
            // let params: Vec<_> = params
            //     .iter()
            //     .map(|param| param.as_str().into_parameter())
            //     .collect();

            // Execute the query as a one off, and pass the parameters.
            let binding = oc.conn.clone();
            let binding = binding.lock().unwrap();

            if let Ok(Some(mut cursor)) = binding.execute(&sql, ()) {
                let mut columns: Vec<DamengColumn> = vec![];

                let mut max_str_lens: Vec<usize> = vec![];

                let mut column_description = Default::default();

                for index in 1..=cursor.num_result_cols().unwrap_or(0) {
                    cursor.describe_col(index as u16, &mut column_description)
                        .map_err(|_err| anyhow!("describe_col err")).unwrap();

                    let nullable = matches!(
                        column_description.nullability,
                        Nullability::Unknown | Nullability::Nullable
                    );
                    let _desc = BufferDesc::from_data_type(
                        column_description.data_type,
                        nullable,
                    ).unwrap_or(BufferDesc::Text { max_str_len: 255 });

                    let mut max_str_len_for_column = column_description.data_type.utf8_len().unwrap_or(nz_max_str_len);
                    if max_str_len_for_column > nz_max_str_len {
                        max_str_len_for_column = nz_max_str_len
                    }
                    max_str_lens.push(max_str_len_for_column.get());

                    columns.push(DamengColumn {
                        name: column_description.name_to_string().unwrap_or("".to_string()).to_lowercase(),
                        column_type: column_description.data_type,
                        nullability: nullable,
                    });
                }

                // println!("columns: {:?}", columns);

                let mut buffer = match TextRowSet::from_max_str_lens(columns.len(), max_str_lens) {
                    Ok(buffers) => buffers,
                    Err(_err) => { return Err(rbdc::Error::from("TextRowSet::for_cursor() err")); }
                };

                let mut row_set_cursor = match cursor.bind_buffer(&mut buffer) {
                    Ok(block_cursor) => block_cursor,
                    Err(_err) => { return Err(rbdc::Error::from("cursor.bind_buffer() err")); }
                };
                // let mut num_batch = 0;

                // let mut results = vec![];

                while let Some(buffer) = row_set_cursor
                    .fetch_with_truncation_check(false)
                    .map_err(|error| provide_context_for_truncation_error(error, &mut columns)).unwrap()
                {
                    // num_batch += 1;
                    //info!(  "Fetched batch {} with {} rows.", num_batch,  buffer.num_rows() );

                    for row_index in 0..buffer.num_rows() {
                        // let record = (0..buffer.num_cols())
                        //     .map(|col_index| buffer.at(col_index, row_index).unwrap_or(&[]));
                        // writer.write_record(record)?;

                        let mut datas = vec![];

                        for col_index in 0..buffer.num_cols() {
                            // let col_dt = cursor.col_data_type(col_index as u16);
                            // let is_sql_null = col_dt.unwrap() == DmDataType::;

                            let col = &columns[col_index];

                            let col_data = buffer.at(col_index, row_index).map(|col| col.to_vec());
                            datas.push(DamengData {
                                column_type: col.column_type,
                                data: col_data,
                                is_sql_null: false,
                            });
                        }

                        let taos_row = DamengRow {
                            columns: Arc::new(columns.clone()),
                            datas: datas,
                        };
                        results.push(Box::new(taos_row) as Box<dyn Row>);
                    }
                }
            }
            // None => {
            //     eprintln!("Query came back empty (not even a schema has been returned). No output has been created.");
            // }
            // };

            return Ok(results);
        });

        Box::pin(async move {
            task.await.map_err(|e| Error::from(e.to_string()))?
        })
    }

    fn exec(&mut self, sql: &str, params: Vec<Value>) -> BoxFuture<Result<ExecResult, Error>> {
        let oc = self.clone();
        let sql = sql.to_string();
        let task = tokio::task::spawn_blocking(move || {
            let mut trans = oc.is_trans.lock()
                .map_err(|e| Error::from(e.to_string()))?;

            let binding = oc.conn.clone();
            let conn = binding.lock().unwrap();

            if sql == "begin" {
                *trans = true;
                let _ = conn.set_autocommit(false);
                Ok(ExecResult {
                    rows_affected: 0,
                    last_insert_id: Value::Null,
                })
            } else if sql == "commit" {
                // manager.aquire().await.unwrap().commit().unwrap();
                let _ = conn.commit();
                let _ = conn.set_autocommit(true);
                *trans = false;
                Ok(ExecResult {
                    rows_affected: 0,
                    last_insert_id: Value::Null,
                })
            } else if sql == "rollback" {
                conn.rollback().unwrap();
                let _ = conn.set_autocommit(true);
                *trans = false;
                Ok(ExecResult {
                    rows_affected: 0,
                    last_insert_id: Value::Null,
                })
            } else {
                let mut sql = sql.to_string();
                // FIXME: 需要检查是否有 sql注入风险 ？
                sql = sql_replacen(sql, params);
                log::debug!("exec执行的sql:{}",sql);
                // println!("将要执行的sql:{}", sql);

                let mut prepared = conn.prepare(&sql)
                    .map_err(|e| Error::from(e.to_string()))?;
                prepared.execute(()).map_err(|e| Error::from(e.to_string()))?;
                let rows_affected = prepared.row_count().unwrap().unwrap_or(0);
                Ok(ExecResult {
                    rows_affected: rows_affected as u64,
                    last_insert_id: Value::Null,
                })
            }
        });
        Box::pin(async {
            task.await.map_err(|e| Error::from(e.to_string()))?
        })
    }

    fn ping(&mut self) -> BoxFuture<Result<(), rbdc::Error>> {
        let oc = self.clone();
        let task = tokio::task::spawn_blocking(move || {
            let binding = oc.conn.clone();
            let binding = binding.lock().unwrap();
            let x = match binding.execute("SELECT 1", ()) {
                Err(e) => {
                    // if let Some(odbc_api::Error::TooLargeValueForBuffer {
                    // rbdc::Error::from(e)
                    Err(rbdc::Error::from(e.to_string()))
                }
                Ok(_) => {
                    Ok(())
                }
            };
            x
        });
        Box::pin(async {
            task.await.map_err(|e| Error::from(e.to_string()))?
        })
    }

    fn close(&mut self) -> BoxFuture<Result<(), rbdc::Error>> {
        let _oc = self.clone();

        let task = tokio::task::spawn_blocking(move || {

            // manager.aquire().await.unwrap().commit().map_err(|e| Error::from(e.to_string()))?;
            // manager.aquire().await.unwrap().close().map_err(|e| Error::from(e.to_string()))?;

            Ok(())
        });
        Box::pin(async {
            task.await.map_err(|e| Error::from(e.to_string()))?
        })
    }
}

impl Drop for DamengConnection {
    fn drop(&mut self) {

        // println!("drop");

        if *self.is_trans.lock().unwrap() {
            let binding = self.conn.clone();
            let binding = binding.lock().unwrap();
            binding.rollback().unwrap();
        }
    }
}

fn provide_context_for_truncation_error(error: odbc_api::Error, headline: &mut Vec<DamengColumn>) -> anyhow::Error {
    match error {
        odbc_api::Error::TooLargeValueForBuffer {
            indicator: Some(required),
            buffer_index,
        } => {
            let col_name = &headline[buffer_index].name;
            anyhow!(
                "Truncation of text or binary data in column '{col_name}' detected. Try using \
                `--max-str-len` larger than {required}. Or do not specify it at all in order to \
                allow for larger values. You can also use the `--ignore-truncation` flag in order \
                to consider truncations warnings only. This will cause the truncated value to be \
                written into the csv, and execution to be continued normally."
            )
        }
        odbc_api::Error::TooLargeValueForBuffer {
            indicator: None,
            buffer_index,
        } => {
            // let col_name = &headline[buffer_index];
            let col_name = &headline[buffer_index].name;
            anyhow!(
                "Truncation of text or binary data in column '{col_name}' detected. Try using \
                larger values of `--max-str-len` (or do not specify it at all) in order to allow \
                for larger values. You can also use the `--ignore-truncation` flag in order to \
                consider truncations warnings only. This will cause the truncated value to be \
                written into the csv, and execution to be continued normally. The ODBC driver has \
                been unable to tell how large the value that caused the truncation is."
            )
        }
        other => other.into(),
    }
}

impl DamengConnection {
    pub async fn establish(opt: &DamengConnectOptions) -> Result<Self, Error> {

        // let conn = OraConnect::connect(opt.username.clone(), opt.password.clone(), opt.connect_string.clone())
        //     .map_err(|e| Error::from(e.to_string()))?;

        let env = &ENV;
        let conn = env.connect_with_connection_string(&opt.connection_string, ConnectionOptions::default());
        if conn.is_err() {
            return Err(Error::from(conn.unwrap_err().to_string()));
        }
        let conn = conn.unwrap();

        let parsed = Self::parse_connection_string(opt.connection_string.as_str());
        // 获取特定的 Schema 值
        let mut schema = parsed.get("schema");
        if schema.is_none() {
            schema = parsed.get("database");
        }

        let sys_info = conn.database_management_system_name().unwrap_or_default();
        info!("sysInfo: {}", sys_info);

        if let Some(database) = schema {
            if sys_info == "DM DATABASE MANAGEMENT SYSTEM" || sys_info == "达梦数据库管理系统"
                || sys_info.contains("DM8") || sys_info.contains("DM") {
                let query = format!("set schema {}", database);

                match conn.execute(query.as_str(), ()) { // 执行 USE azcms; 语句
                    Ok(_) => log::debug!("set schema {} 成功", database),
                    Err(e) => log::debug!("set schema {} 失败: {}", database, e),
                }
            } else {
                let query = format!("USE {}", database);

                match conn.execute(query.as_str(), ()) { // 执行 USE azcms; 语句
                    Ok(_) => log::debug!("USE {} 成功", database),
                    Err(e) => log::debug!("USE {} 失败: {}", database, e),
                }
            }
        }

        Ok(Self {
            // conn_manager: ODBCConnectionManager::new(&opt.connection_string, 5),
            conn: Arc::new(Mutex::new(conn)),
            is_trans: Arc::new(Mutex::new(false)),
            batch_size: opt.batch_size,
            max_str_len: opt.max_str_len,
            sys_info: Some(sys_info),
        })
    }

    fn parse_connection_string(conn_str: &str) -> HashMap<String, String> {
        let mut kv_pairs = HashMap::new();

        // 将连接字符串分割为键值对
        let pairs: Vec<&str> = conn_str.split(';').collect();

        for pair in pairs {
            // 将每个键值对按 '=' 分割
            let parts: Vec<&str> = pair.split('=').collect();
            if parts.len() == 2 {
                let key = parts[0].trim().to_string().to_lowercase();
                let value = parts[1].trim().to_string();
                kv_pairs.insert(key, value);
            }
        }

        kv_pairs // 返回键值对集合
    }
}
