use std::collections::HashMap;
use anyhow::anyhow;
use std::io::Write;
use std::num::{NonZero, NonZeroUsize};
use std::sync::{Arc, Mutex};

use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use log::info;
use odbc_api::{ColumnDescription, Cursor, Environment, IntoParameter, Nullability, Prepared, ResultSetMetadata};
use odbc_api::buffers::{BufferDesc, ColumnarAnyBuffer, RowVec, TextRowSet};
use odbc_api::Connection as OdbcApiConnection;
use rbdc::db::{Connection, ExecResult, Row};
use rbdc::Error;
use rbs::Value;

use crate::{DamengColumn, DamengData, DamengRow};
use crate::driver::DamengDriver;
use crate::encode::{Encode, sql_replacen};
// use crate::encode::Encode;
use crate::options::DamengConnectOptions;

// use crate::pool::manager::ODBCConnectionManager;
use odbc_api::ConnectionOptions;
use odbc_api::handles::{ParameterDescription, StatementImpl};
use odbc_api::parameter::VarCharArray;
use once_cell::sync::Lazy;
use rbdc::pool::conn_manager::ConnManager;
use crate::common::data_type::DmDataType;
// use crate::pool::shared::SharedPool;

// type OdbcConnection = OdbcApiConnection<'static>;

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
        // let sql: String = DamengDriver {}.pub_exchange(sql);
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

            let sql_before_encode = sql.clone();
            let encoded_params = ["10", "20"];

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
                // let headline: Vec<String> = match cursor.column_names() {
                //     Ok(names) => names.collect::<Result<_, _>>().unwrap(),
                //     Err(err) => { return Err(rbdc::Error::from("cursor.column_names() err")); }
                // };

                // let mut buffers = match TextRowSet::for_cursor(oc.batch_size, &mut cursor, oc.max_str_len) {
                //     Ok(buffers) => buffers,
                //     Err(err) => { return Err(rbdc::Error::from("TextRowSet::for_cursor() err")); }
                // };

                let mut max_str_lens: Vec<usize> = vec![];

                let mut column_description = Default::default();

                for index in 1..=cursor.num_result_cols().unwrap_or(0) {
                    cursor.describe_col(index as u16, &mut column_description)
                        .map_err(|err| anyhow!("describe_col err")).unwrap();

                    let nullable = matches!(
                        column_description.nullability,
                        Nullability::Unknown | Nullability::Nullable
                    );
                    let desc = BufferDesc::from_data_type(
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

                println!("columns: {:?}", columns);

                // Row set size of 5000 rows.
                // let mut buffer = ColumnarAnyBuffer::from_descs(oc.batch_size, buffer_description);

                // type Row = (VarCharArray<255>, VarCharArray<255>);
                // let mut buffer = RowVec::<Row>::new(oc.batch_size);

                // let mut buffer = match TextRowSet::for_cursor(oc.batch_size, &mut cursor, oc.max_str_len) {
                //     Ok(buffers) => buffers,
                //     Err(err) => { return Err(rbdc::Error::from("TextRowSet::for_cursor() err")); }
                // };

                let mut buffer = match TextRowSet::from_max_str_lens(columns.len(), max_str_lens) {
                    Ok(buffers) => buffers,
                    Err(err) => { return Err(rbdc::Error::from("TextRowSet::for_cursor() err")); }
                };

                let mut row_set_cursor = match cursor.bind_buffer(&mut buffer) {
                    Ok(block_cursor) => block_cursor,
                    Err(err) => { return Err(rbdc::Error::from("cursor.bind_buffer() err")); }
                };
                let mut num_batch = 0;

                // let mut results = vec![];

                while let Some(buffer) = row_set_cursor
                    .fetch_with_truncation_check(false)
                    .map_err(|error| provide_context_for_truncation_error(error, &mut columns)).unwrap()
                {
                    num_batch += 1;
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

    // fn get_rows(&mut self, sql: &str, params: Vec<Value>) -> BoxFuture<Result<Vec<Box<dyn Row>>, Error>> {
    //     let oc = self.clone();
    //
    //     // let sql: String = DamengDriver {}.pub_exchange(sql);
    //     let mut sql = sql.to_string();
    //
    //     let task = tokio::task::spawn_blocking(move || {
    //         if sql.eq("begin") || sql.eq("commit") || sql.eq("rollback") {
    //             log::warn!("不支持事务相关操作,直接返回");
    //             return Err(rbdc::Error::from("不支持事务相关操作"));
    //         }
    //
    //         // return Ok(Vec::new());
    //
    //         let mut results = Vec::new();
    //
    //         let mut encoded_params: Vec<_> = vec![];
    //         for x in &params {
    //             // encoded_params.push(x.encode(0)?) ;
    //             encoded_params.push(x.clone().encode(0)?);
    //         }
    //
    //         log::debug!("encoded_params: {:?}",encoded_params);
    //
    //         let sql_before_encode = sql.clone();
    //         let encoded_params = ["10", "20"];
    //
    //         // 执行查询
    //         // FIXME: 需要检查是否有 sql注入风险 ？
    //         sql = sql_replacen(sql, params);
    //         log::debug!("get_rows执行的sql:{}",sql);
    //         // println!("将要执行的sql:{}", sql);
    //
    //         // Convert the input strings into parameters suitable to for use with ODBC.
    //         // let params: Vec<_> = params
    //         //     .iter()
    //         //     .map(|param| param.as_str().into_parameter())
    //         //     .collect();
    //
    //         // Execute the query as a one off, and pass the parameters.
    //         let binding = oc.conn.clone();
    //         let binding = binding.lock().unwrap();
    //
    //
    //         if let Ok(Some(mut cursor)) = binding.execute(&sql , ()) {
    //             // Write column names.
    //             // cursor_to_csv(  cursor, &mut writer,   *batch_size,    *max_str_len, *ignore_truncation,  )?;
    //
    //             // results = match cursor_to_dm_row(cursor, oc.batch_size, oc.max_str_len, false) {
    //             //     Ok(rows) =>    rows,
    //             //     Err(err) => {  return Err(rbdc::Error::from("cursor_to_dm_row err")); }
    //             // }
    //
    //             //let headline: Vec<String> = cursor.column_names()?.collect::<Result<_, _>>()?;
    //
    //             let mut columns: Vec<DamengColumn> = vec![];
    //             let headline: Vec<String> = match cursor.column_names() {
    //                 Ok(names) => names.collect::<Result<_, _>>().unwrap(),
    //                 Err(err) => { return Err(rbdc::Error::from("cursor.column_names() err")); }
    //             };
    //
    //             for (index, name) in headline.iter().enumerate() {
    //                 let dt = cursor.col_data_type((index + 1) as u16).unwrap();
    //                 // println!("{}: {:?}", name, dt);
    //                 // let col_dt = cursor.col_data_type(col_index as u16).unwrap();
    //                 // let col = buffer.at(col_index, 0).unwrap_or(&[]);
    //                 columns.push(DamengColumn {
    //                     name: headline[index].to_string(),
    //                     column_type: dt,
    //                 });
    //             }
    //
    //             println!("columns: {:?}", columns);
    //
    //             let mut buffers = match TextRowSet::for_cursor(oc.batch_size, &mut cursor, oc.max_str_len) {
    //                 Ok(buffers) => buffers,
    //                 Err(err) => { return Err(rbdc::Error::from("TextRowSet::for_cursor() err")); }
    //             };
    //             let mut row_set_cursor = match cursor.bind_buffer(&mut buffers) {
    //                 Ok(row_set_cursor) => row_set_cursor,
    //                 Err(err) => { return Err(rbdc::Error::from("cursor.bind_buffer() err")); }
    //             };
    //             let mut num_batch = 0;
    //
    //             // let mut results = vec![];
    //
    //             while let Some(buffer) = row_set_cursor
    //                 .fetch_with_truncation_check(false)
    //                 .map_err(|error| provide_context_for_truncation_error(error, &headline)).unwrap()
    //             {
    //                 num_batch += 1;
    //                 //info!(  "Fetched batch {} with {} rows.", num_batch,  buffer.num_rows() );
    //
    //                 for row_index in 0..buffer.num_rows() {
    //                     // let record = (0..buffer.num_cols())
    //                     //     .map(|col_index| buffer.at(col_index, row_index).unwrap_or(&[]));
    //                     // writer.write_record(record)?;
    //
    //                     let mut datas = vec![];
    //
    //                     for col_index in 0..buffer.num_cols() {
    //                         // let col_dt = cursor.col_data_type(col_index as u16);
    //                         // let is_sql_null = col_dt.unwrap() == DmDataType::;
    //
    //                         let col = &columns[col_index];
    //
    //                         let colData = buffer.at(col_index, row_index).map(|col| col.to_vec());
    //                         datas.push(DamengData {
    //                             column_type: col.column_type,
    //                             data: colData,
    //                             is_sql_null: false,
    //                         });
    //                     }
    //
    //                     let taos_row = DamengRow {
    //                         columns: Arc::new(columns.clone()),
    //                         datas: datas,
    //                     };
    //                     results.push(Box::new(taos_row) as Box<dyn Row>);
    //                 }
    //             }
    //         }
    //         // None => {
    //         //     eprintln!("Query came back empty (not even a schema has been returned). No output has been created.");
    //         // }
    //         // };
    //
    //         return Ok(results);
    //     });
    //
    //     Box::pin(async move {
    //         task.await.map_err(|e| Error::from(e.to_string()))?
    //     })
    // }

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
                conn.set_autocommit(false);
                Ok(ExecResult {
                    rows_affected: 0,
                    last_insert_id: Value::Null,
                })
            } else if sql == "commit" {
                // manager.aquire().await.unwrap().commit().unwrap();
                conn.commit();
                conn.set_autocommit(true);
                *trans = false;
                Ok(ExecResult {
                    rows_affected: 0,
                    last_insert_id: Value::Null,
                })
            } else if sql == "rollback" {
                conn.rollback().unwrap();
                conn.set_autocommit(true);
                *trans = false;
                Ok(ExecResult {
                    rows_affected: 0,
                    last_insert_id: Value::Null,
                })
            } else {
                // let sql: String = DamengDriver {}.pub_exchange(&sql);
                // let builder = manager.aquire().await.unwrap().statement(&sql);
                // let mut stmt = builder.build().map_err(|e| Error::from(e.to_string()))?;
                // for (idx, x) in params.into_iter().enumerate() {
                //     x.encode(idx, &mut stmt).map_err(|e| Error::from(e.to_string()))?
                // }

                let mut sql = sql.to_string();
                // FIXME: 需要检查是否有 sql注入风险 ？
                sql = sql_replacen(sql, params);
                log::debug!("exec执行的sql:{}",sql);
                // println!("将要执行的sql:{}", sql);

                let mut prepared = conn.prepare(&sql)
                    .map_err(|e| Error::from(e.to_string()))?;
                prepared.execute(()).map_err(|e| Error::from(e.to_string()))?;
                let rows_affected = prepared.row_count().unwrap().unwrap_or(0);
                // let mut rows_affected = 0;
                // match prepared.execute(()) {
                //     Err(e) => {
                //         println!("{}", e);
                //         return Err(Error::from(e.to_string()));
                //     },
                //     // Most drivers would return a result set even if no Movie with the title is found,
                //     // the result set would just be empty. Well, most drivers.
                //     Ok(None) => println!("No result set generated."),
                //     Ok(Some(cursor)) => {
                //         // ...print cursor contents...
                //         // rows_affected = prepared.row_count().unwrap().unwrap_or(0);
                //         // return rows_affected;
                //         // 获取行数
                //         rows_affected = cursor.row_count().unwrap_or(0);
                //     }
                // }


                // // let rows_affected = prepared.row_count().unwrap().unwrap_or(0);
                // let rows_affected =  match prepared.row_count() {
                //     Ok(Some(v)) => v,
                //     _ => 0
                // };

                // stmt.execute(&[]) .map_err(|e| Error::from(e.to_string()))?;
                // if !*trans {
                //     manager.aquire().await.unwrap().commit().map_err(|e| Error::from(e.to_string()))?;
                //     *trans = false;
                // }
                // let rows_affected = stmt.row_count().map_err(|e| Error::from(e.to_string()))?;
                // let mut ret = vec![];
                // for i in 1..=stmt.bind_count() {
                //     let res: Result<String, _> = stmt.bind_value(i);
                //     match res {
                //         Ok(v) => {
                //             ret.push(Value::String(v))
                //         }
                //         Err(_) => {
                //             ret.push(Value::Null)
                //         }
                //     }
                // }
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
            // manager.aquire().await.unwrap().ping() .map_err(|e| Error::from(e.to_string()))?;

            // oc.exec("SELECT 1", vec![]);

            let binding = oc.conn.clone();
            let binding = binding.lock().unwrap();
            let x = match binding.execute("SELECT 1", ()) {
                Err(e) => {
                    // if let Some(odbc_api::Error::TooLargeValueForBuffer {
                    // rbdc::Error::from(e)
                    Err(rbdc::Error::from(""))
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
        let oc = self.clone();
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
