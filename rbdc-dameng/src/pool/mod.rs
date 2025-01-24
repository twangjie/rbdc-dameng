
pub mod connection;
pub mod shared;
pub mod manager;
pub mod errors;


// 测试函数
#[cfg(test)]
mod tests {
    use super::*;
    use futures_core::future::BoxFuture;
    use tokio::task;

    // 一个同步函数
    fn synchronous_function(input: i32) -> i32 {
        // 模拟一些计算
        input * 2
    }

    // 返回一个 BoxFuture 的函数
    fn async_wrapper(input: i32) -> BoxFuture<'static, i32> {
        Box::pin(async move {
            task::spawn_blocking(move || synchronous_function(input)).await.unwrap()
        })
    }

    #[tokio::test]
    async fn test_async_wrapper() {
        let input = 10;
        let expected_output = 20; // 10 * 2 = 20

        let result = async_wrapper(input).await; // 调用 async_wrapper
        assert_eq!(result, expected_output); // 验证结果
    }

    fn cursor_to_csv(mut cursor: impl Cursor, batch_size: usize, max_str_len: Option<usize>, ignore_truncation: bool,
    ) -> Result<(), Error> {
        // 获取列名
        let headline: Vec<String> = cursor.column_names()?.collect::<Result<_, _>>()?;
        // 写入列名
        // writer.write_record(&headline)?;
        println!("headline: {:?}", headline);

        // 为游标创建文本行集
        let mut buffers = TextRowSet::for_cursor(batch_size, &mut cursor, max_str_len)?;
        // 绑定缓冲区
        let mut row_set_cursor = cursor.bind_buffer(&mut buffers)?;
        // 批次计数器
        let mut num_batch = 0;
        // 循环获取批次
        while let Some(buffer) = row_set_cursor
            .fetch_with_truncation_check(!ignore_truncation)
            .map_err(|error| provide_context_for_truncation_error(error, &headline))?
        {
            // 增加批次计数器
            // 打印批次信息
            num_batch += 1;
            info!(
            "Fetched batch {} with {} rows.",
            num_batch,
            buffer.num_rows()
        );
            // 循环写入行
            for row_index in 0..buffer.num_rows() {
                // 获取行数据
                let record = (0..buffer.num_cols())
                    .map(|col_index| buffer.at(col_index, row_index).unwrap_or(&[]));
                // 写入行数据
                // writer.write_record(record)?;
                println!("record: {:?}", record);
            }
        }
        // 返回结果
        Ok(())
    }

    #[tokio::test]
    async fn test_pool() {
        use crate::pool::manager::ODBCConnectionManager;
        use odbc_api::Cursor;

        // let connection_string = "Driver={PostgreSQL Unicode};Server=192.168.50.96;UID=root;PWD=Azsy12345.;database=azcms";
        // let connection_string = "Driver={ODBC Driver 18 for SQL Server};Server=192.168.50.96;UID=SA;PWD=Azsy12345.;TrustServerCertificate=yes;CharacterSet=UTF-8;";
        // let connection_string = "Driver={MySQL ODBC 9.1 Unicode Driver};Server=192.168.50.203;port=3306;UID=root;PASSWORD=Azsy12345.;database=AZCMS";
        // let connection_string = "Driver={Oracle in instantclient_23_6};Dbq=//192.168.50.96:1521/xe;UID=system;PWD=system;";
        let connection_string = "Driver={DM8 ODBC Driver};Server=192.168.50.96:30236;UID=SYSDBA;PWD=SYSDBA001;CHARACTER_CODE=PG_UTF8;SCHEMA=AZCMS";

        let mut manager = ODBCConnectionManager::new(connection_string, 5);
        // Execute the drop table command

        let sql = "select * from SUBEQUIPMENT WHERE SUBEQUIPMENTID between ? and ?";
        let params = [10, 20];

        let conn = manager.aquire().await.unwrap();

        let database = Some("azcms");

        // 执行 USE  语句
        if let Some(database) = database.as_deref() {
            let mut query = format!("USE {}", database);
            match conn.execute(query.as_str(), ()) { // 执行 USE azcms; 语句
                Ok(_) => println!("USE {} 成功", database),
                Err(_) => {
                    // eprintln!("USE {} 失败: {}", database, e)
                    query = format!("set schema {}", database);
                    match conn.execute(query.as_str(), ()) { // 执行 USE azcms; 语句
                        Ok(_) => println!("set schema {} 成功", database),
                        Err(e) => eprintln!("set schema {} 失败: {}", database, e),
                    }
                }
            }
        }

        if let Ok(Some(mut cursor)) = conn.execute(sql, &params[..]) {
            while let Ok(Some(mut row)) = cursor.next_row() {
                // 假设 column1 是整数，column2 是字符串
                let mut buffer1 = Vec::new();
                let mut buffer2 = Vec::new();
                let column1 = row.get_text(1, &mut buffer1); // 获取第一个列
                let column2 = row.get_text(4, &mut buffer2); // 获取第二个列

                let column1 = String::from_utf8_lossy(&buffer1);
                let column2 = String::from_utf8_lossy(&buffer2);

                // 打印结果
                println!("column1: {}, column2: {}", column1, column2);
            }

            // cursor_to_csv(&mut cursor, 10, 51200, false).await.unwrap();
        };
    }
}
