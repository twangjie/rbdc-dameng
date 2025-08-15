#[cfg(test)]
mod test {

    use log::info;
    use odbc_api::{ColumnDescription, ConnectionOptions, Cursor, Environment, IntoParameter, ResultSetMetadata};
    use serde::{Deserialize, Serialize};
    use std::fs;
    use std::ops::Deref;
    use odbc_api::parameter::{InputParameter, VarCharSlice};
    use rbdc::db::Driver;
    use rbs::Error;
    use tokio::runtime::Runtime;
    use rbdc_dameng::connection::DamengConnection;
    use rbdc_dameng::DamengDriver;
    use rbdc_dameng::options::DamengConnectOptions;

    #[test]
    fn test_mysql_odbc() {

        let env = Environment::new().unwrap();

        let connection_string
            = "Driver={MySQL ODBC 9.4 Unicode Driver};Server=127.0.0.1;UID=root;PWD=rootroot;database=az_watermark;CHARSET=utf8mb4;";

        let conn = env
            .connect_with_connection_string(&connection_string, ConnectionOptions::default())
            .unwrap();

        let json_str = r#"[{"key":"key1","value":"value1","desc":"中文"}]"#;

        // let odbc_params: Vec<Box<dyn InputParameter>> = ["test", "测试", json_str]
        //     .iter()
        //     .map(|s| s.into_parameter()) // 或者 s.clone().into_parameter()
        //     .map(|p| Box::new(p) as Box<dyn InputParameter>)
        //     .collect();

        let odbc_params: Vec<Box<dyn InputParameter>> = vec![
            Box::new("test".into_parameter()),
            Box::new("测试".into_parameter()),       // UNICODE版本的驱动， UTF-16，中文没问题
            Box::new(VarCharSlice::new(json_str.as_bytes())), // JSON 用 UTF-8
        ];

        conn.execute("DELETE FROM watermark_template WHERE id = ?", odbc_params.as_slice(), None).unwrap();

        conn.execute(
            "INSERT INTO watermark_template (id, name, variables) VALUES (?, ?, ?)",
            odbc_params.as_slice(),
            None
        ).unwrap();

        println!("insert success");
    }

    #[test]
    fn test_dm_odbc()   {
        fast_log::init(fast_log::Config::new().console()).expect("");

        let start_time = std::time::Instant::now();

        let connection_str = "Driver={DM8 ODBC Driver};Server=192.168.50.96:30236;UID=SYSDBA;PWD=SYSDBA001;CHARACTER_CODE=PG_UTF8;SCHEMA=TEST";
        let sql = "select * from test.BIZ_ACTIVITY WHERE ID between ? and ?";

        let env = Environment::new().unwrap();
        let conn = env
            .connect_with_connection_string(connection_str, ConnectionOptions::default())
            .unwrap();

        info!("connection time: {:?}", start_time.elapsed());

        let params = [10, 20];
        // let cursor_impl = conn.execute(&config.sql, ()).unwrap().unwrap();
        // let cursor_impl = conn.execute(&config.sql, &params[..]).unwrap().unwrap();
        // cursor_impl.print_all_tables().unwrap();

        if let Ok(Some(mut cursor)) = conn.execute(&sql, &params[..], None) {
            info!("execute time: {:?}", start_time.elapsed());

            for (i) in 1..cursor.num_result_cols().unwrap_or(0) {
                let mut desc = ColumnDescription {
                    name: vec![],
                    data_type: Default::default(),
                    nullability: Default::default(),
                };
                cursor.describe_col(i as u16, &mut desc).unwrap();
                info!("column {}, desc name: {}, data_type: {:?}, nullability: {:?}",
                    i, desc.name_to_string().unwrap_or("".to_string()), desc.data_type, desc.nullability);
            }

            info!("describe_col time: {:?}", start_time.elapsed());

            // Use cursor to process query results.
            // 4. 打印每行结果
            while let Ok(Some(mut row)) = cursor.next_row() {
                // 假设 column1 是整数，column2 是字符串
                let mut buffer1 = Vec::new();
                let mut buffer2 = Vec::new();
                let column1 = row.get_text(1, &mut buffer1); // 获取第一个列
                let column2 = row.get_text(4, &mut buffer2); // 获取第二个列


                let column1 = String::from_utf8_lossy(&buffer1);

                // // 将 buffer2 转换为 UTF-8 字符串
                // let (decoded, _, had_errors) = GBK.decode(&buffer2);
                // if had_errors {
                //     eprintln!("Error decoding GBK text");
                // }
                //
                // let column2 = decoded.to_string(); // 转换为 String

                let column2 = String::from_utf8_lossy(&buffer2);

                // 打印结果
                info!("column1: {}, column2: {}", column1, column2);
            }
        };

        info!("finish time: {:?}", start_time.elapsed());

    }
}
