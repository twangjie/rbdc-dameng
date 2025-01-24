
#[cfg(test)]
mod test {
    use rbdc::db::Placeholder;
    use rbdc::pool::conn_manager::ConnManager;
    use rbdc::pool::Pool;
    use rbdc_pool_fast::FastPool;
    use rbs::{Error, Value};
    use tokio::time::Instant;
    use rbdc_dameng::driver::DamengDriver;

    #[tokio::test]
    async fn test_odbc_driver() -> Result<(), Error> {

        let connection_string = "Driver={DM8 ODBC Driver};Server=192.168.50.96:30236;UID=SYSDBA;PWD=SYSDBA001;CHARACTER_CODE=PG_UTF8;SCHEMA=AZCMS";
        let pool = FastPool::new(ConnManager::new(DamengDriver {}, connection_string)?)?;

        // let connection_string = "Driver={MySQL ODBC 9.1 Unicode Driver};Server=192.168.50.253;port=3306;UID=root;PASSWORD=Azsy12345.;database=AZCMS";
        // let pool = FastPool::new(ConnManager::new(DamengDriver {}, connection_string)?)?;

        let connection_string = "Driver={MySQL ODBC 9.1 Unicode Driver};Server=127.0.0.1;port=3306;UID=root;PASSWORD=rootroot;database=AZCMS";
        let pool = FastPool::new(ConnManager::new(DamengDriver {}, connection_string)?)?;

        let mut conn = pool.get().await?;

        // let v = conn.get_values("select * from userinfo", vec![]).await?;
        // println!("{}", rbs::Value::Array(v));

        let pingResult = conn.ping().await;
        println!("ping result: {:?}", pingResult);

        let mut start_time = Instant::now();

        let data = conn.exec("SELECT  * from azcms.subequipment limit 5", vec![]).await?;
        println!("exec result: {:?}, elapsed: {} ms", data, start_time.elapsed().as_millis());

        start_time = Instant::now();
        let param: Vec<Value> = vec![0.into(), 10.into()];
        // let param: Vec<Value> = vec![];

        let data = conn
            .get_values("SELECT  * from azcms.subequipment where subequipmentid > ? order by subequipmentid limit ?", param)
            // .get_values("SELECT  * from azcms.subequipment where subequipmentid = ?", vec![10.into()])
            .await
            .unwrap();

        let mut idx = 0;
        for x in data {
            idx+=1;
            println!("[{}] {}", idx, x);
        }
        println!("get_values elapsed: {} ms", start_time.elapsed().as_millis());

        Ok(())
   }
}
