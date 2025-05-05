use std::sync::Arc;
use dark_std::sync::SyncVec;
use fast_log::print;
use log::{info, LevelFilter};
use rbatis::{crud, DefaultPool, impl_delete, impl_select, impl_select_page, impl_update, PageRequest, RBatis};
use rbatis::intercept_log::LogInterceptor;
use rbatis::intercept_page::PageIntercept;
use rbdc_dameng::driver::{DamengDriver, OdbcDriver};
use rbs::Value;
use rbdc::db::{Connection, Driver};
use rbdc::Error;
use rbdc::pool::ConnectionManager;
use rbdc::pool::Pool;
use rbdc_mysql::MysqlDriver;
use rbdc_pool_fast::FastPool;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use rbatis::rbdc::datetime::DateTime;
use rbatis::snowflake::Snowflake;
use rbdc_sqlite::SqliteDriver;


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BizActivity {
    pub id: i32,
    pub name: Option<String>,
    pub pc_link: Option<String>,
    pub h5_link: Option<String>,
    pub pc_banner_img: Option<String>,
    pub h5_banner_img: Option<String>,
    pub sort: Option<String>,
    pub status: Option<i32>,
    pub remark: Option<String>,
    pub create_time: Option<DateTime>,
    pub version: Option<i64>,
    pub delete_flag: Option<i32>,
}
crud!(BizActivity{});//crud = insert+select_by_column+update_by_column+delete_by_column

impl_select!(BizActivity{select_all_by_id(id:i32,name:&str) => "`where id = #{id} and name = #{name}`"});
impl_select!(BizActivity{select_by_id(id:i32) -> Option => "`where id = #{id} limit 1`"});
impl_update!(BizActivity{update_by_name(name:&str) => "`where name = #{name}`"});
impl_delete!(BizActivity {delete_by_name(name:&str) => "`where name= #{name}`"});
impl_delete!(BizActivity {delete_all() => "`where id!=0`"});
impl_select_page!(BizActivity{select_page(name:&str) => "`where name != #{name}`"});


#[tokio::main]
async fn main() -> Result<(), Error> {
    /// enable log crate to show sql logs
    fast_log::init(fast_log::Config::new().console().level(LevelFilter::Debug)).expect("rbatis init fail");

    let mut start_time = Instant::now();
    let mut begin_tme = start_time.clone();

    let mut connection_string = "odbc://SYSDBA:SYSDBA001@192.168.50.96:30236/test?CHARACTER_CODE=PG_UTF8&odbc_driver=DM8 ODBC Driver";
    let mut connection_string = "dameng://SYSDBA:SYSDBA001@192.168.50.96:30236/test?CHARACTER_CODE=PG_UTF8";
    // let mut connection_string = "Driver={DM8 ODBC Driver};Server=192.168.50.96:30236;UID=SYSDBA;PWD=SYSDBA001;CHARACTER_CODE=PG_UTF8;SCHEMA=test";
    // let mut connection_string = "Driver={MySQL ODBC 9.1 Unicode Driver};Server=127.0.0.1;port=3306;UID=root;PASSWORD=rootroot;database=test";

    // 从命令行第一个参数获取 connection_string
    let binding = std::env::args().nth(1).unwrap_or(connection_string.to_string());
    connection_string = &*binding;

    let pool = FastPool::new(ConnectionManager::new(DamengDriver {}, connection_string)?)?;
    pool.set_max_open_conns(4).await;
    pool.set_max_idle_conns(4).await;

    let rb = RBatis::new(); // 包含 PageIntercept 分页插件
    rb.pool.set(Box::new(pool)).unwrap();

    let mut activity = BizActivity {
        id: 1,
        name: Some("1".into()),
        pc_link: Some("2".into()),
        h5_link: Some("2".into()),
        pc_banner_img: None,
        h5_banner_img: None,
        sort: None,
        status: Some(2),
        remark: Some("2".into()),
        create_time: Some(DateTime::now()),
        version: Some(1),
        delete_flag: Some(0),
    };

    // BizActivity::delete_all(&rb).await.expect("TODO: panic message");
    // rb.exec("delete from biz_activity where id = ?", vec![]).await.unwrap();
    rb.exec("truncate table biz_activity", vec![]).await.expect("truncate table failed");

    // let table: Option<BizActivity> = rb
    //     .query_decode("select delete_flag from biz_activity limit ?", vec![rbs::to_value!(1)])
    //     .await
    //     .unwrap();
    // println!("table = {:?}", table);

    let mut batch = vec![];
    for i in 1..1001 {
        activity.id = i;
        activity.name = Some(i.to_string());

        // let data = BizActivity::insert(&rb, &activity).await;

        batch.push(activity.clone());

        if i % 100 == 0 {
            let data = BizActivity::insert_batch(&rb, &batch, 100).await;
            info!("[elapsed: {:?}] batch insert , result: {:?}", start_time.elapsed(), data);
            start_time = Instant::now();
            batch.clear();
        }
    }

    if batch.len() > 0 {
        let data = BizActivity::insert_batch(&rb, &batch, 100).await;
        info!("[elapsed: {:?}] batch insert , result: {:?}", start_time.elapsed(), data);
        start_time = Instant::now();
        batch.clear();
    }

    let data = BizActivity::select_all_by_id(&rb, 3, "3").await;
    info!("[elapsed: {:?}] select_all_by_id = {:?}", start_time.elapsed(),  data);
    start_time = Instant::now();

    let data = BizActivity::select_by_id(&rb, 6).await;
    info!("[elapsed: {:?}] select_by_id = {:?}", start_time.elapsed(), data);
    start_time = Instant::now();

    activity.id = 5;
    activity.name = Some("test".to_string());
    let data = BizActivity::update_by_column(&rb, &activity, "id").await;
    info!("[elapsed: {:?}] update_by_column = {:?}",start_time.elapsed(),  data);
    start_time = Instant::now();

    activity.h5_link = Some("http://".to_string());
    let data = BizActivity::update_by_name(&rb, &activity, "test").await;
    info!("[elapsed: {:?}] update_by_name = {:?}", start_time.elapsed(),  data);

    let data = BizActivity::delete_by_column(&rb, "id", 1).await;
    info!("[elapsed: {:?}] delete_by_column = {:?}", start_time.elapsed(),  data);

    let data = BizActivity::delete_by_name(&rb, "2").await;
    info!("[elapsed: {:?}] delete_by_column = {:?}", start_time.elapsed(),  data);

    let data = BizActivity::select_page(&rb, &PageRequest::new(1, 3), "1").await;
    info!("[elapsed: {:?}] select_page = {:?}", start_time.elapsed(),  data);

    info!("finish elapsed time: {:?}", begin_tme.elapsed());

    Ok(())
}

