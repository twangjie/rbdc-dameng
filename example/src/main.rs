use log::{info, LevelFilter};
use rbatis::rbdc::datetime::DateTime;
use rbatis::{crud, impl_delete, impl_select, impl_select_page, impl_update, PageRequest, RBatis};
use rbdc::pool::ConnectionManager;
use rbdc::pool::Pool;
use rbdc::Error;
use rbdc_dameng::driver::DamengDriver;
use rbdc_pool_fast::FastPool;
use rbs::value;
use serde::de::Visitor;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use tokio::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Error> {
    /// enable log crate to show sql logs
    fast_log::init(fast_log::Config::new().console().level(LevelFilter::Debug))
        .expect("rbatis init fail");

    test1().await;

    match test2().await {
        Ok(_) => println!("test2 completed successfully"),
        Err(e) => {
            println!("test1 failed with error: {}", e);
            panic!("{}", e);
        }
    }

    Ok(())
}

async fn test2() -> Result<(), Error> {
    /// 变量
    #[derive(Debug, Serialize, Deserialize, Clone)]
    // 定义一个结构体Variable，用于存储变量信息
    pub struct Variable {
        /// 变量名
        pub key: String,
        /// 变量值
        pub value: String,
        /// 变量描述
        pub desc: Option<String>,
    }

    // 自定义序列化函数
    pub fn serialize_variables<S>(
        variables: &Option<Vec<Variable>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match variables {
            Some(vars) => {
                let s = serde_json::to_string(vars).map_err(serde::ser::Error::custom)?;
                serializer.serialize_str(&s)
            }
            None => serializer.serialize_none(),
        }
    }

    // 自定义反序列化函数
    pub fn deserialize_variables<'de, D>(deserializer: D) -> Result<Option<Vec<Variable>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<String>::deserialize(deserializer)?;
        match opt {
            Some(s) if !s.is_empty() => serde_json::from_str(&s).map_err(serde::de::Error::custom),
            _ => Ok(None),
        }
    }

    /// 水印模板
    #[derive(Debug, Serialize, Deserialize, Clone, Default)]
    pub struct WatermarkTemplate {
        /// 模板ID
        pub id: String,
        /// 模板名称
        pub name: String,
        /// 模板变量
        #[serde(
            serialize_with = "serialize_variables",
            deserialize_with = "deserialize_variables",
            default
        )]
        pub variables: Option<Vec<Variable>>,
        /// 创建时间
        pub created_at: Option<DateTime>, // 或者使用 String
        /// 更新时间
        pub updated_at: Option<DateTime>, // 或者使用 String
    }
    crud!(WatermarkTemplate {});

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Position {
        pub top: i32,  // 百分比
        pub left: i32, // 百分比
    }

    // 自定义反序列化函数
    pub fn deserialize_position<'de, D>(deserializer: D) -> Result<Position, D::Error>
    where
        D: Deserializer<'de>,
    {
        // 定义一个访问者来处理不同类型
        struct PositionVisitor;

        impl<'de> Visitor<'de> for PositionVisitor {
            type Value = Position;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a Position object or a JSON string representing Position")
            }

            // 处理已经解析好的 Position 对象
            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let pos = Position::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(pos)
            }

            // 处理字符串情况
            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if s.is_empty() {
                    // Ok(Some(Position { left: 0, top: 0 }))
                    Ok(Position { left: 0, top: 0 })
                } else {
                    serde_json::from_str(s).map_err(de::Error::custom)
                }
            }

            // 处理 null 情况
            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                // Ok(Some(Position { left: 0, top: 0 }))
                Ok(Position { left: 0, top: 0 })
            }

            // 处理 Option 情况
            fn visit_some<D>(self, d: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserialize_position(d)
            }
        }

        deserializer.deserialize_any(PositionVisitor)
    }

    pub fn serialize_position<S>(pos: &Position, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = serde_json::to_string(pos).map_err(serde::ser::Error::custom)?;
        serializer.serialize_str(&s)

        // match pos {
        //     Some(vars) => {
        //         let s = serde_json::to_string(vars).map_err(serde::ser::Error::custom)?;
        //         serializer.serialize_str(&s)
        //     }
        //     None => serializer.serialize_none(),
        // }
    }

    /// Watermark
    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct Watermark {
        /// 水印ID
        pub id: i64,
        /// 水印名称
        pub name: String,
        /// 模板ID
        pub template_id: Option<String>,
        /// 水印类型 image / text
        pub r#type: String,
        /// 水印位置
        #[serde(
            serialize_with = "serialize_position",
            deserialize_with = "deserialize_position"
        )]
        pub position: Position,
        /// 百分比
        pub size: Option<i32>,
        /// 透明度
        pub opacity: Option<f32>,
        /// 存储不同的结构
        pub extension: Option<String>,
        /// 创建时间
        pub created_at: Option<DateTime>,
        /// 更新时间
        pub updated_at: Option<DateTime>,
    }
    crud!(Watermark {}); //crud = insert+select_by_column+update_by_column+delete_by_column

    let mut start_time = Instant::now();
    let mut begin_tme = start_time.clone();

    let mut connection_string = "odbc://SYSDBA:SYSDBA001@192.168.50.96:30236/az_watermark?CHARACTER_CODE=PG_UTF8&odbc_driver=DM8 ODBC Driver";
    let mut connection_string = "dameng://SYSDBA:SYSDBA001@192.168.50.96:30236/az_watermark?CHARACTER_CODE=PG_UTF8";
    // let mut connection_string = "Driver={DM8 ODBC Driver};Server=192.168.50.96:30236;UID=SYSDBA;PWD=SYSDBA001;CHARACTER_CODE=PG_UTF8;SCHEMA=test";
    // let mut connection_string = "Driver={MySQL ODBC 9.4 Unicode Driver};Server=127.0.0.1;port=3306;NO_BINARY_RESULT=1;UID=root;PASSWORD=rootroot;database=az_watermark;CHARSET=utf8mb4";
    // 从命令行第一个参数获取 connection_string
    let binding = std::env::args()
        .nth(1)
        .unwrap_or(connection_string.to_string());
    connection_string = &*binding;

    let pool = FastPool::new(ConnectionManager::new(DamengDriver {}, connection_string)?)?;
    pool.set_max_open_conns(4).await;
    pool.set_max_idle_conns(4).await;

    let rb = RBatis::new(); // 包含 PageIntercept 分页插件
    rb.pool.set(Box::new(pool)).unwrap();

    WatermarkTemplate::delete_by_map(&rb, value! {"id": &["test"]})
        .await
        .unwrap();

    let variables = vec![Variable {
        key: "key1".to_string(),
        value: "测试".to_string(),
        desc: Some("desc1".to_string()),
    }];

    let template = WatermarkTemplate {
        id: "test".to_string(),
        name: "测试".to_string(),
        variables: Some(variables),
        created_at: None,
        updated_at: None,
    };
    WatermarkTemplate::insert(&rb, &template).await.unwrap();

    let watermark_str = r#"{"id":0,"template_id":"test","name":"测试水印1","type":"text","position":{"left":15,"top":15},"opacity":1,"size":97,"extension":"{\"color\":\"ff0000\",\"content\":\"文本水印\",\"rotation\":0,\"tile\":{\"enable\":false,\"rotation\":0,\"horizontalSpacing\":10,\"verticalSpacing\":10}}"}"#;
    let watermark: Watermark = serde_json::from_str(watermark_str).unwrap();
    Watermark::insert(&rb, &watermark).await.unwrap();

    let wt1 = WatermarkTemplate::select_by_map(&rb, value! {"id": &["test"]})
        .await
        .unwrap();
    info!(
        "[elapsed: {:?}] select_by_column = {:?}",
        start_time.elapsed(),
        wt1
    );

    let templates = WatermarkTemplate::select_all(&rb).await;
    info!(
        "[elapsed: {:?}] select_all = {:?}",
        start_time.elapsed(),
        templates
    );

    let watermark = Watermark::select_by_map(&rb, value! {"template_id": &["test"]})
        .await
        .unwrap();
    info!(
        "[elapsed: {:?}] select_by_column = {:?}",
        start_time.elapsed(),
        watermark
    );

    Ok(())
}

async fn test1() -> Result<(), Error> {
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
    crud!(BizActivity {}); //crud = insert+select_by_column+update_by_column+delete_by_column

    impl_select!(BizActivity{select_all_by_id(id:i32,name:&str) => "`where id = #{id} and name = #{name}`"});
    impl_select!(BizActivity{select_by_id(id:i32) -> Option => "`where id = #{id} limit 1`"});
    impl_update!(BizActivity{update_by_name(name:&str) => "`where name = #{name}`"});
    impl_delete!(BizActivity {delete_by_name(name:&str) => "`where name= #{name}`"});
    impl_delete!(BizActivity {delete_all() => "`where id!=0`"});
    impl_select_page!(BizActivity{select_page(name:&str) => "`where name != #{name}`"});

    let mut start_time = Instant::now();
    let mut begin_tme = start_time.clone();

    let mut connection_string = "odbc://SYSDBA:SYSDBA001@192.168.50.96:30236/test?CHARACTER_CODE=PG_UTF8&odbc_driver=DM8 ODBC Driver";
    let mut connection_string =
        "dameng://SYSDBA:SYSDBA001@192.168.50.96:30236/test?CHARACTER_CODE=PG_UTF8";
    // let mut connection_string = "Driver={DM8 ODBC Driver};Server=192.168.50.96:30236;UID=SYSDBA;PWD=SYSDBA001;CHARACTER_CODE=PG_UTF8;SCHEMA=test";
    // let mut connection_string = "Driver={MySQL ODBC 9.4 Unicode Driver};Server=127.0.0.1;port=3306;UID=root;PASSWORD=rootroot;database=test";

    // 从命令行第一个参数获取 connection_string
    let binding = std::env::args()
        .nth(1)
        .unwrap_or(connection_string.to_string());
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
    rb.exec("truncate table biz_activity", vec![])
        .await
        .expect("truncate table failed");

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
            info!(
                "[elapsed: {:?}] batch insert , result: {:?}",
                start_time.elapsed(),
                data
            );
            start_time = Instant::now();
            batch.clear();
        }
    }

    if batch.len() > 0 {
        let data = BizActivity::insert_batch(&rb, &batch, 100).await;
        info!(
            "[elapsed: {:?}] batch insert , result: {:?}",
            start_time.elapsed(),
            data
        );
        start_time = Instant::now();
        batch.clear();
    }

    let data = BizActivity::select_all_by_id(&rb, 3, "3").await;
    info!(
        "[elapsed: {:?}] select_all_by_id = {:?}",
        start_time.elapsed(),
        data
    );
    start_time = Instant::now();

    let data = BizActivity::select_by_id(&rb, 6).await;
    info!(
        "[elapsed: {:?}] select_by_id = {:?}",
        start_time.elapsed(),
        data
    );
    start_time = Instant::now();

    activity.id = 5;
    activity.name = Some("test".to_string());

    // select_by_map(&rb, value!{"template_id": &["test"]})
    let data = BizActivity::update_by_name(&rb, &activity, "id").await;
    info!(
        "[elapsed: {:?}] update_by_column = {:?}",
        start_time.elapsed(),
        data
    );
    start_time = Instant::now();

    activity.h5_link = Some("http://".to_string());
    let data = BizActivity::update_by_name(&rb, &activity, "test").await;
    info!(
        "[elapsed: {:?}] update_by_name = {:?}",
        start_time.elapsed(),
        data
    );

    let data = BizActivity::delete_by_map(&rb, value! {"id": 1}).await;
    info!(
        "[elapsed: {:?}] delete_by_column = {:?}",
        start_time.elapsed(),
        data
    );

    let data = BizActivity::delete_by_name(&rb, "2").await;
    info!(
        "[elapsed: {:?}] delete_by_column = {:?}",
        start_time.elapsed(),
        data
    );

    let data = BizActivity::select_page(&rb, &PageRequest::new(1, 3), "1").await;
    info!(
        "[elapsed: {:?}] select_page = {:?}",
        start_time.elapsed(),
        data
    );

    info!("finish elapsed time: {:?}", begin_tme.elapsed());

    Ok(())
}
