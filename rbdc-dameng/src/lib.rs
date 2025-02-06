use std::sync::Arc;

use rbdc::db::{MetaData, Row};
use rbs::Value;

pub use driver::DamengDriver;
pub use driver::DamengDriver as Driver;

use crate::common::data_type::DmDataType;
use crate::decode::Decode;

pub mod common;
pub mod decode;
pub mod driver;
pub mod encode;
pub mod options;
pub mod connection;

#[derive(Debug, Clone)]
pub struct DamengColumn {
    pub name: String,
    pub column_type: DmDataType,
    pub nullability: bool,
}

#[derive(Debug)]
pub struct DamengMetaData(pub Arc<Vec<DamengColumn>>);

impl MetaData for DamengMetaData {
    fn column_len(&self) -> usize {
        self.0.len()
    }

    fn column_name(&self, i: usize) -> String {
        self.0[i].name.to_string()
    }

    fn column_type(&self, i: usize) -> String {
        format!("{:?}", self.0[i].column_type)
    }
}

#[derive(Debug)]
pub struct DamengData {
    pub data: Option<Vec<u8>>,
    pub column_type: DmDataType,
    pub is_sql_null: bool,
}


#[derive(Debug)]
pub struct DamengRow {
    pub columns: Arc<Vec<DamengColumn>>,
    pub datas: Vec<DamengData>,
}


impl Row for DamengRow {
    fn meta_data(&self) -> Box<dyn MetaData> {
        Box::new(DamengMetaData(self.columns.clone()))
    }

    fn get(&mut self, i: usize) -> Result<Value, rbdc::Error> {
        Value::decode(
            &self.datas[i],
        )
    }
}