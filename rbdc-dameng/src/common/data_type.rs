
use serde::{Deserialize, Serialize};
use std::str::FromStr;
// use crate::common::error::{OdbcStdError, Result};

pub type DmDataType = odbc_api::DataType;

//
// impl Default for DmDataType {
//     fn default() -> Self {
//         Self::Unknown
//     }
// }
//
// impl FromStr for DmDataType {
//     type Err = rbdc::error::Error;
//     // type Err = StdError;
//
//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         let data_type = match &*s.to_uppercase() {
//             "NUMERIC" => Self::NUMERIC,
//             "NUMBER" => Self::NUMBER,
//             "DECIMAL" | "DEC" => Self::DECIMAL,
//             "BIT" => Self::BIT,
//             "INT" | "INTEGER" | "PLS_INTEGER" => Self::INTEGER,
//             "BIGINT" => Self::BIGINT,
//             "TINYINT" => Self::TINYINT,
//             "BYTE" => Self::BYTE,
//             "SMALLINT" => Self::SMALLINT,
//             "BINARY" => Self::BINARY,
//             "VARBINARY" => Self::VARBINARY,
//             "REAL" => Self::REAL,
//             "FLOAT" => Self::FLOAT,
//             "DOUBLE" => Self::DOUBLE,
//             "DOUBLE PRECISION" => Self::DOUBLE_PRECISION,
//             "CHAR" => Self::CHAR,
//             "VARCHAR" | "CHARACTER VARYING" => Self::VARCHAR,
//             "TEXT" => Self::TEXT,
//             "IMAGE" => Self::IMAGE,
//             "BLOB" => Self::BLOB,
//             "CLOB" => Self::CLOB,
//             "BFILE" => Self::BFILE,
//             "DATE" => Self::DATE,
//             "TIME" => Self::TIME,
//             "TIMESTAMP" => Self::TIMESTAMP,
//             "TIME WITH TIME ZONE" => Self::TIME_WITH_TIME_ZONE,
//             "DATETIME WITH TIME ZONE" => Self::TIMESTAMP_WITH_TIME_ZONE,
//             "TIMESTAMP WITH LOCAL TIME ZONE" => Self::TIMESTAMP_WITH_LOCAL_TIME_ZONE,
//             _ => return Err(Self::Err::from(s.to_string())),
//         };
//         Ok(data_type)
//     }
// }
//
// pub trait TryToString {
//     type Err;
//     fn try_to_string(&self) -> Result<String, Self::Err>;
// }
//
// impl TryToString for DmDataType {
//     type Err = rbdc::error::Error;
//
//     fn try_to_string(&self) -> Result<String, Self::Err> {
//         match self {
//             DmDataType::NUMERIC => Ok("NUMERIC".to_string()),
//             DmDataType::NUMBER => Ok("NUMBER".to_string()),
//             DmDataType::DECIMAL => Ok("DECIMAL".to_string()),
//             DmDataType::BIT => Ok("BIT".to_string()),
//             DmDataType::INTEGER => Ok("INT".to_string()),
//             DmDataType::BIGINT => Ok("BIGINT".to_string()),
//             DmDataType::TINYINT => Ok("TINYINT".to_string()),
//             DmDataType::BYTE => Ok("BYTE".to_string()),
//             DmDataType::SMALLINT => Ok("SMALLINT".to_string()),
//             DmDataType::BINARY => Ok("BINARY".to_string()),
//             DmDataType::VARBINARY => Ok("VARBINARY".to_string()),
//             DmDataType::REAL => Ok("REAL".to_string()),
//             DmDataType::FLOAT => Ok("FLOAT".to_string()),
//             DmDataType::DOUBLE => Ok("DOUBLE".to_string()),
//             DmDataType::DOUBLE_PRECISION => Ok("DOUBLE PRECISION".to_string()),
//             DmDataType::CHAR => Ok("CHAR".to_string()),
//             DmDataType::VARCHAR => Ok("VARCHAR".to_string()),
//             DmDataType::TEXT => Ok("TEXT".to_string()),
//             DmDataType::IMAGE => Ok("IMAGE".to_string()),
//             DmDataType::BLOB => Ok("BLOB".to_string()),
//             DmDataType::CLOB => Ok("CLOB".to_string()),
//             DmDataType::BFILE => Ok("BFILE".to_string()),
//             DmDataType::DATE => Ok("DATE".to_string()),
//             DmDataType::TIME => Ok("TIME".to_string()),
//             DmDataType::TIMESTAMP => Ok("TIMESTAMP".to_string()),
//             DmDataType::TIME_WITH_TIME_ZONE => Ok("TIME WITH TIME ZONE".to_string()),
//             DmDataType::TIMESTAMP_WITH_TIME_ZONE => Ok("DATETIME WITH TIME ZONE".to_string()),
//             DmDataType::TIMESTAMP_WITH_LOCAL_TIME_ZONE => {
//                 Ok("TIMESTAMP WITH LOCAL TIME ZONE".to_string())
//             }
//             _ => Err(Self::Err::from(format!("{self:?}"))),
//         }
//     }
// }
