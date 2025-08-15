use std::str::FromStr;

use crate::common::data_type::DmDataType;
use crate::DamengData;
use bigdecimal::BigDecimal;
use odbc_api::sys::SqlDataType;
use rbdc::{datetime::DateTime, Error};
use rbs::Value;

pub trait Decode {
    fn decode(row: &DamengData) -> Result<Value, Error>;
}

/// 将数据库返回的数据转换为 Rbatis 的Value 类型
impl Decode for Value {
    fn decode(row: &DamengData) -> Result<Value, Error> {
        if row.is_sql_null {
            return Ok(Value::Null);
        }

        if row.data.as_ref().is_none() {
            return Ok(Value::Null);
        }

        let value;

        match &row.data {
            Some(data) => {
                value = String::from_utf8(data.to_vec()).unwrap_or_default();
                // // 处理空数组的情况
                // if value.trim() == "[]" {
                //     return Ok(Value::Array(vec![]));
                // }
            }
            None => {
                value = "".to_string();
            }
        }

        match row.column_type {
            DmDataType::Numeric { precision: p, scale: s } => {
                // let value = row.data.as_ref().unwrap().clone();
                if p == 0 && s == -127 {
                    // it means number(*)
                    let dec =
                        BigDecimal::from_str(&value).map_err(|e| Error::from(e.to_string()))?;
                    if dec.is_integer() {
                        let d = dec.digits();
                        if 1 <= d && d <= 9 {
                            let a = value.parse::<i32>()?;
                            return Ok(Value::I32(a));
                        } else if 10 <= d && d <= 18 {
                            let a = value.parse::<i64>()?;
                            return Ok(Value::I64(a));
                        }
                        return Ok(Value::String(dec.to_string()).into_ext("Decimal"));
                    }
                    return Ok(Value::String(dec.to_string()).into_ext("Decimal"));
                }
                if s > 0 {
                    let dec =
                        BigDecimal::from_str(&value).map_err(|e| Error::from(e.to_string()))?;
                    return Ok(Value::String(dec.to_string()).into_ext("Decimal"));
                } else if 1 <= p && p <= 9 {
                    let a = value.parse::<i32>()?;
                    return Ok(Value::I32(a));
                } else if 10 <= p && p <= 18 {
                    let a = value.parse::<i64>()?;
                    return Ok(Value::I64(a));
                }
                let dec = BigDecimal::from_str(&value).map_err(|e| Error::from(e.to_string()))?;
                return Ok(Value::String(dec.to_string()).into_ext("Decimal"));
            }
            DmDataType::SmallInt => {
                let a = value.parse::<i32>()?;
                return Ok(Value::I32(a));
            }
            DmDataType::Integer => {
                let a = value.parse::<i32>()?;
                return Ok(Value::I32(a));
            }
            // DmDataType::Int64 is integer
            DmDataType::BigInt => {
                let a = value.parse::<i64>()?;
                return Ok(Value::I64(a));
            }
            DmDataType::Float { precision: p } => {
                return if p >= 24 {
                    let a = value.parse::<f64>()?;
                    Ok(Value::F64(a))
                } else {
                    let a = value.parse::<f32>()?;
                    Ok(Value::F32(a))
                };
            }
            DmDataType::Double  => {
                let a = value.parse::<f64>()?;
                return Ok(Value::F64(a));
            }
            DmDataType::Binary { length: _ } => {
                if let Some(a) = &row.data {
                    return Ok(Value::Binary(a.clone()));
                }
                return Ok(Value::Null);
            }
            DmDataType::LongVarbinary { length: _ } => {
                if let Some(a) = &row.data {
                    return Ok(Value::Binary(a.clone()));
                }
                return Ok(Value::Null);
            }
            DmDataType::Char { length: _ } => {
                return Ok(Value::String(value));
            }
            DmDataType::Varchar { length: _ } => {
                return Ok(Value::String(value));
            }
            DmDataType::WChar { length: _ } => {
                return Ok(Value::String(value));
            }
            DmDataType::WVarchar { length: _ } => {
                return Ok(Value::String(value));
            }
            // DmDataType::CLOB => {
            //     return Ok(Value::String(value))
            // }
            // DmDataType::LongVarchar { length: _ } => {
            //     return Ok(Value::String(value));
            // }
            DmDataType::Date => {
                let a = DateTime::from_str(&value)?;
                // return Ok(Value::from(a));
                return Ok(Value::Ext("Date", Box::new(Value::I64(a.unix_timestamp_millis()))));
            }
            DmDataType::Time { precision: _ } => {
                // let date=FastDateTime::from_str(&value).unwrap().unix_timestamp_millis();
                //  let timestamp=Timestamp::from_str(&value).unwrap();
                //  // let tv=TV::new("Timestamp",Value::I64(date));
                // return  Ok(Value::from(timestamp));
                let date = DateTime::from_str(&value).unwrap().unix_timestamp_millis();
                return Ok(Value::Ext("Time", Box::new(Value::I64(date))));
                //
                // let datetime=DateTime::from_str(&value).unwrap();
                // return Ok(Value::from(datetime));
            }
            DmDataType::Timestamp { precision: _ } => {
                // let date=FastDateTime::from_str(&value).unwrap().unix_timestamp_millis();
                //  let timestamp=Timestamp::from_str(&value).unwrap();
                //  // let tv=TV::new("Timestamp",Value::I64(date));
                // return  Ok(Value::from(timestamp));
                let date = DateTime::from_str(&value).unwrap().unix_timestamp_millis();
                return Ok(Value::Ext("Timestamp", Box::new(Value::I64(date))));
                //
                // let datetime=DateTime::from_str(&value).unwrap();
                // return Ok(Value::from(datetime));
            }
            DmDataType::Other { data_type, column_size: _, decimal_digits: _ } => {
                return match data_type {
                    SqlDataType::CHAR => { Ok(Value::String(value)) }
                    SqlDataType::NUMERIC => { Ok(Value::String(value)) }
                    SqlDataType::DECIMAL => { Ok(Value::String(value)) }
                    SqlDataType::INTEGER => { Ok(Value::I32(value.parse::<i32>()?)) }
                    SqlDataType::SMALLINT => { Ok(Value::I32(value.parse::<i32>()?)) }
                    SqlDataType::FLOAT => { Ok(Value::F32(value.parse::<f32>()?)) }
                    SqlDataType::REAL => { Ok(Value::F32(value.parse::<f32>()?)) }
                    SqlDataType::DOUBLE => { Ok(Value::F64(value.parse::<f64>()?)) }
                    SqlDataType::DATETIME => { Ok(Value::String(value)) }
                    SqlDataType::VARCHAR => { Ok(Value::String(value)) }
                    SqlDataType::DATE => { Ok(Value::String(value)) }
                    SqlDataType::TIME => { Ok(Value::String(value)) }
                    SqlDataType::TIMESTAMP => { Ok(Value::String(value)) }
                    SqlDataType::EXT_TIME_OR_INTERVAL => { Ok(Value::String(value)) }
                    SqlDataType::EXT_TIMESTAMP => { Ok(Value::String(value)) }
                    SqlDataType::EXT_LONG_VARCHAR => { Ok(Value::String(value)) }
                    SqlDataType::EXT_BINARY => { Ok(Value::String(value)) }
                    SqlDataType::EXT_VAR_BINARY => { Ok(Value::String(value)) }
                    SqlDataType::EXT_LONG_VAR_BINARY => { Ok(Value::String(value)) }
                    SqlDataType::EXT_BIG_INT => { Ok(Value::I64(value.parse::<i64>()?)) }
                    SqlDataType::EXT_TINY_INT => { Ok(Value::I32(value.parse::<i32>()?)) }
                    SqlDataType::EXT_BIT => { Ok(Value::I32(value.parse::<i32>()?)) }
                    SqlDataType::EXT_W_CHAR => { Ok(Value::String(value)) }
                    SqlDataType::EXT_W_VARCHAR => { Ok(Value::String(value)) }
                    SqlDataType::EXT_W_LONG_VARCHAR => { Ok(Value::String(value)) }
                    SqlDataType::EXT_GUID => { Ok(Value::String(value)) }
                    _ => { Ok(Value::String(value)) }
                };
            }
            _ => {
                // return Err(Error::from(format!("Unsupported type: {:?}", row.column_type)));
                return Ok(Value::String(value));
                // if !value.is_empty() {
                //     // 处理空数组的情况
                //     match value.trim() {
                //         "null" | "NULL" => {
                //             return Ok(Value::Null);
                //         }
                //         "[]" => {
                //             return Ok(Value::Array(vec![]));
                //         }
                //         "{}" => {
                //             return Ok(Value::Map(ValueMap::new()));
                //         }
                //         _ => {
                //             // return Ok("".to_string().into());
                //         }
                //     }
                // 
                //     if value.trim().starts_with("[") {
                //         let value = value.trim();
                // 
                //         match serde_json::from_str::<Value>(&value) {
                //             Ok(map) => {
                //                 return Ok(map);
                //             }
                //             Err(_) => {
                //                 return Ok("".to_string().into());
                //             }
                //         }
                //     }
                // 
                //     if value.trim().starts_with("{") {
                //         let value = value.trim();
                //         match serde_json::from_str::<Value>(&value) {
                //             Ok(map) => {
                //                 return Ok(map);
                //             }
                //             Err(_) => {
                //                 return Ok("".to_string().into());
                //             }
                //         }
                //     }
                //     return Ok(Value::String(value));
                // 
                // 
                // } else {
                //     return Ok("".to_string().into());
                // }
            }
        };
    }
}
