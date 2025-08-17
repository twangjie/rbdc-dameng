use std::str::FromStr;

use bigdecimal::BigDecimal;
use rbs::{Error, Value};

pub trait Encode {
    fn encode(self, idx: usize) -> Result<String, Error>;
}

impl Encode for Value {
    fn encode(self, _idx: usize) -> Result<String, Error> {
        // let idx = idx + 1;//oracle is one-based

        match self {
            Value::Ext(t, v) => match t {
                "Date" => {
                    let s = v.as_str().unwrap_or_default();
                    // let _d = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
                    // statement.bind(idx, &d).map_err(|e| e.to_string())?
                    return Ok(s.to_string());
                }
                "DateTime" => {
                    let s = v.as_str().unwrap_or_default();
                    // let _d = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%z").unwrap();
                    // statement.bind(idx, &d).map_err(|e| e.to_string())?
                    return Ok(s.to_string());
                }
                "Time" => {
                    //TODO: need to fix this
                    let s = v.into_string().unwrap();
                    // statement.bind(idx, &s).map_err(|e| e.to_string())?
                    return Ok(s.to_string());
                }
                "Decimal" => {
                    let d = BigDecimal::from_str(&v.into_string().unwrap_or_default()).unwrap().to_string();
                    // statement.bind(idx, &d).map_err(|e| e.to_string())?
                    return Ok(d.to_string());
                }
                "Json" => {
                    return Err(Error::from("unimpl"));
                }
                "Timestamp" => {
                    let t = v.as_u64().unwrap_or_default() as i64;
                    // statement.bind(idx, &t).map_err(|e| e.to_string())?
                    return Ok(t.to_string());
                }
                "Uuid" => {
                    let d = v.into_string().unwrap();
                    // statement.bind(idx, &d).map_err(|e| e.to_string())?
                    return Ok(d);
                }
                _ => {
                    return Err(Error::from("unimpl"));
                }
            }
            Value::String(str) => {
                // statement.bind(idx, &str).map_err(|e| e.to_string())?
                return Ok(str);
            }
            Value::U32(u) => {
                // statement.bind(idx, &u).map_err(|e| e.to_string())?
                return Ok(u.to_string());
            }
            Value::U64(u) => {
                // statement.bind(idx, &u).map_err(|e| e.to_string())?
                return Ok(u.to_string());
            }
            Value::I32(int) => {
                // statement.bind(idx, &int).map_err(|e| e.to_string())?
                return Ok(int.to_string());
            }
            Value::I64(long) => {
                // statement.bind(idx, &long).map_err(|e| e.to_string())
                return Ok(long.to_string());
            }
            Value::F32(float) => {
                // statement.bind(idx, &float).map_err(|e| e.to_string())?
                return Ok(float.to_string());
            }
            Value::F64(double) => {
                // statement.bind(idx, &double).map_err(|e| e.to_string())?
                return Ok(double.to_string());
            }
            Value::Binary(_bin) => {
                // statement.bind(idx, &bin).map_err(|e| e.to_string())?
                return Err(Error::from("unimpl"));
            }
            Value::Null => {
                // statement.bind(idx, &Option::<String>::None).unwrap();
                return Ok("NULL".to_string());
            }
            Value::Array(arr) => {

                if arr.is_empty() {
                    return Ok("[]".to_string());
                }

                let s = serde_json::to_string(&arr).unwrap();
                return Ok(s);
            }
            Value::Map(arr) => {

                if arr.is_empty() {
                    return Ok("{}".to_string());
                }

                let s = serde_json::to_string(&arr).unwrap();
                return Ok(s);
            }
            //TODO: more types!
            _ => {
                // statement.bind(idx, &self.to_string()).map_err(|e| e.to_string())?
                return Ok(self.to_string());
            }
        }
        // Ok(())
    }
}
