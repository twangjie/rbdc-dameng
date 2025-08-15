use std::str::FromStr;

use bigdecimal::BigDecimal;
// use std::str::FromStr;
use rbs::{Error, Value};

// use std::ops::Index;
//
// use bigdecimal::BigDecimal;


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

fn value_to_json_string(val: &Value) -> String {
    let s = match val {
        Value::Null => "NULL".to_string(), // 或者根据类型自行处理
        _ => serde_json::to_string(val).unwrap(),
    };
    // 转为 ODBC 支持的字符串参数
    s
}

/// 将sql 语名中的 ？ 替换 为Value 中的值
pub fn sql_replacen(mut sql: String, params: Vec<Value>) -> String {
    // let  placeholders=vec!["###","@","##"];
    // let mut  base64s=vec![];
    // let mut index=0;
    for v in params {
        match v {
            // Value::Null => {}
            // Value::Bool(_) => {
            //     sql= sql.replacen("?", &*format!("{}", v), 1);
            // }
            // Value::I32(_) => {}
            // Value::I64(_) => {}
            // Value::U32(_) => {}
            // Value::U64(_) => {}
            // Value::F32(_) => {}
            // Value::F64(_) => {}
            Value::String(_) => {
                // sql = sql.replacen("?", "#", 1);
                // sql = sql.replace("\"", "'");
                //  println!("{}",v);
                //  let v_string=format!("{}",v);
                //  while let Some(find)= v_string.find(placeholders[index]) {
                //      index=index+1;
                //      break;
                //  };
                // println!("{},{}",index,placeholders[index]);
                // let base64= base64_url::encode(v_string.as_str());
                //  base64s.push(v_string);
                // println!("base64={}",base64);
                // let data=base64_url::decode(&base64);
                //  println!("{:?}",data);

                // let v_rep=format!("{}",v).replace("\"",placeholders[index]);
                // let v_rep=v_rep.trim_start_matches(placeholders[index]);
                // let v_rep=v_rep.trim_end_matches(placeholders[index]);
                //
                // println!("{}",v_rep);

                sql = sql.replacen("?", format!("{}", v).as_str(), 1);

                // sql = sql.replace("\"", "'");
            }
            // Value::Binary(_) => {}
            Value::Array(_) => {
                // 转成json字符串
                let json_str =  serde_json::to_string(&v).unwrap_or_default();
                let quoted_json = format!("'{}'", json_str); // 加上单引号
                
                sql = sql.replacen("?", quoted_json.as_str(), 1);

            }
            // Value::Map(_) => {}
            Value::Ext(name, ext_v) => {
                if name.eq("Timestamp") {
                    let v = format!("{}", ext_v);
                    let v = v.parse::<u64>().unwrap_or_default();
                    sql = sql.replacen("?", &*format!("{}", v), 1);
                }
                if name.eq("DateTime") {
                    let v = format!("{}", ext_v);
                    let date = fastdate::DateTime::from_str(&v.as_str())
                        .unwrap_or(fastdate::DateTime::now());
                    // to yyyy-MM-dd HH:mm:ss.SSS

                    //let v= date.format("'YYYY-MM-DD hh:mm:ss'");
                    let mut v = date.to_string();
                    v = format!("'{}'", v.replace("T", " "));
                    // println!("v: {}", v);

                    sql = sql.replacen("?", &*v, 1);
                }
                if name.eq("Time") {
                    sql = sql.replacen("?", &*format!("{}", ext_v), 1);
                }
                if name.eq("Date") {
                    sql = sql.replacen("?", &*format!("{}", ext_v), 1);
                }
            }
            // Value::Map(mut m) => {
            //     //Ok(IsNull::Yes)
            //     println!("{}",m);
            //     let t = m.index("type").as_str().unwrap_or_default();
            //     if t != "" {
            //         match t {
            //             "Date" => {
            //                 let ext_v = m.rm("value");
            //                 sql = sql.replacen("?", &*format!("{}", ext_v), 1);
            //             }
            //             "DateTime" => {
            //                 let ext_v = m.rm("value");
            //                 sql = sql.replacen("?", &*format!("{}", ext_v), 1);
            //             }
            //             "Time" => {
            //                 let ext_v = m.rm("value");
            //                 sql = sql.replacen("?", &*format!("{}", ext_v), 1);
            //             }
            //             "Timestamp" => {
            //                 let ext_v = m.rm("value");
            //                 let ext_v_string=format!("{}", ext_v).replace("TS","");
            //                 sql = sql.replacen("?", &ext_v_string, 1);
            //             }
            //             "Decimal" => {
            //                 let ext_v = m.rm("value");
            //                 sql = sql.replacen("?", &*format!("{}", ext_v), 1);
            //             }
            //             "Json" => {
            //                 let ext_v = m.rm("value");
            //                 sql = sql.replacen("?", &*format!("{}", ext_v), 1);
            //             }
            //             "Uuid" => {
            //                 let ext_v = m.rm("value");
            //                 sql = sql.replacen("?", &*format!("{}", ext_v), 1);
            //             }
            //             _ => {
            //                 let ext_v = m.rm("value");
            //                 sql = sql.replacen("?", &*format!("{}", ext_v), 1);
            //             }
            //         }
            //     }
            // }
            _ => {
                sql = sql.replacen("?", &*format!("{}", v), 1);
            }
        }
    }
    sql = sql.replace("\"", "'");

    // sql=sql.replace(placeholders[index],"\"");


    return sql;
}

#[cfg(test)]
mod test {
    use std::fmt::Debug;

    use rbs::Value;

    // use taos::ColumnView;
    use crate::encode::sql_replacen;

    // use crate::encode::Encode;

    #[test]
    fn test_value() {
        let string_v = Value::String("测试".to_string());
        let timestamp_v = Value::Ext("Timestamp", Box::new(Value::I64(1677859610000)));
        println!("{},{}", timestamp_v, string_v);
        // let mut  cvs:Vec<ColumnView>=vec![];
        // string_v.encode(&mut cvs);
        // timestamp_v.encode(&mut cvs);
        // for cv in cvs {
        //     println!("{}",cv.to_vec());
        // }
    }

    #[test]
    fn string_replacen() {
        let mut sql = "select * from table where id=? and name=? and u32=? and bool=? timestamp<? and date>? and datetime<? and time=?".to_string();
        let json_string = r#"[{"ts##":"2023-04-13 22:32:38.223747","id":null,"device_no":"00","patientId":null,"vital_sign_name":"MDC_PULS_OXIM_SAT_O2","vital_sign_value":"100","vital_sign_unit":"MDC_DIM_PERCENT","acq_timestamp":1666277450000,"time_slot":null,"record_timestamp":null,"userId":null},{"ts":"2023-04-13 22:32:38.223848","id":null,"device_no":"00","patientId":null,"vital_sign_name":"MDC_PULS_OXIM_PULS_RATE","vital_sign_value":"94","vital_sign_unit":"MDC_DIM_BEAT_PER_MIN","acq_timestamp":1666277450000,"time_slot":null,"record_timestamp":null,"userId":null},{"ts":"2023-04-13 22:32:38.223929","id":null,"device_no":"00","patientId":null,"vital_sign_name":"MDC_BLD_PERF_INDEX","vital_sign_value":"2.19","vital_sign_unit":"MDC_DIM_PERCENT","acq_timestamp":1666277450000,"time_slot":null,"record_timestamp":null,"userId":null},{"ts":"2023-04-13 22:32:38.224007","id":null,"device_no":"00","patientId":null,"vital_sign_name":"MDC_TTHOR_RESP_RATE","vital_sign_value":"20","vital_sign_unit":"MDC_DIM_RESP_PER_MIN","acq_timestamp":1666277450000,"time_slot":null,"record_timestamp":null,"userId":null},{"ts":"2023-04-13 22:32:38.224084","id":null,"device_no":"00","patientId":null,"vital_sign_name":"MDC_ECG_V_P_C_RATE","vital_sign_value":"0","vital_sign_unit":"MDC_DIM_BEAT_PER_MIN","acq_timestamp":1666277450000,"time_slot":null,"record_timestamp":null,"userId":null},{"ts":"2023-04-13 22:32:38.224169","id":null,"device_no":"00","patientId":null,"vital_sign_name":"MNDRY_ECG_PAUSE_RATE","vital_sign_value":"0","vital_sign_unit":"MDC_DIM_BEAT_PER_MIN","acq_timestamp":1666277450000,"time_slot":null,"record_timestamp":null,"userId":null},{"ts":"2023-04-13 22:32:38.224248","id":null,"device_no":"00","patientId":null,"vital_sign_name":"MNDRY_ECG_VPB_RATE","vital_sign_value":"0","vital_sign_unit":"MDC_DIM_BEAT_PER_MIN","acq_timestamp":1666277450000,"time_slot":null,"record_timestamp":null,"userId":null},{"ts":"2023-04-13 22:32:38.224324","id":null,"device_no":"00","patientId":null,"vital_sign_name":"MNDRY_ECG_RHY_V_P_C_CPLT_RATE","vital_sign_value":"0","vital_sign_unit":"MDC_DIM_BEAT_PER_MIN","acq_timestamp":1666277450000,"time_slot":null,"record_timestamp":null,"userId":null},{"ts":"2023-04-13 22:32:38.224378","id":null,"device_no":"00","patientId":null,"vital_sign_name":"MNDRY_ECG_RHY_MISSB_RATE","vital_sign_value":"0","vital_sign_unit":"MDC_DIM_BEAT_PER_MIN","acq_timestamp":1666277450000,"time_slot":null,"record_timestamp":null,"userId":null},{"ts":"2023-04-13 22:32:38.224429","id":null,"device_no":"00","patientId":null,"vital_sign_name":"MNDRY_ECG_BEAT_V_P_C_RonT_RATE","vital_sign_value":"0","vital_sign_unit":"MDC_DIM_BEAT_PER_MIN","acq_timestamp":1666277450000,"time_slot":null,"record_timestamp":null,"userId":null},{"ts":"2023-04-13 22:32:38.22446","id":null,"device_no":"00","patientId":null,"vital_sign_name":"MDC_ECG_HEART_RATE","vital_sign_value":"95","vital_sign_unit":"MDC_DIM_BEAT_PER_MIN","acq_timestamp":1666277450000,"time_slot":null,"record_timestamp":null,"userId":null}]"#;
        let name = "字\\'符\\'串";
        let vaules = vec![
            Value::I64(10),
            Value::String(name.to_string()),
            Value::U32(32),
            Value::Bool(false),
            Value::Ext("Timestamp", Box::new(Value::I64(1677859610000))),
            Value::Ext("Date", Box::new(Value::String("2023-03-20".to_string()))),
            // Value::from(Date::from_str("2023-03-20").unwrap()),
            Value::Ext("DateTime", Box::new(Value::String(fastdate::DateTime::now().to_string()))),
            // Value::from(datetime::DateTime::from(fastdate::DateTime::now())),
            Value::Ext("Time", Box::new(Value::String("15:04:05.999999999".to_string()))),
            // Value::from(rbdc::types::time::Time::from_str("15:04:05.999999999").unwrap()),
        ];
        sql = sql_replacen(sql, vaules);
        // sql=sql.replace("\"","'");
        println!("{}", sql);
    }
}
