use anyhow::anyhow;
use clap::ValueHint::Url;
use futures_core::future::BoxFuture;
use log::info;
use rbdc::db::{Connection, ConnectOptions};
use rbdc::Error;
use serde::{Deserialize, Serialize};

use crate::connection::DamengConnection;

#[derive(Serialize, Deserialize, Debug)]
pub struct DamengConnectOptions {
    pub connection_string: String,
    pub batch_size: usize,
    pub max_str_len: Option<usize>,
}

impl ConnectOptions for DamengConnectOptions {
    fn connect(&self) -> BoxFuture<Result<Box<dyn Connection>, Error>> {
        Box::pin(async move {
            let v = DamengConnection::establish(self)
                .await
                .map_err(|e| Error::from(e.to_string()))?;
            Ok(Box::new(v) as Box<dyn Connection>)
        })
    }

    fn set_uri(&mut self, url: &str) -> Result<(), Error> {
        *self = DamengConnectOptions::from_str(url)?;
        Ok(())
    }
}

impl Default for DamengConnectOptions {
    fn default() -> Self {
        Self {
            connection_string: "{DM8 ODBC Driver};Server=192.168.50.96:30236;UID=SYSDBA;PWD=SYSDBA001;CHARACTER_CODE=PG_UTF8;".to_owned(),
            batch_size: 100,
            max_str_len: Some(65536),
        }
    }
}

impl DamengConnectOptions {
    pub fn from_str(s: &str) -> Result<Self, Error> {
        // serde_json::from_str(s).map_err(|e| Error::from(e.to_string()))

        // dameng://localhost:5236?user=SYSDBA&password=123456789&SCHEMA=test
        // {DM8 ODBC Driver};Server=192.168.50.96:30236;UID=SYSDBA;PWD=SYSDBA001;CHARACTER_CODE=PG_UTF8;SCHEMA=test;

        let mut connectiong_string = s.to_owned();

        // dm://SA:TestPass!123456@localhost:1433/test
        if connectiong_string.starts_with("dameng://") ||
            connectiong_string.starts_with("odbc://") {
            let url = url::Url::parse(&connectiong_string)
                .map_err(|e| anyhow!("invalid url: {}", e))
                .unwrap();

            let mut driver = "".to_string();
            if let Some(driver_key_pair) = url.query_pairs().find(|(k, _)| k.to_lowercase() == "odbc_driver") {
                if !driver_key_pair.1.to_string().is_empty() {
                    driver = driver_key_pair.1.to_string();
                }
            }

            if driver.is_empty() {
                if connectiong_string.starts_with("dameng://") {
                    driver = "DM8 ODBC Driver".to_string();
                } else {
                    return Err("odbc_driver is empty".into());
                }
            }

            connectiong_string = format!("Driver={{{}}};Server={}:{};UID={};PWD={};CHARACTER_CODE=PG_UTF8",
                                         driver,
                                         url.host_str().unwrap_or("localhost"),
                                         url.port().unwrap_or(5236),
                                         url.username(),
                                         url.password().unwrap_or(""),
            );
            let path = url.path();
            if !path.is_empty() {
                connectiong_string.push_str(&format!(";SCHEMA={}", path[1..].to_owned()));
            }
        }

        let mut conn = DamengConnectOptions::default();
        conn.connection_string = connectiong_string;

        info!("connection_string: {}", conn.connection_string);

        Ok(conn)
    }

    pub fn set_batch_size(&mut self, batch_size: usize) {
        self.batch_size = batch_size;
    }

    pub fn set_max_str_len(&mut self, max_str_len: usize) {
        self.max_str_len = Some(max_str_len);
    }
}
