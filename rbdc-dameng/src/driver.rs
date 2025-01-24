use crate::options::DamengConnectOptions;
use crate::connection::DamengConnection;
use futures_core::future::BoxFuture;
use rbdc::db::{ConnectOptions, Connection};
use rbdc::db::{Driver, Placeholder};
use rbdc::{Error, impl_exchange};

#[derive(Debug)]
pub struct DamengDriver {}

impl Driver for DamengDriver {
    fn name(&self) -> &str {
        "Dameng"
    }

    fn connect(&self, _url: &str) -> BoxFuture<Result<Box<dyn Connection>, Error>> {
        Box::pin(async move {
            unimplemented!();
        })
    }

    fn connect_opt<'a>(
        &'a self,
        opt: &'a dyn ConnectOptions,
    ) -> BoxFuture<Result<Box<dyn Connection>, Error>> {
        let opt = opt.downcast_ref::<DamengConnectOptions>().unwrap();
        Box::pin(async move {
            let conn = DamengConnection::establish(opt).await?;
            Ok(Box::new(conn) as Box<dyn Connection>)
        })
    }

    fn default_option(&self) -> Box<dyn ConnectOptions> {
        Box::new(DamengConnectOptions::default())
    }
}

impl Placeholder for DamengDriver {
    fn exchange(&self, sql: &str) -> String {
        impl_exchange(":",1,sql)
    }
}

impl DamengDriver {
    pub fn pub_exchange(&self, sql: &str) -> String{
        self.exchange(sql)
    }
}