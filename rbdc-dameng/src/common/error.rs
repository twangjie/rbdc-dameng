// use std::{
//     char::DecodeUtf16Error,
//     num::{ParseFloatError, ParseIntError, TryFromIntError},
//     str::ParseBoolError,
// };
//
// use chrono::ParseError;
// pub use odbc_error::OdbcWrapperError;
//
// use rbdc::{datetime::DateTime, Error};
//
// impl Default for Error {
//     fn default() -> Self {
//         Error::StringError(String::new())
//     }
// }
//
// impl From<&str> for Error {
//     fn from(e: &str) -> Self {
//         Error::StringError(e.into())
//     }
// }
//
// impl From<String> for Error {
//     fn from(e: String) -> Self {
//         Error::StringError(e)
//     }
// }
//
// impl From<odbc_api::Error> for Error {
//     fn from(e: odbc_api::Error) -> Self {
//         Error::OdbcError(e.into())
//     }
// }
//
// impl From<TryFromIntError> for Error {
//     fn from(e: TryFromIntError) -> Self {
//         Error::TypeConversionError(e.to_string())
//     }
// }
//
// impl From<ParseIntError> for Error {
//     fn from(e: ParseIntError) -> Self {
//         Error::TypeConversionError(e.to_string())
//     }
// }
//
// impl From<ParseBoolError> for Error {
//     fn from(e: ParseBoolError) -> Self {
//         Error::TypeConversionError(e.to_string())
//     }
// }
//
// impl From<ParseFloatError> for Error {
//     fn from(e: ParseFloatError) -> Self {
//         Error::TypeConversionError(e.to_string())
//     }
// }
//
// impl From<ParseError> for Error {
//     fn from(e: ParseError) -> Self {
//         Error::TypeConversionError(e.to_string())
//     }
// }
//
// impl From<DecodeUtf16Error> for Error {
//     fn from(e: DecodeUtf16Error) -> Self {
//         Error::OdbcError(OdbcWrapperError::DataHandlerError(e.to_string()))
//     }
// }
//
// pub mod odbc_error {
//     use odbc_api::handles::slice_to_cow_utf8;
//     use std::fmt::{self, Display, Formatter};
//     use std::str::Utf8Error;
//     use rbdc::Error;
//
//     #[derive(Debug, Error)]
//     pub enum OdbcWrapperError {
//         #[error("data handler error:`{0}`")]
//         DataHandlerError(String),
//         #[error("statement error:`{0}`")]
//         StatementError(StatementError),
//     }
//
//     #[derive(Debug, Error)]
//     pub struct StatementError {
//         pub state: String,
//         pub error_msg: String,
//     }
//
//     impl Display for StatementError {
//         fn fmt(&self, f: &mut Formatter) -> fmt::Result {
//             write!(
//                 f,
//                 "state: {:?}, error_msg: {:?}",
//                 self.state, self.error_msg
//             )
//         }
//     }
//
//     impl From<Utf8Error> for OdbcWrapperError {
//         fn from(error: Utf8Error) -> Self {
//             OdbcWrapperError::DataHandlerError(error.to_string())
//         }
//     }
//
//     impl From<odbc_api::Error> for OdbcWrapperError {
//         fn from(error: odbc_api::Error) -> Self {
//             match &error {
//                 odbc_api::Error::Diagnostics { record, .. }
//                 | odbc_api::Error::UnsupportedOdbcApiVersion(record)
//                 | odbc_api::Error::InvalidRowArraySize { record, .. }
//                 | odbc_api::Error::UnableToRepresentNull(record)
//                 | odbc_api::Error::OracleOdbcDriverDoesNotSupport64Bit(record) => {
//                     let msg_info = slice_to_cow_utf8(&record.message);
//                     let state = match std::str::from_utf8(&record.state.0) {
//                         Ok(state_data) => state_data,
//                         Err(e) => {
//                             return OdbcWrapperError::DataHandlerError(e.to_string());
//                         }
//                     };
//                     return OdbcWrapperError::StatementError(StatementError {
//                         state: state.to_string(),
//                         error_msg: msg_info.to_string(),
//                     });
//                 }
//                 _ => {}
//             }
//             OdbcWrapperError::DataHandlerError(error.to_string())
//         }
//     }
// }
