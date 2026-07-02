#![allow(clippy::module_name_repetitions)]
#![allow(clippy::missing_errors_doc)]

pub mod cdc;
pub mod config;
pub mod meili;
pub mod mysql;
pub mod state;
pub mod value;

pub use config::Config;
