//! Test client for stratum

mod config;
mod test_client;

use std::env::args;
use std::io::Write;

pub type Error = stratum_common::Error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut env_builder = env_logger::builder();

    env_builder
        .format(move |buf, record| {
            writeln!(
                buf,
                "({}) {} - {}",
                record.target(),
                record.level(),
                record.args()
            )
        })
        .filter(None, log::LevelFilter::Info);

    env_builder.init();

    // Deref config to trigger loading it before starting up the proxy service
    let _ = &*config::CONFIG;
    
    let mut args = args();
    
    match args.len() {
        1 => test_client::client().await,
        2 => {
            match args.nth(1).expect("Failed to get second arg").as_str() {
                "test_client" => test_client::client().await,
                _ => Err("Usage: stratum [test_client]. (could not recognize second arg)".into())
            }
        }
        _ => Err("Usage: stratum_torracat [test_client]".into())
    }
}
