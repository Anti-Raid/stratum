mod config;
mod eventparse;
mod server;
mod test_client;

// special cachers
mod cacher_guild;

use std::env::args;
use std::io::Write;

pub type Error = Box<dyn std::error::Error + Send + Sync>; // This is constant and should be copy pasted

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
        1 => server::server().await,
        2 => {
            match args.nth(1).expect("Failed to get second arg").as_str() {
                "test_client" => test_client::client().await,
                _ => Err("Usage: stratum [test_client]. (could not recognize second arg)".into())
            }
        }
        _ => Err("Usage: stratum [test_client]".into())
    }
}