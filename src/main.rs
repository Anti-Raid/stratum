mod config;
mod eventparse;
mod server;

// special cachers
mod cacher_guild;

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
    
    server::server().await
}