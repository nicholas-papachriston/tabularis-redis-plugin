mod metadata;
mod methods;
mod models;
mod redis_client;
mod rpc;

use std::collections::HashMap;

fn main() {
    let mut logger = env_logger::Builder::from_env(
        env_logger::Env::default().filter_or("RUST_LOG", "tabularis_redis_plugin=info"),
    );
    logger.target(env_logger::Target::Stderr).init();

    log::info!("Starting Tabularis Redis plugin");
    let mut connections: HashMap<String, redis_client::RedisClient> = HashMap::new();
    rpc::run_loop(&mut connections);
    log::info!("Tabularis Redis plugin stopped");
}
