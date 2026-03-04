mod metadata;
mod methods;
mod models;
mod redis_client;
mod rpc;
mod schema_cache;

use std::collections::HashMap;

fn main() {
    log::info!("Starting Tabularis Redis plugin");
    let mut connections: HashMap<String, redis_client::RedisClient> = HashMap::new();
    rpc::run_loop(&mut connections);
    log::info!("Tabularis Redis plugin stopped");
}
