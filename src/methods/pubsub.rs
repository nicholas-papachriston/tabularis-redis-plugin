use crate::redis_client::RedisClient;
use serde_json::Value as JsonValue;

/// Returns active Pub/Sub channels with subscriber counts.
/// Uses PUBSUB CHANNELS * and PUBSUB NUMSUB for each channel.
pub fn get_pubsub_channels(client: &mut RedisClient) -> Result<JsonValue, String> {
    let channel_names = client.pubsub_channels(None)?;
    let counts = client.pubsub_numsub(&channel_names)?;
    let count_map: std::collections::HashMap<String, u64> = counts.into_iter().collect();
    let channels: Vec<JsonValue> = channel_names
        .into_iter()
        .map(|name| {
            let subscribers = count_map.get(&name).copied().unwrap_or(0);
            serde_json::json!({
                "name": name,
                "subscribers": subscribers
            })
        })
        .collect();
    Ok(serde_json::json!({ "channels": channels }))
}
