//! Backport from https://docs.rs/twilight-gateway/latest/src/twilight_gateway/json.rs.html#26-84 but borrows the event string instead of taking ownership of it.

use twilight_gateway::EventTypeFlags;
use serde::de::DeserializeSeed;
use twilight_model::gateway::{
    OpCode,
    event::{GatewayEvent, GatewayEventDeserializer},
};

/// Parse a JSON encoded gateway event into a `GatewayEvent` if
/// `wanted_event_types` contains its type. Also returns the event type as a string if present in JSON
///
/// # Errors
///
/// Returns a [`ReceiveMessageErrorType::Deserializing`] error if the *known*
/// event could not be deserialized.
pub fn parse(
    event: &str,
    wanted_event_types: EventTypeFlags,
) -> Result<(Option<GatewayEvent>, Option<String>, OpCode), crate::Error> {
    let Some(gateway_deserializer) = GatewayEventDeserializer::from_json(event) else {
        return Err(format!("Failed to parse event: {}", event).into());
    };

    let Some(opcode) = OpCode::from(gateway_deserializer.op()) else {
        return Ok((None, None, OpCode::Identify));
    };

    let event_type_raw = gateway_deserializer.event_type();

    let Ok(event_type) = EventTypeFlags::try_from((opcode, event_type_raw)) else {
        return Ok((None, None, opcode));
    };

    if wanted_event_types.contains(event_type) {
        let mut json_deserializer = serde_json::Deserializer::from_str(&event);
        let event_type_raw = event_type_raw.map(|s| s.to_string());

        Ok((gateway_deserializer
            .deserialize(&mut json_deserializer)
            .map(Some)?, event_type_raw, opcode))
    } else {
        Ok((None, None, opcode))
    }
}