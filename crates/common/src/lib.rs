use serde::Deserialize;
use tonic::Status;

/// Internal transport layer
pub mod pb {
    tonic::include_proto!("stratum");
}

fn encode_any<T: serde::Serialize>(msg: &T) -> Result<Vec<u8>, crate::Error> {
    let bytes = rmp_serde::encode::to_vec(msg)
        .map_err(|e| format!("Failed to serialize Mesophyll any: {}", e))?;
    Ok(bytes)
}

#[allow(dead_code)]
fn decode_any<T: for<'de> serde::Deserialize<'de>>(msg: &[u8]) -> Result<T, crate::Error> {
    let decoded: T = rmp_serde::from_slice(msg)
        .map_err(|e| format!("Failed to deserialize Mesophyll any: {}", e))?;
    Ok(decoded)
}

#[allow(dead_code)]
impl pb::AnyValue {
    pub fn from_real<T: serde::Serialize>(value: &T) -> Result<Self, Status> {
        Self::from_real_exec(value).map_err(|e| Status::internal(e.to_string()))
    }

    pub fn from_real_exec<T: serde::Serialize>(value: &T) -> Result<Self, crate::Error> {
        let data = encode_any(value)
            .map_err(|e| format!("Failed to encode response value: {}", e))?;
        Ok(Self { data })
    }

    pub fn to_real<T: for<'de> serde::Deserialize<'de>>(&self) -> Result<T, Status> {
        self.to_real_exec().map_err(|e| Status::internal(e.to_string()))
    }

    pub fn to_real_exec<T: for<'de> serde::Deserialize<'de>>(&self) -> Result<T, crate::Error> {
        let val = decode_any(&self.data)
            .map_err(|e| format!("Failed to decode request value: {}", e))?;
        Ok(val)
    }
}

impl pb::DiscordEvent {
    /// Extracts the id, event name and the discord event data corresponding to said event from a DiscordEvent
    pub fn extract<'a>(&'a self) -> Result<(pb::Id, &'a str, serde_json::Value), crate::Error> {
        #[derive(Deserialize)]
        pub struct Evt {
            d: serde_json::Value,
        }

        let Some(id) = self.id else {
            return Err("protocol violation: id not set".into());
        };

        let payload = serde_json::from_str::<Evt>(&self.payload)?;

        Ok((id, &self.event_name, payload.d))
    }
}

pub type Error = Box<dyn std::error::Error + Send + Sync>; // This is constant and should be copy pasted
