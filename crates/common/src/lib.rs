use tonic::Status;

bitflags::bitflags! {
    #[derive(Copy, Clone)]
    pub struct GuildFetchOpts: u32 {
        // Whether to also fetch members
        const INCLUDE_MEMBERS = 1 << 0;
        // Whether to also fetch presences
        const INCLUDE_PRESENCES = 1 << 1;

        // Externally defined 
        const _ = !0;
    }
}

impl GuildFetchOpts {
    /// Returns true if GuildFetchOpts is expensive enough to need to run on its own thread
    pub fn is_expensive(&self) -> bool {
        self.contains(Self::INCLUDE_MEMBERS)
    } 
}

/// Internal transport layer
pub mod pb {
    tonic::include_proto!("stratum");
}

fn encode_any<T: serde::Serialize>(msg: &T) -> Result<String, crate::Error> {
    let bytes = serde_json::to_string(msg)
        .map_err(|e| format!("Failed to serialize Mesophyll any: {}", e))?;
    Ok(bytes)
}

#[allow(dead_code)]
fn decode_any<T: for<'de> serde::Deserialize<'de>>(msg: &str) -> Result<T, crate::Error> {
    let decoded: T = serde_json::from_str(msg)
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

pub type Error = Box<dyn std::error::Error + Send + Sync>; // This is constant and should be copy pasted
