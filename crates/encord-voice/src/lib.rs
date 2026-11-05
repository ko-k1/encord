//! encord-voice: M0 stub. Full voice (M3) = voice WS+UDP, Opus 48kHz stereo,
//! XSalsa20/XChaCha20 via libsodium, RNNoise VAD, DAVE E2EE via libdave.
//! This stub locks the API shape so UI/plugins can build against it now.

use encord_core::Snowflake;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum VoiceError {
    #[error("not connected")]
    NotConnected,
    #[error("unsupported in M0: {0}")]
    Unsupported(String),
}

#[derive(Clone, Debug, Default)]
pub struct VoiceState {
    pub guild_id: Option<Snowflake>,
    pub channel_id: Option<Snowflake>,
    pub muted: bool,
    pub deafened: bool,
    pub speaking: bool,
}

/// M0 no-op handle; `join` validates ids and records intent only.
pub struct Voice {
    state: VoiceState,
}

impl Voice {
    pub fn new() -> Self {
        Self {
            state: VoiceState::default(),
        }
    }

    pub fn state(&self) -> &VoiceState {
        &self.state
    }

    pub fn join(&mut self, guild_id: Snowflake, channel_id: Snowflake) -> Result<(), VoiceError> {
        self.state.guild_id = Some(guild_id);
        self.state.channel_id = Some(channel_id);
        Ok(())
    }

    pub fn leave(&mut self) {
        self.state = VoiceState::default();
    }

    pub fn set_muted(&mut self, muted: bool) {
        self.state.muted = muted;
    }
}

impl Default for Voice {
    fn default() -> Self {
        Self::new()
    }
}
