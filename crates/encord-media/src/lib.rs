//! encord-media: CDN download + disk cache for guild icons and image
//! attachments. Files are named by sha256(url) so repeat views never
//! re-download; the UI renders the local path (GPU-decoded by Slint).

use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MediaError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("too large: {0} bytes")]
    TooLarge(u64),
}

#[derive(Clone)]
pub struct MediaCache {
    dir: PathBuf,
    client: reqwest::Client,
}

impl MediaCache {
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self, MediaError> {
        let dir = dir.into();
        std::fs::create_dir_all(dir.join("icons"))?;
        std::fs::create_dir_all(dir.join("attachments"))?;
        let client = reqwest::Client::builder()
            .user_agent(encord_identity_ua())
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self { dir, client })
    }

    pub fn guild_icon_url(guild_id: u64, hash: &str) -> String {
        // Animated hashes start with `a_`; callers pass png to freeze them
        // when animations are off (Discord serves a still for .png).
        format!("https://cdn.discordapp.com/icons/{guild_id}/{hash}.png?size=64")
    }

    fn filename(url: &str, content_type: Option<&str>) -> String {
        let mut hasher = Sha256::new();
        hasher.update(url.as_bytes());
        let hex = format!("{:x}", hasher.finalize());
        format!("{hex}{}", extension_for(url, content_type))
    }

    fn path_for(&self, kind: &str, filename: &str) -> PathBuf {
        self.dir.join(kind).join(filename)
    }

    /// Local path if already cached, without any I/O beyond metadata.
    pub fn cached(&self, kind: &str, url: &str, content_type: Option<&str>) -> Option<PathBuf> {
        let path = self.path_for(kind, &Self::filename(url, content_type));
        path.is_file().then_some(path)
    }

    /// Download (unless cached) with a size cap. Returns the local path.
    pub async fn fetch(
        &self,
        kind: &str,
        url: &str,
        content_type: Option<&str>,
        max_bytes: u64,
    ) -> Result<PathBuf, MediaError> {
        if let Some(path) = self.cached(kind, url, content_type) {
            return Ok(path);
        }
        let res = self.client.get(url).send().await?;
        if !res.status().is_success() {
            return Err(MediaError::Http(res.error_for_status().unwrap_err()));
        }
        if let Some(len) = res.content_length() {
            if len > max_bytes {
                return Err(MediaError::TooLarge(len));
            }
        }
        let body = res.bytes().await?;
        if body.len() as u64 > max_bytes {
            return Err(MediaError::TooLarge(body.len() as u64));
        }
        let path = self.path_for(kind, &Self::filename(url, content_type));
        // Racy double-downloads just overwrite identical bytes; write to a
        // temp file + rename for readers that may already hold the path.
        let tmp = path.with_extension("part");
        std::fs::write(&tmp, &body)?;
        std::fs::rename(&tmp, &path)?;
        Ok(path)
    }
}

fn encord_identity_ua() -> String {
    // Media CDN is less picky; a plain browser UA avoids fingerprint drift
    // between app releases.
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36".to_string()
}

fn extension_for(url: &str, content_type: Option<&str>) -> &'static str {
    if let Some(ct) = content_type {
        let ct = ct.split(';').next().unwrap_or("").trim();
        return match ct {
            "image/png" => ".png",
            "image/jpeg" => ".jpg",
            "image/gif" => ".gif",
            "image/webp" => ".webp",
            _ => ".bin",
        };
    }
    let path = url.split('?').next().unwrap_or("");
    let lower = path.to_lowercase();
    if lower.ends_with(".png") {
        ".png"
    } else if lower.ends_with(".jpg") || lower.ends_with(".jpeg") {
        ".jpg"
    } else if lower.ends_with(".gif") {
        ".gif"
    } else if lower.ends_with(".webp") {
        ".webp"
    } else {
        ".bin"
    }
}

/// Cache directory next to the config file (or cwd fallback).
pub fn default_dir(config_path: &Path) -> PathBuf {
    config_path
        .parent()
        .map(|p| p.join("cache"))
        .unwrap_or_else(|| PathBuf::from("cache"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn icon_url_shape() {
        assert_eq!(
            MediaCache::guild_icon_url(123, "abc"),
            "https://cdn.discordapp.com/icons/123/abc.png?size=64"
        );
    }

    #[test]
    fn filenames_are_stable_and_typed() {
        let a = MediaCache::filename("https://x/y.png?size=64", Some("image/png"));
        let b = MediaCache::filename("https://x/y.png?size=64", Some("image/png"));
        assert_eq!(a, b);
        assert!(a.ends_with(".png"));
        assert_ne!(
            a,
            MediaCache::filename("https://x/other.png?size=64", Some("image/png"))
        );
        assert!(MediaCache::filename("https://x/y", None).ends_with(".bin"));
    }

    #[test]
    fn cached_miss_then_hit() {
        let dir = std::env::temp_dir().join("encord-media-test");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = MediaCache::new(&dir).unwrap();
        assert!(cache.cached("icons", "https://x/y.png", None).is_none());
        let path = cache.path_for("icons", &MediaCache::filename("https://x/y.png", None));
        std::fs::write(&path, b"fake").unwrap();
        assert_eq!(cache.cached("icons", "https://x/y.png", None), Some(path));
        std::fs::remove_dir_all(&dir).ok();
    }
}
