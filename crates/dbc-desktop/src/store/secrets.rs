//! Encrypted credential vault.
//!
//! The OS keyring this replaces needs a D-Bus session bus and a running Secret
//! Service daemon on Linux, so it simply fails over SSH, in containers and on
//! headless machines. A local file encrypted with a master password behaves
//! identically on every platform.
//!
//! Only the container format is written here — key derivation is Argon2id and
//! the cipher is ChaCha20-Poly1305, both from audited implementations. No
//! cryptographic primitive is implemented by hand.

use std::{collections::BTreeMap, path::Path};

use argon2::{Algorithm, Argon2, Params, Version};
use chacha20poly1305::{
    ChaCha20Poly1305, Key, Nonce,
    aead::{Aead as _, Generate, KeyInit as _},
};
use zeroize::Zeroize as _;

use crate::atomic_file::write_atomic;

/// File magic plus format version, so a future format change is detected
/// rather than decrypted as garbage.
const MAGIC: &[u8; 8] = b"DBCVLT\x00\x01";
const SALT_LEN: usize = 32;
const NONCE_LEN: usize = 12;

#[derive(Debug, thiserror::Error)]
pub enum VaultError {
    #[error("凭据库读写失败：{0}")]
    Io(String),
    #[error("主密码不正确，或凭据库已损坏")]
    Unlock,
    #[error("凭据库格式无法识别")]
    Format,
    #[error("凭据库内容已损坏")]
    Corrupt,
    #[error("主密码不能为空")]
    EmptyMasterPassword,
}

/// Unlocked credential store, held in memory for the session.
pub struct Vault {
    key: Key,
    salt: [u8; SALT_LEN],
    secrets: BTreeMap<String, String>,
}

impl std::fmt::Debug for Vault {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Vault")
            .field("entries", &self.secrets.len())
            .finish_non_exhaustive()
    }
}

impl Drop for Vault {
    fn drop(&mut self) {
        for secret in self.secrets.values_mut() {
            secret.zeroize();
        }
    }
}

impl Vault {
    /// Start an empty vault protected by `master`.
    ///
    /// # Errors
    ///
    /// Returns [`VaultError::EmptyMasterPassword`] for a blank password.
    pub fn create(master: &str) -> Result<Self, VaultError> {
        if master.is_empty() {
            return Err(VaultError::EmptyMasterPassword);
        }
        let salt = random_bytes();
        Ok(Self {
            key: derive_key(master, &salt)?,
            salt,
            secrets: BTreeMap::new(),
        })
    }

    /// Decrypt an existing vault.
    ///
    /// # Errors
    ///
    /// Returns [`VaultError::Unlock`] for a wrong master password — the same
    /// error as a tampered file, so the caller cannot distinguish them.
    pub fn unlock(encoded: &[u8], master: &str) -> Result<Self, VaultError> {
        if master.is_empty() {
            return Err(VaultError::EmptyMasterPassword);
        }
        let header_len = MAGIC.len() + SALT_LEN + NONCE_LEN;
        if encoded.len() < header_len || &encoded[..MAGIC.len()] != MAGIC {
            return Err(VaultError::Format);
        }
        let mut salt = [0_u8; SALT_LEN];
        salt.copy_from_slice(&encoded[MAGIC.len()..MAGIC.len() + SALT_LEN]);
        let nonce = Nonce::try_from(&encoded[MAGIC.len() + SALT_LEN..header_len])
            .map_err(|_| VaultError::Format)?;
        let key = derive_key(master, &salt)?;

        let plaintext = ChaCha20Poly1305::new(&key)
            .decrypt(&nonce, &encoded[header_len..])
            .map_err(|_| VaultError::Unlock)?;
        let secrets = serde_json::from_slice(&plaintext).map_err(|_| VaultError::Corrupt)?;

        Ok(Self { key, salt, secrets })
    }

    /// Encrypt and atomically replace the vault file.
    ///
    /// A fresh nonce is drawn on every save; the key and its salt stay put so
    /// the (deliberately slow) derivation runs once per session.
    ///
    /// # Errors
    ///
    /// Returns [`VaultError::Io`] when the file cannot be replaced.
    pub fn save(&self, path: &Path) -> Result<(), VaultError> {
        let mut plaintext = serde_json::to_vec(&self.secrets)
            .map_err(|error| VaultError::Io(error.to_string()))?;
        let nonce = Nonce::generate();
        let ciphertext = ChaCha20Poly1305::new(&self.key)
            .encrypt(&nonce, plaintext.as_slice())
            .map_err(|_| VaultError::Corrupt)?;
        plaintext.zeroize();

        let mut encoded = Vec::with_capacity(
            MAGIC.len() + SALT_LEN + NONCE_LEN + ciphertext.len(),
        );
        encoded.extend_from_slice(MAGIC);
        encoded.extend_from_slice(&self.salt);
        encoded.extend_from_slice(&nonce);
        encoded.extend_from_slice(&ciphertext);

        write_atomic(path, &encoded, true).map_err(|error| VaultError::Io(error.to_string()))
    }

    #[must_use]
    pub fn get(&self, id: &str) -> Option<&str> {
        self.secrets.get(id).map(String::as_str)
    }

    pub fn set(&mut self, id: impl Into<String>, secret: impl Into<String>) {
        if let Some(previous) = self.secrets.insert(id.into(), secret.into()) {
            let mut previous = previous;
            previous.zeroize();
        }
    }

    pub fn remove(&mut self, id: &str) {
        if let Some(mut secret) = self.secrets.remove(id) {
            secret.zeroize();
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.secrets.len()
    }
}

/// Argon2id with the reference implementation's defaults (19 MiB, 2 passes).
fn derive_key(master: &str, salt: &[u8]) -> Result<Key, VaultError> {
    let mut material = [0_u8; 32];
    Argon2::new(Algorithm::Argon2id, Version::V0x13, Params::default())
        .hash_password_into(master.as_bytes(), salt, &mut material)
        .map_err(|_| VaultError::Unlock)?;
    let key = Key::try_from(&material[..]).map_err(|_| VaultError::Corrupt)?;
    material.zeroize();
    Ok(key)
}

/// A salt drawn from the operating system CSPRNG.
fn random_bytes() -> [u8; SALT_LEN] {
    <[u8; SALT_LEN] as Generate>::generate()
}

#[cfg(test)]
mod tests {
    use super::{Vault, VaultError};

    #[test]
    fn a_saved_vault_round_trips_through_the_correct_master_password() {
        let directory = std::env::temp_dir().join("dbc-vault-roundtrip");
        let _ignored = std::fs::remove_dir_all(&directory);
        let path = directory.join("secrets.bin");

        let mut vault = Vault::create("correct horse").expect("vault should be created");
        vault.set("connection-1", "s3cret");
        vault.save(&path).expect("vault should be written");

        let encoded = std::fs::read(&path).expect("vault file should exist");
        let reopened = Vault::unlock(&encoded, "correct horse").expect("vault should unlock");

        assert_eq!(reopened.get("connection-1"), Some("s3cret"));
        let _ignored = std::fs::remove_dir_all(&directory);
    }

    #[test]
    fn a_wrong_master_password_is_rejected() {
        let mut vault = Vault::create("right").expect("vault should be created");
        vault.set("connection-1", "s3cret");
        let directory = std::env::temp_dir().join("dbc-vault-wrong-password");
        let _ignored = std::fs::remove_dir_all(&directory);
        let path = directory.join("secrets.bin");
        vault.save(&path).expect("vault should be written");
        let encoded = std::fs::read(&path).expect("vault file should exist");

        assert!(matches!(
            Vault::unlock(&encoded, "wrong"),
            Err(VaultError::Unlock)
        ));
        let _ignored = std::fs::remove_dir_all(&directory);
    }

    #[test]
    fn the_stored_file_never_contains_the_plaintext_secret() {
        let directory = std::env::temp_dir().join("dbc-vault-ciphertext");
        let _ignored = std::fs::remove_dir_all(&directory);
        let path = directory.join("secrets.bin");
        let mut vault = Vault::create("master").expect("vault should be created");
        vault.set("connection-1", "plaintext-must-not-leak");
        vault.save(&path).expect("vault should be written");

        let encoded = std::fs::read(&path).expect("vault file should exist");

        assert!(
            !encoded
                .windows("plaintext-must-not-leak".len())
                .any(|window| window == b"plaintext-must-not-leak"),
            "the vault file must not contain the secret in the clear"
        );
        let _ignored = std::fs::remove_dir_all(&directory);
    }

    #[test]
    fn a_truncated_or_foreign_file_is_reported_as_a_format_error() {
        assert!(matches!(
            Vault::unlock(b"not a vault", "master"),
            Err(VaultError::Format)
        ));
    }

    #[test]
    fn an_empty_master_password_is_refused() {
        assert!(matches!(
            Vault::create(""),
            Err(VaultError::EmptyMasterPassword)
        ));
    }
}
