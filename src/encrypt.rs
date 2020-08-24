use crate::Result;

#[cfg(feature = "aes-gcm")]
use aes_gcm::{
    aead::{generic_array::GenericArray, Aead},
    Aes256Gcm,
};

pub trait Encrypt {
    fn decrypt(&self, data: &[u8], nonce: &[u8]) -> Result<Vec<u8>>;
    fn encrypt(&self, data: &[u8], nonce: &[u8]) -> Result<Vec<u8>>;
}

#[cfg(feature = "aes-gcm")]
impl Encrypt for Aes256Gcm {
    fn encrypt(&self, data: &[u8], nonce: &[u8]) -> Result<Vec<u8>> {
        let mut fallback = Default::default();
        let nonce = prepare_nonce_aes_gcm(nonce, &mut fallback);
        Aead::encrypt(self, nonce, data).map_err(crate::Error::AesGcm)
    }

    fn decrypt(&self, data: &[u8], nonce: &[u8]) -> Result<Vec<u8>> {
        let mut fallback = Default::default();
        let nonce = prepare_nonce_aes_gcm(nonce, &mut fallback);
        Aead::decrypt(self, nonce, data).map_err(crate::Error::AesGcm)
    }
}

#[cfg(feature = "aes-gcm")]
fn prepare_nonce_aes_gcm<'a>(
    key: &'a [u8],
    fallback: &'a mut [u8; 12],
) -> &'a GenericArray<u8, aes_gcm::aead::consts::U12> {
    // aes needs nonce of 12 bytes.
    if key.len() >= 12 {
        // if the key is longer than the required len, we just take the required data from the key.
        // This is a zero copy (fastest)
        GenericArray::from_slice(&key[0..12])
    } else {
        // if the key is shorter than the required len, we pad with 0
        // This requires copy but since we are using a fallback, we do not need heap allocation (faster).
        *fallback = [0u8; 12];
        fallback[0..key.len()].copy_from_slice(&key);
        GenericArray::from_slice(&*fallback)
    }
}
