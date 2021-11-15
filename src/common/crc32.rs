use crc32fast::Hasher;


pub fn checksum(content: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(content);
    hasher.finalize()
}