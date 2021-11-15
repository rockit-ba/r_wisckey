use crc32fast::Hasher;

/// 根据字节序列获取 u32 checksum 值
pub fn checksum(content: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(content);
    hasher.finalize()
}