//! 内存表等

use std::collections::BTreeMap;
use crate::engines::lsm_log_engine::wal_log::Key;

/// 单个内存表的结构体表示
#[derive(Debug)]
pub struct MemTable {
    pub table: BTreeMap<String,Key>,
    /// 是否不可变
    pub is_imu_table: bool,
}
impl MemTable {
    /// 将当前的table 标记为不可变
    pub fn mark_imu(&mut self) {
        self.is_imu_table = true;
    }
    /// 将当前的table 标记为可变
    pub fn mark_mut(&mut self) {
        self.is_imu_table = false;
    }
    /// 获取当前内存表的数据长度
    pub fn len(&self) -> usize {
        self.table.len()
    }
}
/// 包含可变和不可变内存表的结构体表示
///
/// 循环地方式使用两个 mem_table
#[derive(Debug)]
pub struct MemTables {
    mem_table_01: MemTable,
    mem_table_02: MemTable,
}
impl MemTables {
    pub fn new() -> Self {
        MemTables {
            mem_table_01: MemTable { table: Default::default(), is_imu_table: false },
            mem_table_02: MemTable { table: Default::default(), is_imu_table: true },
        }
    }
    /// 获取其中的可变内存表
    pub fn mut_table(&mut self) -> &mut MemTable {
        if self.mem_table_01.is_imu_table {
            &mut self.mem_table_02
        }else {
            &mut self.mem_table_01 }
    }
    /// 获取其中的不可变内存表
    pub fn imu_table(&mut self) -> &mut MemTable {
        if self.mem_table_01.is_imu_table {
            &mut self.mem_table_01
        }else {
            &mut self.mem_table_02 }
    }
    /// 写入memtable
    pub fn add_record(&mut self, key: &Key) {
        self.mut_table().table
            .insert(key.get_sort_key(),key.clone());
    }
}


#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test() {
        let a = MemTables::new();
        println!("{:?}",a);
    }
}





