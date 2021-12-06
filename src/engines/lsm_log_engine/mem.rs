//! 内存表等

use std::collections::BTreeMap;
use crate::engines::lsm_log_engine::wal_log::Key;

#[derive(Debug)]
pub struct MemTable {
    table: BTreeMap<String,Key>,
    /// 是否不可变
    is_imu_table: bool,
}

#[derive(Debug)]
/// 循环地方式使用两个 mem_table
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
    /// 写入memtable
    pub fn add_record(&mut self, key: &Key) {
        let mem_table = if self.mem_table_01.is_imu_table {
            &mut self.mem_table_02
        }else { &mut self.mem_table_01 };

        mem_table.table.insert(key.get_sort_key(),key.clone());
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





