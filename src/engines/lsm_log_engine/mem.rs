//! 内存表等

use crate::engines::lsm_log_engine::wal_log::Key;
use crossbeam_skiplist::SkipMap;

/// 单个内存表的结构体表示
#[derive(Debug)]
pub struct MemTable {
    pub table: SkipMap<String,Key>,
    /// 是否可变
    pub status: MemTableStatus,
    num: u8,
}
impl MemTable {
    /// 将当前的table 标记为不可变, 返回当前操作的table的标号
    pub fn mark_imu(&mut self) {
        self.status = MemTableStatus::Imu;
    }
    /// 将当前的table 标记为可变, 返回当前操作的table的标号
    pub fn mark_mut(&mut self) {
        self.status = MemTableStatus::Mut;
    }
    /// 将当前的table 标记为临时状态, 返回当前操作的table的标号
    pub fn mark_temp(&mut self) {
        self.status = MemTableStatus::Temp;
    }
    #[allow(unused)]
    /// 获取当前内存表的数据长度
    pub fn len(&self) -> usize {
        self.table.len()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum MemTableStatus {
    Imu,
    Mut,
    /// 临时中转状态
    Temp
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
            mem_table_01: MemTable { table: Default::default(), status: MemTableStatus::Mut, num: 0 },
            mem_table_02: MemTable { table: Default::default(), status: MemTableStatus::Imu, num: 1 },
        }
    }
    /// 获取其中的可变内存表
    pub fn mut_table(&mut self) -> Option<&mut MemTable> {
        if self.mem_table_01.status == MemTableStatus::Mut {
            Some(&mut self.mem_table_01)
        }else if self.mem_table_02.status == MemTableStatus::Mut {
            Some(&mut self.mem_table_02)
        }else { None }

    }
    /// 获取其中的不可变内存表
    pub fn imu_table(&mut self) -> Option<&mut MemTable> {
        if self.mem_table_01.status == MemTableStatus::Imu {
            Some(&mut self.mem_table_01)
        }else if self.mem_table_02.status == MemTableStatus::Imu {
            Some(&mut self.mem_table_02)
        }else { None }
    }
    /// 获取其中的临时状态内存表
    ///
    /// 注意调用此方法的时候需要确保上文中进行过 `mark_temp` 操作
    pub fn temp_table(&mut self) -> Option<&mut MemTable> {
        match self.mem_table_01.status {
            MemTableStatus::Temp => {Some(&mut self.mem_table_01)}
            _ => {Some(&mut self.mem_table_02)}
        }
    }
    /// 写入memtable
    pub fn add_record(&mut self, key: &Key) {
        loop {
            if self.mut_table().is_some() { break; }
        }
        self.mut_table().unwrap().table.insert(key.get_sort_key(),key.clone());
    }

    /// 调换两个table的状态
    ///
    /// 可能会阻塞
    pub fn exchange(&mut self) {
        // 需要将当前的 memtable 变为 不可变,等待 minor;
        // 然后将原来的不可变变为可变
        // 写入之前要判断
        // 如果 当前imu_table 的长度不为0，表示刷盘动作为完成，阻塞用户的当前操作，
        // 直到 imu_table 的长度为0
        loop {
            // 如果当前 imu_table 没有flush完成，mut_table 又满了，那么这时交换是需要阻塞的，
            // 等imu_table flush 结束之后才会交换状态
            if let Some(imu_table) = self.imu_table() {
                if imu_table.table.is_empty() {
                    break;
                }
            }
        }
        // 中间状态只会存在于这段代码中，
        // 因此 一下的状态的table都必定有值的
        self.mut_table().unwrap().mark_temp();
        self.imu_table().unwrap().mark_mut();
        self.temp_table().unwrap().mark_imu();
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





