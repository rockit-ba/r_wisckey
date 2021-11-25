# r_wisckey
rust 实现基于 基本的log（区别于传统的关系型数据库的page存储）存储的kv存储引擎

=====================================================
示例：

服务启动：

```
cargo run --bin wisc_server
```
客户端命令启动：

```
cargo run --bin wisc_client
```
命令行：

    wisc-db>> get 桐人;

`delete`  `insert` `update` 类似。

==============================================================

在 `base_log` 版本，我们实现了基本的基于日志的键值存储，但它并不是日志存储的惯用成熟方案。

相对，日志存储的常见数据布局是`LSM-tree` ,它的基本思想是将数据在内存中排序，然后顺序持久化到磁盘。

在接下来的后续中后台线程不断压缩磁盘中的数据文件，去除无效数据。

详细的相关资料可查看相关的一些论文或者参考`levelDB` 的wiki。

**大概的操作流程**：

1.写WAL LOG

2.更新内存：`MemTable`

3.当`MemTable` size 达到一定程度的时候。把`Memtable`变成不可变的内存块。 把这个不可变的内存块与磁盘上的`SSTable`文件进行合并。

4.磁盘上的`SSTable`根据新旧先后分层。总是上面一层的与下面一层的合并。

5.读的时候先查`MemTable`，没有的时候，再顺次从`Level-0`~`Level-N`里面的`SSTable` 中查找。
## **current version**： `base_lsm_0.1.0`：LSM。

待更新。。。

======================================================================

**log:**
