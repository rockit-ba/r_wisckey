# r_wisckey
rust 实现基于 基本的log（区别于传统的关系型数据库的page存储）存储的kv存储引擎

`v-0.1.0`:

实现基于原始命令行的 `get`  `delete`  `insert` `update`, 数据将在内存索引和.wisc 数据文件中存在。

示例：

```
cargo run --bin wisc_server get aa
```

 `delete`  `insert` `update` 类似。

目前存在的问题，我们是已日志添加的方式记录磁盘数据文件的，因此，删除和更新后的数据将一直存在磁盘文件中，为了节省空间，提高加载速度，我们需要添加数据文件压缩功能，剔除 delete 和 update 的失效数据。

current version： `0.2.0`：**添加日志压缩**。

`fix-bug-0.1.0`-在append方法中添加文件大小限制

`add`-添加配置文件