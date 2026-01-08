# mysql_binlog_to_starRocks
mysql到starRocks的推送业务，监听mysql的binlog日志，然后python去做中转方案，实时推送写入starRocks中

## 功能说明
这是一个通过监听mysql的数据的binlog然后进行处理后数据匹配后写到远端的starrocks的分析数据库当基础数据，相当于这里做了数据清洗和拆分。

## 前置配置注意事项
这是一个本地联调的项目，线上服务器资源有限，所以在不同机器部署了mysql（开启binlog）和StarRocks数据库，由本程序作为中间件进行数据处理。

## 环境配置步骤

### 1. 网络连接验证
在Windows下，使用以下命令行验证网络连通性：
```powershell
Test-NetConnection 192.168.6.83 -Port 3306  # MySQL端口
Test-NetConnection 192.168.6.30 -Port 9030  # StarRocks端口
```
确保 `TcpTestSucceeded : True` 都为true

### 2. 获取本机IP地址
使用命令查看本机IP：
```cmd
ipconfig
```
例如：IPv4 地址 . . . . . . . . . . . . : 192.168.5.58

### 3. MySQL Binlog配置验证
登录MySQL，执行以下命令验证配置：

#### 检查binlog是否开启
```sql
show variables like 'log_bin';
```
结果应为：`log_bin | ON`

#### 检查binlog格式
```sql
show variables like 'binlog_format';
```
结果应为：`binlog_format | ROW`

#### 查看master状态
```sql
show master status;
```
示例结果：
```
+------------------+----------+--------------+------------------+
| File             | Position | Binlog_Do_DB | Binlog_Ignore_DB |
+------------------+----------+--------------+------------------+
| mysql-bin.000024 | 3620901  |              |                  |
+------------------+----------+--------------+------------------+
```

### 4. MySQL用户权限配置
为确保能读取和监听binlog，需要为用户授权：

```sql
-- 1. 授权data_hourse库的所有常规权限（和你原来的语句一致）
GRANT ALL PRIVILEGES ON data_hourse.* TO 'root'@'192.168.5.58';

-- 2. 授权全局级的复制权限（监听binlog必须的）
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'root'@'192.168.5.58';

-- 3. 刷新权限使配置生效（必须执行）
FLUSH PRIVILEGES;
```

**注意：** 不要先执行第2步再去搞第1步，这样会导致无法连接。

### 5. 查看权限验证
```sql
SHOW GRANTS FOR 'root'@'192.168.5.58';
```

### 6. 环境配置
将上述信息配置到.env文件中，然后使用test_ping.py验证连接：
```bash
python test_ping.py
```

### 7. 运行同步脚本
连接测试通过后，再运行同步脚本。

## 业务说明
本同步脚本涉及2个库2张表的融合同步，因此比较复杂。这只是一个demo，需要结合自己的业务去修改。

## 补充说明

### 为什么选择Flink+StarRocks而不是云方案
本来一期是想直接上云，用RDS + DataWorks + MaxCompute + Hologres，但是这个费用比较高，就替换成为了Flink+StarRocks。中间是各个数据业务收集到OSS，再用Flink清洗拆分到StarRocks里面。

- RDS用于同步数据
- MaxCompute用于处理数据存储
- Hologres用于分析数据

这是典型的ETL（抽取、转换、加载）数据处理技术。

### Flink简介
Apache Flink是一个开源的流处理框架，具有高吞吐量、低延迟的特点，支持批处理和流处理统一计算模型。它能够处理无界和有界数据流，提供精确一次的状态一致性保证，适用于实时数据分析、事件驱动应用等场景。

### Flink到StarRocks的两种同步方式

#### 1. 数据表直接同步
通过业务yaml文件配置表之间的同步关系。但如果从MySQL同步到StarRocks，会遇到兼容性问题，因为两者在表结构和索引上有很大差异：
- MySQL的索引基于B-tree结构
- StarRocks采用列式存储，基于列的索引结构逻辑

StarRocks支持两种主要索引类型：
- **聚合索引**（Aggregate Index）：主要用于加速聚合查询（如SUM、COUNT、AVG），预先计算聚合值并存储在索引中，减少查询时需要扫描的数据量。
- **位图索引**（Bitmap Index）：主要用于加速包含/排除查询（如IN、NOT IN），通过位图形式存储数据，使得查询操作非常高效。

#### 2. 数据到OSS，再通过Flink CDC处理
数据先收集到OSS，然后通过Flink的CDC（Change Data Capture）功能进行处理，最后写入StarRocks。这种方式需要编写配置文件和脚本运行逻辑，更加灵活但配置相对复杂。