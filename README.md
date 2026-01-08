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
包在requirements.txt中，请自行安装。