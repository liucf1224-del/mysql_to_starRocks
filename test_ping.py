import os
from dotenv import load_dotenv
import pymysql

# 加载.env配置
load_dotenv(encoding='utf-8')

# 测试MySQL连接
try:
    mysql_conn = pymysql.connect(
        host=os.getenv('MYSQL_HOST'),
        port=int(os.getenv('MYSQL_PORT')),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        db=os.getenv('MYSQL_DB'),
        charset='utf8mb4'
    )
    print("✅ MySQL连接成功")
    mysql_conn.close()
except Exception as e:
    print(f"❌ MySQL连接失败：{e}")

# 测试StarRocks连接
try:
    sr_conn = pymysql.connect(
        host=os.getenv('STARROCKS_HOST'),
        port=int(os.getenv('STARROCKS_PORT')),
        user=os.getenv('STARROCKS_USER'),
        password=os.getenv('STARROCKS_PASSWORD'),
        db=os.getenv('STARROCKS_DB'),
        charset='utf8mb4'
    )
    print("✅ StarRocks连接成功")
    sr_conn.close()
except Exception as e:
    print(f"❌ StarRocks连接失败：{e}")