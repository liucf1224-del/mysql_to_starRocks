import os
import random
import logging
import pymysql
from dotenv import load_dotenv
from pymysql.err import OperationalError, ProgrammingError
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent
)

# ===================== 1. 加载.env配置文件（Windows路径） =====================
# 把.env文件和脚本放在同一目录，或指定绝对路径（比如D:\binlog_sync\.env）
env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=env_path, encoding='utf-8')  # 加编码避免中文乱码

# ===================== 2. 日志配置（Windows路径） =====================
# 日志文件放在脚本同目录的logs文件夹（自动创建）
log_dir = os.path.join(os.path.dirname(__file__), 'logs')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'club_sync.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ===================== 3. 数据库连接函数 =====================
def get_mysql_conn():
    """获取MySQL连接（fa_clubs所在库）"""
    try:
        conn = pymysql.connect(
            host=os.getenv('MYSQL_HOST'),
            port=int(os.getenv('MYSQL_PORT')),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD'),
            db=os.getenv('MYSQL_DB'),
            charset='utf8mb4',
            connect_timeout=10  # 增加超时时间，避免连接慢
        )
        logger.info("MySQL连接成功")
        return conn
    except OperationalError as e:
        logger.error(f"MySQL连接失败：{e}")
        raise


def get_starrocks_conn():
    """获取StarRocks连接（兼容MySQL协议）"""
    try:
        conn = pymysql.connect(
            host=os.getenv('STARROCKS_HOST'),
            port=int(os.getenv('STARROCKS_PORT')),
            user=os.getenv('STARROCKS_USER'),
            password=os.getenv('STARROCKS_PASSWORD'),
            db=os.getenv('STARROCKS_DB'),
            charset='utf8mb4',
            connect_timeout=10
        )
        logger.info("StarRocks连接成功")
        return conn
    except OperationalError as e:
        logger.error(f"StarRocks连接失败：{e}")
        raise


# ===================== 4. binlog位置管理（Windows路径） =====================
def save_binlog_pos(binlog_file, binlog_pos):
    """保存当前处理的binlog位置（断点续传）"""
    try:
        pos_file = os.path.join(log_dir, 'binlog_pos.txt')
        with open(pos_file, 'w', encoding='utf-8') as f:
            f.write(f"{binlog_file}\n{binlog_pos}")
        logger.info(f"已保存binlog位置：{binlog_file} - {binlog_pos}")
    except Exception as e:
        logger.error(f"保存binlog位置失败：{e}")


def load_binlog_pos():
    """加载上次处理的binlog位置"""
    pos_file = os.path.join(log_dir, 'binlog_pos.txt')
    if not os.path.exists(pos_file):
        # 首次运行，使用.env中的初始位置
        init_file = os.getenv('MYSQL_BINLOG_FILE')
        init_pos = int(os.getenv('MYSQL_BINLOG_POS'))
        logger.info(f"首次运行，使用初始binlog位置：{init_file} - {init_pos}")
        return init_file, init_pos
    try:
        with open(pos_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            binlog_file = lines[0].strip()
            binlog_pos = int(lines[1].strip())
        logger.info(f"加载到上次binlog位置：{binlog_file} - {binlog_pos}")
        return binlog_file, binlog_pos
    except Exception as e:
        logger.error(f"加载binlog位置失败，使用初始值：{e}")
        return os.getenv('MYSQL_BINLOG_FILE'), int(os.getenv('MYSQL_BINLOG_POS'))


# ===================== 5. 核心同步函数（补全缺失的逻辑） =====================

def sync_to_starrocks(club_data):
    """将单条club数据同步到StarRocks clubs_mix表（高性能+高可靠+易调试）"""
    if not club_data:
        logger.warning("同步数据为空，跳过")
        return

    # 1. 定义StarRocks表默认值映射
    starrocks_defaults = {
        'id': 0, 'provider_id': 0, 'code': '', 'name': '', 'is_test_club': 0,
        'name_of_registration': '', 'type': '', 'organization_credit_code': '',
        'legal_representative': '', 'principal_name': '', 'principal_phone': '',
        'reservation_phone': '', 'number_of_chain_clubs': 1, 'number_of_employees': 1,
        'area_of_club': 0, 'province': '', 'city': '', 'county': '', 'address': '',
        'introduce': '', 'trial_count': 0, 'max_client_count': 0, 'online_client_count': 0,
        'export_area': 0, 'disabled': 0, 'admin_id': 0, 'status': 2, 'check_status': 0,
        'comment': None, 'check_comment': None, 'create_time': 0, 'update_time': None,
        'forbid_load': 0, 'facilitator_id': None, 'signup_status': 0, 'signup_time': None,
        'sync_status': 0, 'sync_switch_status': 0, 'sync_time': None, 'sync_switch_time': None,
        'expiration_time': None, 'is_culturaltravel': 1, 'business_status': 1, 'cavca_status': 1,
        'cavca_stop_time': None, 'cavca_cancel_stop_time': None, 'cavca_room_number': 0,
        'cavca_sign_status': 0, 'code_time': None, 'url': None, 'code_url': None, 'is_branch': 0,
        'latest_order_time': None, 'account': 0, 'grade': 0, 'downloadLimit': 1000,
        'activeDate': None, 'deviceCode': '', 'serialNumber': '', 'clientCount': 0,
        'keepLoginTime': 0, 'loginUserId': 0, 'ipAreaId': 1, 'forbiddenSongsVer': 1,
        'forbiddenSingersVer': 1, 'vipClientCount': -1, 'bannedSongVer': 1,
        'bannedSingerVer': 1, 'vipStatus': 1, 'vipStopDate': None, 'version': None,
    }

    # 2. 合并数据（MySQL数据覆盖默认值）
    final_data = starrocks_defaults.copy()
    for key, value in club_data.items():
        if key in final_data and value is not None:
            final_data[key] = value

    # 3. 构建参数化SQL（核心：单SQL+全%s占位）
    table_name = 'clubs_mix'
    fields = [f'`{k}`' for k in final_data.keys()]
    placeholders = ['%s'] * len(fields)
    values = list(final_data.values())

    # 调试日志（融合你的优点）
    logger.info(f"同步准备 - 字段数: {len(fields)}, 参数数: {len(values)}, 目标ID: {final_data['id']}")
    for i, (field, val) in enumerate(zip(fields[:5], values[:5])):  # 打印前5个参数便于调试
        logger.debug(f"参数{i+1} | {field}: {val} (类型: {type(val)})")

    # 4. 执行同步（主逻辑：高性能INSERT ON DUPLICATE，兜底：先查后插/更）
    conn = None
    cursor = None
    try:
        conn = get_starrocks_conn()
        cursor = conn.cursor()

        # 主逻辑：单SQL完成插入/更新（高性能+原子性）
        update_clause = ', '.join([f'{f}=VALUES({f})' for f in fields if f != '`id`'])
        insert_sql = f"""
            INSERT INTO {table_name} ({', '.join(fields)}) 
            VALUES ({', '.join(placeholders)})
            ON DUPLICATE KEY UPDATE {update_clause}
        """
        cursor.execute(insert_sql, values)
        conn.commit()
        logger.info(f"同步成功 - StarRocks | id={final_data['id']}")

    except Exception as e:
        logger.error(f"主同步逻辑失败 - 错误: {e} (类型: {type(e).__name__})")
        if conn:
            conn.rollback()

        # 兜底逻辑：先查后插/更（仅主逻辑失败时触发）
        try:
            logger.info(f"触发兜底逻辑 - 尝试先查后插/更 | id={final_data['id']}")
            # 检查记录是否存在
            check_sql = f"SELECT 1 FROM {table_name} WHERE id = %s LIMIT 1"
            cursor.execute(check_sql, (final_data['id'],))
            exists = cursor.fetchone()

            if exists:
                # 执行更新
                update_fields = [f for f in fields if f != '`id`']
                update_placeholders = [f'{f}=%s' for f in update_fields]
                update_values = [final_data[k.strip('`')] for k in update_fields] + [final_data['id']]
                update_sql = f"UPDATE {table_name} SET {', '.join(update_placeholders)} WHERE id = %s"
                cursor.execute(update_sql, update_values)
                logger.info(f"兜底逻辑 - 更新成功 | id={final_data['id']}")
            else:
                # 执行插入
                insert_sql_simple = f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
                cursor.execute(insert_sql_simple, values)
                logger.info(f"兜底逻辑 - 插入成功 | id={final_data['id']}")
            conn.commit()

        except Exception as e2:
            logger.error(f"兜底逻辑也失败 - 错误: {e2} | 数据: {final_data}")
            if conn:
                conn.rollback()
            raise  # 抛出异常，让上层感知同步失败

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def parse_binlog_old():
    """解析MySQL binlog并同步到StarRocks"""
    # 加载binlog起始位置
    binlog_file, binlog_pos = load_binlog_pos()

    # 配置binlog读取器（必须开启MySQL的binlog，且格式为ROW）
    stream_settings = {
        "host": os.getenv('MYSQL_HOST'),
        "port": int(os.getenv('MYSQL_PORT')),
        "user": os.getenv('MYSQL_USER'),
        "password": os.getenv('MYSQL_PASSWORD'),
        "database": os.getenv('MYSQL_DB'),  # 只监听指定数据库
        "table": "fa_clubs",  # 只监听fa_clubs表
        "log_file": binlog_file,
        "log_pos": binlog_pos,
        "resume_stream": True,  # 断点续传
        "server_id": random.randint(100000, 999999),  # 唯一server_id，避免冲突
        "only_events": [WriteRowsEvent, UpdateRowsEvent,DeleteRowsEvent]  # 只监听插入/更新事件
    }

    # 启动binlog监听
    stream = BinLogStreamReader(**stream_settings)
    logger.info("开始监听MySQL binlog...")

    try:
        for binlog_event in stream:
            # 记录当前binlog位置
            current_pos = stream.log_pos
            logger.debug(f"当前binlog位置：{stream.log_file}:{current_pos}")

            # 只处理fa_clubs表的事件
            if hasattr(binlog_event, 'table') and binlog_event.table != 'fa_clubs':
                continue

            # 处理插入事件
            if isinstance(binlog_event, WriteRowsEvent):
                logger.info(f"捕获INSERT事件 - 表: {binlog_event.table}, 事件位置: {stream.log_file}:{current_pos}")
                for row in binlog_event.rows:
                    club_data = row['values']
                    logger.info(f"捕获插入事件：{club_data}")
                    sync_to_starrocks(club_data)

            # 处理更新事件
            elif isinstance(binlog_event, UpdateRowsEvent):
                logger.info(f"捕获UPDATE事件 - 表: {binlog_event.table}, 事件位置: {stream.log_file}:{current_pos}")
                for row in binlog_event.rows:
                    # row['before_values']是更新前，row['after_values']是更新后
                    # club_data = row['after_values']
                    before_data = row['before_values']
                    after_data = row['after_values']
                    # logger.info(f"捕获更新事件：{club_data}")
                    logger.info(f"更新前: {before_data}")
                    logger.info(f"更新后: {after_data}")
                    sync_to_starrocks(after_data)

                    # 处理删除事件
            elif isinstance(binlog_event, DeleteRowsEvent):
                logger.info(f"捕获DELETE事件 - 表: {binlog_event.table}, 事件位置: {stream.log_file}:{current_pos}")
                for row in binlog_event.rows:
                    club_data = row['values']
                    logger.info(f"删除数据: {club_data}")
                    # 从StarRocks删除对应记录
                    delete_from_starrocks(club_data)

            # 保存当前binlog位置（断点续传）
            save_binlog_pos(stream.log_file, stream.log_pos)

    except Exception as e:
        logger.error(f"解析binlog异常：{e}")
        raise
    finally:
        stream.close()
        logger.info("关闭binlog连接")


def parse_binlog():
    """解析MySQL binlog并同步到StarRocks"""
    # 加载binlog起始位置
    binlog_file, binlog_pos = load_binlog_pos()

    # ========== 修正：连接参数需要放入 connection_settings 字典 ==========
    connection_settings = {
        "host": os.getenv('MYSQL_HOST'),
        "port": int(os.getenv('MYSQL_PORT')),
        "user": os.getenv('MYSQL_USER'),
        "passwd": os.getenv('MYSQL_PASSWORD'),  # pymysql 使用 passwd 或 password，但在 mysql-replication 中通常建议用 connection_settings 传参
        "charset": "utf8mb4",
    }

    # 映射表字段：根据 StarRocks 的 clubs_mix 表结构手动定义 fa_clubs 的列名
    # 注意：binlog 中的顺序必须与 MySQL 原表 fa_clubs 的列定义顺序完全一致
    # 这里我们根据日志中 'UNKNOWN_COL0' 到 'UNKNOWN_COL52' 的顺序，结合 StarRocks DDL 推断列名
    # 请确保这个顺序与 MySQL `desc fa_clubs` 的顺序完全一致！
    ctl_columns = [
        'id', 'provider_id', 'code', 'name', 'is_test_club', 
        'name_of_registration', 'type', 'organization_credit_code', 'legal_representative', 'principal_name',
        'principal_phone', 'reservation_phone', 'number_of_chain_clubs', 'number_of_employees', 'area_of_club',
        'province', 'city', 'county', 'address', 'introduce',
        'trial_count', 'max_client_count', 'online_client_count', 'export_area', 'disabled',
        'admin_id', 'status', 'check_status', 'comment', 'check_comment',
        'create_time', 'update_time', 'forbid_load', 'facilitator_id', 'signup_status',
        'signup_time', 'sync_status', 'sync_switch_status', 'sync_time', 'sync_switch_time',
        'expiration_time', 'is_culturaltravel', 'business_status', 'cavca_status', 'cavca_stop_time',
        'cavca_cancel_stop_time', 'cavca_room_number', 'cavca_sign_status', 'code_time', 'url',
        'code_url', 'is_branch', 'latest_order_time'
    ]

    # binlog读取器配置
    stream_settings = {
        "connection_settings": connection_settings,
        "server_id": random.randint(100000, 999999),
        "only_events": [WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
        "log_file": binlog_file,
        "log_pos": binlog_pos,
        "resume_stream": True,
        "blocking": True,  # 阻塞模式，持续监听新事件
        "only_schemas": [os.getenv('MYSQL_DB')],
        "only_tables": ["fa_clubs"]
    }

    # 启动binlog监听
    stream = BinLogStreamReader(**stream_settings)
    logger.info("开始监听MySQL binlog...")

    try:
        for binlog_event in stream:
            # 记录当前binlog位置
            current_pos = stream.log_pos
            logger.debug(f"当前binlog位置：{stream.log_file}:{current_pos}")

            # 只处理fa_clubs表的事件（binlog_event.schema是数据库名，table是表名）
            if hasattr(binlog_event, 'schema') and binlog_event.schema != os.getenv('MYSQL_DB'):
                continue
            if hasattr(binlog_event, 'table') and binlog_event.table != 'fa_clubs':
                continue

            # 处理插入事件
            if isinstance(binlog_event, WriteRowsEvent):
                logger.info(f"捕获INSERT事件 - 表: {binlog_event.table}, 事件位置: {stream.log_file}:{current_pos}")
                for row in binlog_event.rows:
                    # 映射列名
                    if 'values' in row:
                        raw_values = row['values']
                        # 如果是 UNKNOWN_COL 格式，尝试手动映射
                        if any(k.startswith('UNKNOWN_COL') for k in raw_values.keys()):
                            # 按顺序提取值
                            # 注意：mysql-replication 返回的 values 字典在 Python 3.7+ 中是保持插入顺序的
                            # 但为了保险，我们最好根据 'UNKNOWN_COLx' 的索引来排序
                            sorted_values = []
                            for i in range(len(raw_values)):
                                key = f'UNKNOWN_COL{i}'
                                if key in raw_values:
                                    sorted_values.append(raw_values[key])
                                else:
                                    # 假如某个索引不存在，可能出问题了，或者列数不对
                                    logger.warning(f"无法找到列 {key}，原始数据: {raw_values}")
                                    break
                            
                            # 重新组装成带列名的字典
                            if len(sorted_values) == len(ctl_columns):
                                club_data = dict(zip(ctl_columns, sorted_values))
                                logger.info(f"手动映射列名成功: {club_data}")
                            else:
                                logger.error(f"列数不匹配！定义了 {len(ctl_columns)} 列，但 binlog 数据有 {len(sorted_values)} 列。请检查 ctl_columns 定义。")
                                club_data = raw_values # 回退到原始数据，虽然会报错
                        else:
                            club_data = raw_values

                    # club_data = row['values']
                    # logger.info(f"捕获插入事件：{club_data}")
                    sync_to_starrocks(club_data)

            # 处理更新事件
            elif isinstance(binlog_event, UpdateRowsEvent):
                logger.info(f"捕获UPDATE事件 - 表: {binlog_event.table}, 事件位置: {stream.log_file}:{current_pos}")
                for row in binlog_event.rows:
                    before_data = row['before_values']
                    after_data = row['after_values']
                    
                    # 同样处理列映射
                    if any(k.startswith('UNKNOWN_COL') for k in after_data.keys()):
                        sorted_values = []
                        for i in range(len(after_data)):
                            key = f'UNKNOWN_COL{i}'
                            if key in after_data:
                                sorted_values.append(after_data[key])
                        
                        if len(sorted_values) == len(ctl_columns):
                            after_data = dict(zip(ctl_columns, sorted_values))
                            logger.info(f"手动映射更新后数据成功")
                    
                    logger.info(f"更新前: {before_data}")
                    logger.info(f"更新后: {after_data}")
                    sync_to_starrocks(after_data)

            # 处理删除事件
            elif isinstance(binlog_event, DeleteRowsEvent):
                logger.info(f"捕获DELETE事件 - 表: {binlog_event.table}, 事件位置: {stream.log_file}:{current_pos}")
                for row in binlog_event.rows:
                    club_data = row['values']
                    logger.info(f"删除数据: {club_data}")
                    delete_from_starrocks(club_data)

            # 保存当前binlog位置（断点续传）
            save_binlog_pos(stream.log_file, stream.log_pos)

    except Exception as e:
        logger.error(f"解析binlog异常：{e}")
        raise
    finally:
        stream.close()
        logger.info("关闭binlog连接")



def delete_from_starrocks(club_data):
    """从StarRocks删除数据"""
    if not club_data or 'id' not in club_data:
        logger.warning("删除数据中缺少id字段，跳过")
        return

    table_name = 'clubs_mix'
    club_id = club_data['id']

    try:
        conn = get_starrocks_conn()
        cursor = conn.cursor()

        delete_sql = f"DELETE FROM {table_name} WHERE id = %s"
        cursor.execute(delete_sql, (club_id,))
        conn.commit()
        logger.info(f"从StarRocks删除成功：id={club_id}")
    except Exception as e:
        conn.rollback()
        logger.error(f"从StarRocks删除失败：{e}，数据：{club_data}")
    finally:
        cursor.close()
        conn.close()

# ===================== 6. 主函数 =====================
if __name__ == "__main__":
    logger.info("===== 启动fa_clubs同步到StarRocks脚本 =====")
    try:
        # 先测试数据库连接（手动测试第一步：验证连接）
        get_mysql_conn().close()
        get_starrocks_conn().close()

        # 再启动binlog解析
        parse_binlog()
    except Exception as e:
        logger.error(f"脚本运行异常：{e}")
        raise
    logger.info("===== 脚本正常结束 =====")