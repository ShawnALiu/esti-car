from sqlalchemy import create_engine, text, inspect
from sqlalchemy.pool import QueuePool
import os
import threading
from typing import List, Dict, Any, Optional, Tuple

# --- 1. DDL 定义 (保持原样) ---
account_config_ddl = """
CREATE TABLE IF NOT EXISTS account_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site_name TEXT NOT NULL,
    site_url TEXT NOT NULL,
    username TEXT NOT NULL,
    password TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""

task_ddl = """
CREATE TABLE IF NOT EXISTS task (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    task_type TEXT NOT NULL,
    account_id INTEGER NOT NULL,
    auction_time TEXT,
    max_count INTEGER DEFAULT 100,
    enabled INTEGER DEFAULT 0,
    schedule_type TEXT DEFAULT 'manual',
    cron_expression TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (account_id) REFERENCES account_config(id)
)
"""

task_execution_ddl = """
CREATE TABLE IF NOT EXISTS task_execution (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL,
    start_time TEXT,
    end_time TEXT,
    status TEXT DEFAULT 'running',
    message TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (task_id) REFERENCES task(id)
)
"""

accident_car_ddl = """
CREATE TABLE IF NOT EXISTS accident_car (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER,
    execution_id INTEGER,
    pai_mai_id TEXT,
    site_name TEXT,
    detail_urls TEXT,
    car_id TEXT,
    che_liang_pin_pai TEXT,
    xuan_ze_xi_lie TEXT,
    xuan_ze_zi_xi_lie TEXT,
    pai_liang TEXT,
    chu_chang_ri_qi TEXT,
    che_pai_hao TEXT,
    che_liang_zan_cun_di TEXT,
    is_auction_finish INTEGER,
    yu_zhan_shi_jian TEXT,
    attention TEXT,
    chesunyuanyin TEXT,
    pai_mai_hui_start_time TEXT,
    pai_mai_hui_lei_xing INTEGER,
    zui_xin_chu_jia TEXT,
    yi_kou_jia REAL,
    gu_jia_ping_ji TEXT,
    wai_guan_ping_ji TEXT,
    main_car INTEGER,
    is_new_chu_jia INTEGER,
    is_yi_kou_jia INTEGER,
    is_xian_pai INTEGER,
    wu_zi_ming_cheng TEXT,
    che_liang_zhong_lei TEXT,
    pei_jian_zhong_lei TEXT,
    pai_mai_jie_shu_date TEXT,
    vehicle_name TEXT,
    is_xin_neng_yuan INTEGER,
    biao_di_type INTEGER,
    image_url TEXT,
    pai_pin_count INTEGER,
    wei_guan_count INTEGER,
    bid_count INTEGER,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (task_id) REFERENCES task(id),
    FOREIGN KEY (execution_id) REFERENCES task_execution(id)
)
"""

used_car_ddl = """
CREATE TABLE IF NOT EXISTS used_car (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER,
    execution_id INTEGER,
    pai_mai_id TEXT,
    site_name TEXT,
    detail_urls TEXT,
    car_id TEXT,
    che_liang_pin_pai TEXT,
    xuan_ze_xi_lie TEXT,
    xuan_ze_zi_xi_lie TEXT,
    pai_liang TEXT,
    chu_chang_ri_qi TEXT,
    che_pai_hao TEXT,
    che_liang_zan_cun_di TEXT,
    is_auction_finish INTEGER,
    yu_zhan_shi_jian TEXT,
    attention TEXT,
    chesunyuanyin TEXT,
    pai_mai_hui_start_time TEXT,
    pai_mai_hui_lei_xing INTEGER,
    zui_xin_chu_jia TEXT,
    yi_kou_jia REAL,
    gu_jia_ping_ji TEXT,
    wai_guan_ping_ji TEXT,
    main_car INTEGER,
    is_new_chu_jia INTEGER,
    is_yi_kou_jia INTEGER,
    is_xian_pai INTEGER,
    wu_zi_ming_cheng TEXT,
    che_liang_zhong_lei TEXT,
    pei_jian_zhong_lei TEXT,
    pai_mai_jie_shu_date TEXT,
    vehicle_name TEXT,
    is_xin_neng_yuan INTEGER,
    biao_di_type INTEGER,
    image_url TEXT,
    pai_pin_count INTEGER,
    wei_guan_count INTEGER,
    bid_count INTEGER,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (task_id) REFERENCES task(id),
    FOREIGN KEY (execution_id) REFERENCES task_execution(id)
)
"""


# --- 2. 连接池管理器 (单例模式) ---
class DatabasePool:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, db_path: str = None):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, db_path: str = None):
        if hasattr(self, 'engine'):  # 防止重复初始化
            return

        # 构建数据库路径
        if db_path is None:
            app_data = os.path.join(os.path.expanduser("~"), ".esticar")
            os.makedirs(app_data, exist_ok=True)
            db_path = os.path.join(app_data, "esticar.db")

        # --- 核心配置：连接池 ---
        # pool_size: 连接池中保持的常驻连接数
        # max_overflow: 最多允许超出多少个连接
        # pool_recycle: 每隔多少秒重建连接 (防止长时间空闲导致的数据库断连)
        # pool_pre_ping: 每次取连接前发一个轻量级 ping，确保连接有效 (解决 "Database is locked" 的神器)
        self.engine = create_engine(
            f"sqlite:///{db_path}?check_same_thread=False",  # SQLite 默认只允许创建线程访问，这里强制允许
            poolclass=QueuePool,
            pool_size=10,  # 常驻 10 个连接
            max_overflow=20,  # 最多溢出 20 个
            pool_recycle=3600,  # 每小时重建一次连接，防止老化
            pool_pre_ping=True,  # 每次用前检查
            echo=False  # 调试时可设为 True 查看 SQL
        )

        # 初始化表结构
        self._init_tables()

    def _init_tables(self):
        """使用连接池连接来执行建表语句"""
        ddl_list = [
            account_config_ddl, task_ddl, task_execution_ddl,
            accident_car_ddl, used_car_ddl
        ]

        with self.engine.connect() as conn:
            for ddl in ddl_list:
                conn.execute(text(ddl))
            conn.commit()


# --- 3. 业务数据库操作类 (接口丰富) ---
class Database:
    def __init__(self, db_path: Optional[str] = None):
        self.pool = DatabasePool(db_path)
        self.engine = self.pool.engine

    # --- 工具方法：将字典转为 SQL 参数 ---
    def _dict_to_params(self, data: Dict[str, Any]):
        return {f"val_{k}": v for k, v in data.items()}

    # --- 工具方法：将 Row 转为 Dict ---
    def _row_to_dict(self, row):
        if row is None:
            return None
        return dict(row._mapping)

    # --- 1. 插入单条 ---
    def insert(self, table: str, data: Dict[str, Any]) -> int:
        if not data:
            return 0

        columns = ", ".join(data.keys())
        placeholders = ", ".join([f":val_{k}" for k in data.keys()])
        sql = text(f"INSERT INTO {table} ({columns}) VALUES ({placeholders})")

        with self.engine.connect() as conn:
            result = conn.execute(sql, self._dict_to_params(data))
            conn.commit()
            return result.lastrowid

    # --- 2. 批量插入 (高性能) ---
    def batch_insert(self, table: str, data_list: List[Dict[str, Any]]) -> int:
        if not data_list:
            return 0

        # 确保所有字典结构一致
        columns = data_list[0].keys()
        col_str = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])  # SQLAlchemy 支持直接用 key 名
        sql = text(f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})")

        try:
            with self.engine.connect() as conn:
                # 直接传入字典列表，SQLAlchemy 会自动处理批量插入
                conn.execute(sql, data_list)
                conn.commit()
                return len(data_list)
        except Exception as e:
            print(f"批量插入失败: {e}")
            return 0

    # --- 3. 更新 ---
    def update(self, table: str, data: Dict[str, Any], condition: str, params: Optional[Dict] = None) -> bool:
        if not data:
            return False

        set_clause = ", ".join([f"{k} = :val_{k}" for k in data.keys()])
        sql_text = f"UPDATE {table} SET {set_clause} WHERE {condition}"
        sql = text(sql_text)

        # 合并参数
        bound_params = self._dict_to_params(data)
        if params:
            bound_params.update(params)

        with self.engine.connect() as conn:
            result = conn.execute(sql, bound_params)
            conn.commit()
            return result.rowcount > 0

    # --- 4. 删除 ---
    def delete(self, table: str, condition: str, params: Optional[Dict] = None) -> bool:
        sql = text(f"DELETE FROM {table} WHERE {condition}")
        with self.engine.connect() as conn:
            result = conn.execute(sql, params or {})
            conn.commit()
            return result.rowcount > 0

    # --- 5. 查询多行 ---
    def query(self, sql_str: str, params: Optional[Dict] = None) -> List[Dict]:
        sql = text(sql_str)
        with self.engine.connect() as conn:
            result = conn.execute(sql, params or {})
            return [self._row_to_dict(row) for row in result.fetchall()]

    # --- 6. 查询单行 ---
    def query_one(self, sql_str: str, params: Optional[Dict] = None) -> Optional[Dict]:
        sql = text(sql_str)
        with self.engine.connect() as conn:
            result = conn.execute(sql, params or {})
            row = result.fetchone()
            return self._row_to_dict(row)

    # --- 7. 统计 ---
    def count(self, table: str, condition: Optional[str] = None, params: Optional[Dict] = None) -> int:
        sql_str = f"SELECT COUNT(*) as cnt FROM {table}"
        if condition:
            sql_str += f" WHERE {condition}"
        result = self.query_one(sql_str, params)
        return result["cnt"] if result else 0

    # --- 8. 原生执行 (复杂 SQL 用) ---
    def execute_raw(self, sql_str: str, params: Optional[Dict] = None) -> Any:
        """用于执行存储过程或复杂语句"""
        sql = text(sql_str)
        with self.engine.connect() as conn:
            result = conn.execute(sql, params or {})
            conn.commit()
            return result


# --- 4. 使用示例 ---
if __name__ == "__main__":
    # 初始化数据库 (自动创建连接池)
    db = Database()

    # 插入测试数据
    account_id = db.insert("account_config", {
        "site_name": "Test Site",
        "site_url": "http://test.com",
        "username": "user",
        "password": "pass"
    })
    print(f"插入 ID: {account_id}")

    # 查询测试
    result = db.query_one("SELECT * FROM account_config WHERE id = :id", {"id": account_id})
    print(f"查询结果: {result}")