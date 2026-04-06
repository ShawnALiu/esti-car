import copy
import time

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.pool import QueuePool
import os
import threading
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

# --- 1. DDL 定义 (保持原样) ---
account_config_ddl = """
CREATE TABLE account_config (
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
CREATE TABLE task (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    task_type TEXT NOT NULL,
    account_id INTEGER,
    account_site_name TEXT,
    max_count INTEGER DEFAULT 1000,
    enabled INTEGER DEFAULT 0,
    schedule_type TEXT DEFAULT 'manual',
    cron_expression TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""

task_execution_ddl = """
CREATE TABLE task_execution (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER,
    start_time TEXT,
    end_time TEXT,
    status TEXT DEFAULT 'running',
    message TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""

accident_car_ddl = """
CREATE TABLE accident_car (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site_name TEXT,
    detail_urls TEXT,
    car_id TEXT,
    che_liang_pin_pai TEXT,
    xuan_ze_xi_lie TEXT,
    xuan_ze_zi_xi_lie TEXT,
    pai_liang TEXT,
    chu_chang_ri_qi INTEGER,
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
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
)
"""

used_car_ddl = """
CREATE TABLE used_car (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site_name TEXT,
    detail_urls TEXT,
    car_id TEXT,
    che_liang_pin_pai TEXT,
    xuan_ze_xi_lie TEXT,
    xuan_ze_zi_xi_lie TEXT,
    pai_liang TEXT,
    chu_chang_ri_qi INTEGER,
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
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
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
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            data_dir = os.path.join(project_root, "data", "db")
            os.makedirs(data_dir, exist_ok=True)
            db_path = os.path.join(data_dir, "esticar.db")

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


# --- 3. 业务数据库操作类 (接口丰富) ---
class Database:
    def __init__(self, db_path: Optional[str] = None):
        self.pool = DatabasePool(db_path)
        self.engine = self.pool.engine
        self.init_tables()
        self._migrate()

    def _migrate(self):
        with self.engine.connect() as conn:
            task_columns = [row[1] for row in conn.execute(text("PRAGMA table_info(task)")).fetchall()]
            
            need_rebuild = "account_site_name" not in task_columns
            
            if need_rebuild:
                temp_tables = {
                    "account_config_temp": ["id", "site_name", "site_url", "username", "password", "created_at", "updated_at"],
                    "task_temp": ["id", "name", "task_type", "account_id", "account_site_name", "max_count", "enabled", "schedule_type", "cron_expression", "created_at", "updated_at"],
                    "task_execution_temp": ["id", "task_id", "start_time", "end_time", "status", "message", "created_at"],
                    "accident_car_temp": ["id", "site_name", "detail_urls", "car_id", "che_liang_pin_pai", "xuan_ze_xi_lie", "xuan_ze_zi_xi_lie", "pai_liang", "chu_chang_ri_qi", "che_pai_hao", "che_liang_zan_cun_di", "is_auction_finish", "yu_zhan_shi_jian", "attention", "chesunyuanyin", "pai_mai_hui_start_time", "pai_mai_hui_lei_xing", "zui_xin_chu_jia", "yi_kou_jia", "gu_jia_ping_ji", "wai_guan_ping_ji", "main_car", "is_new_chu_jia", "is_yi_kou_jia", "is_xian_pai", "wu_zi_ming_cheng", "che_liang_zhong_lei", "pei_jian_zhong_lei", "pai_mai_jie_shu_date", "vehicle_name", "is_xin_neng_yuan", "biao_di_type", "image_url", "pai_pin_count", "wei_guan_count", "bid_count", "created_at"],
                    "used_car_temp": ["id", "site_name", "detail_urls", "car_id", "che_liang_pin_pai", "xuan_ze_xi_lie", "xuan_ze_zi_xi_lie", "pai_liang", "chu_chang_ri_qi", "che_pai_hao", "che_liang_zan_cun_di", "is_auction_finish", "yu_zhan_shi_jian", "attention", "chesunyuanyin", "pai_mai_hui_start_time", "pai_mai_hui_lei_xing", "zui_xin_chu_jia", "yi_kou_jia", "gu_jia_ping_ji", "wai_guan_ping_ji", "main_car", "is_new_chu_jia", "is_yi_kou_jia", "is_xian_pai", "wu_zi_ming_cheng", "che_liang_zhong_lei", "pei_jian_zhong_lei", "pai_mai_jie_shu_date", "vehicle_name", "is_xin_neng_yuan", "biao_di_type", "image_url", "pai_pin_count", "wei_guan_count", "bid_count", "created_at"],
                }
                
                for old_table, columns in temp_tables.items():
                    try:
                        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {old_table} ({', '.join(columns)})"))
                    except:
                        pass
                
                for old_table, new_table in [("account_config", "account_config_temp"), ("task", "task_temp"), ("task_execution", "task_execution_temp"), ("accident_car", "accident_car_temp"), ("used_car", "used_car_temp")]:
                    try:
                        conn.execute(text(f"INSERT INTO {new_table} SELECT * FROM {old_table}"))
                    except:
                        pass
                
                for table in ["accident_car", "used_car", "task_execution", "task", "account_config"]:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
                
                conn.execute(text(account_config_ddl))
                conn.execute(text(task_ddl))
                conn.execute(text(task_execution_ddl))
                conn.execute(text(accident_car_ddl))
                conn.execute(text(used_car_ddl))
                
                for old_table, new_table in [("account_config_temp", "account_config"), ("task_temp", "task"), ("task_execution_temp", "task_execution"), ("accident_car_temp", "accident_car"), ("used_car_temp", "used_car")]:
                    try:
                        conn.execute(text(f"INSERT INTO {new_table} SELECT * FROM {old_table}"))
                        conn.execute(text(f"DROP TABLE {old_table}"))
                    except:
                        pass
            
            conn.commit()

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

    def batch_insert(self, table: str, data_list: List[Dict[str, Any]]) -> int:
        if not data_list:
            return 0

        # 确保所有字典结构一致
        columns = data_list[0].keys()
        col_str = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])
        sql = text(f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})")

        try:
            with self.engine.connect() as conn:
                conn.execute(sql, data_list)
                conn.commit()
                return len(data_list)
        except Exception as e:
            print(f"批量插入失败: {e}")
            return 0

    def batch_upsert_sqlite(self, table_name: str, data_list: List[Dict[str, Any]]) -> int:
        if not data_list:
            return 0

        # 1. 基础检查
        if "car_id" not in data_list[0]:
            return self._batch_insert_normal(table_name, data_list)

        # 提取所有待处理的 car_id
        target_car_ids = [d['car_id'] for d in data_list]

        with self.engine.connect() as conn:
            # 2. 【关键优化】一次性查出数据库中已存在的 car_id
            # 使用参数化查询防止注入，且利用索引快速查找
            placeholders = ",".join([f":cid{i}" for i in range(len(target_car_ids))])
            params = {f"cid{i}": cid for i, cid in enumerate(target_car_ids)}

            # 假设数据库表中有 'id' (主键) 和 'car_id' (唯一索引)
            query = text(f"SELECT id, car_id FROM {table_name} WHERE car_id IN ({placeholders})")
            result = conn.execute(query, params).fetchall()

            # 构建映射: car_id -> db_id
            existing_map = {row.car_id: row.id for row in result}

            insert_data = []
            update_data = []

            # 3. 在内存中将数据分为“需插入”和“需更新”两类
            for data in data_list:
                cid = data.get('car_id')
                if cid in existing_map:
                    # --- 需更新 ---
                    update_item = data.copy()
                    update_item['db_id'] = existing_map[cid]  # 映射主键
                    update_item['updated_at'] = time.strftime("%Y-%m-%d %H:%M:%S")
                    update_data.append(update_item)
                else:
                    # --- 需插入 ---
                    insert_data.append(data)

            # 4. 批量执行更新 (如果存在数据)
            if update_data:
                # 动态构建 SET 子句，排除 car_id (作为条件) 和 db_id (主键)
                # 注意：这里假设所有数据的字段结构一致，取第一条做模板
                columns_to_update = [k for k in update_data[0].keys() if k not in ['car_id', 'db_id']]

                set_clause = ", ".join([f"{k} = :{k}" for k in columns_to_update])
                update_sql = text(f"UPDATE {table_name} SET {set_clause} WHERE id = :db_id")

                # 批量执行
                conn.execute(update_sql, update_data)

            # 5. 批量执行插入 (如果存在新数据)
            if insert_data:
                # 获取列名
                columns = list(insert_data[0].keys())
                col_str = ", ".join(columns)
                # 这里的 :col 会自动匹配字典中的键
                placeholders = ", ".join([f":{col}" for col in columns])

                insert_sql = text(f"INSERT INTO {table_name} ({col_str}) VALUES ({placeholders})")
                conn.execute(insert_sql, insert_data)

            # 6. 提交事务
            conn.commit()

        return len(data_list)

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

    def init_tables(self):
        ddl_list = [
            account_config_ddl, task_ddl, task_execution_ddl,
            accident_car_ddl, used_car_ddl
        ]

        with self.engine.connect() as conn:
            for ddl in ddl_list:
                if_exist_ddl = copy.deepcopy(ddl)
                if_exist_ddl = if_exist_ddl.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
                conn.execute(text(if_exist_ddl))
            conn.commit()

    # --- 9. 清空单个表并重建 ---
    def rebuild_table(self, table_name: str):
        ddl_map = {
            "account_config": account_config_ddl,
            "task": task_ddl,
            "task_execution": task_execution_ddl,
            "accident_car": accident_car_ddl,
            "used_car": used_car_ddl
        }
        ddl = ddl_map.get(table_name)
        if ddl:
            with self.engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                conn.execute(text(ddl))
                conn.commit()

    # --- 10. 清空所有表并重建 ---
    def rebuild_all_table(self):
        tables = ["accident_car", "used_car", "task_execution", "task", "account_config"]
        for table in tables:
            self.rebuild_table(table)




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
    _result = db.query_one("SELECT * FROM account_config WHERE id = :id", {"id": account_id})
    print(f"查询结果: {_result}")
