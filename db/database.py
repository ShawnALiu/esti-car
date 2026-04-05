import sqlite3
import os


class Database:
    def __init__(self, db_path=None):
        if db_path is None:
            app_data = os.path.join(os.path.expanduser("~"), ".esticar")
            os.makedirs(app_data, exist_ok=True)
            db_path = os.path.join(app_data, "esticar.db")
        self.db_path = db_path
        self.init_db()

    def get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        conn = self.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS account_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                site_name TEXT NOT NULL,
                site_url TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
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
        """)

        cursor.execute("""
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
        """)

        cursor.execute("""
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
        """)

        cursor.execute("""
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
        """)

        conn.commit()
        self._migrate(conn)
        conn.close()

    def _migrate(self, conn):
        cursor = conn.cursor()
        migrations = {
            "task_execution": ["created_at"],
            "account_config": ["created_at", "updated_at"],
            "task": ["created_at", "updated_at"],
            "accident_car": [],
            "used_car": [],
        }
        for table, columns in migrations.items():
            cursor.execute(f"PRAGMA table_info({table})")
            existing = [row[1] for row in cursor.fetchall()]
            for col in columns:
                if col not in existing:
                    cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT DEFAULT CURRENT_TIMESTAMP")
        conn.commit()

    def execute(self, sql, params=None):
        conn = self.get_conn()
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        conn.commit()
        last_id = cursor.lastrowid
        conn.close()
        return last_id

    def query(self, sql, params=None):
        conn = self.get_conn()
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return rows

    def query_one(self, sql, params=None):
        conn = self.get_conn()
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        row = cursor.fetchone()
        result = dict(row) if row else None
        conn.close()
        return result

    def delete(self, table, condition, params=None):
        sql = f"DELETE FROM {table} WHERE {condition}"
        self.execute(sql, params)

    def update(self, table, data, condition, params=None):
        set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
        sql = f"UPDATE {table} SET {set_clause} WHERE {condition}"
        all_params = list(data.values())
        if params:
            all_params.extend(params)
        self.execute(sql, all_params)

    def insert(self, table, data):
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?" for _ in data])
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        return self.execute(sql, list(data.values()))

    def batch_insert(self, table, data_list):
        if not data_list:
            return 0
        conn = self.get_conn()
        cursor = conn.cursor()
        
        columns = list(data_list[0].keys())
        col_str = ", ".join(columns)
        placeholders = ", ".join(["?" for _ in columns])
        sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"
        
        for data in data_list:
            cursor.execute(sql, [data.get(col, "") for col in columns])
        
        conn.commit()
        count = cursor.rowcount
        conn.close()
        return count

    def count(self, table, condition=None, params=None):
        sql = f"SELECT COUNT(*) as cnt FROM {table}"
        if condition:
            sql += f" WHERE {condition}"
        result = self.query_one(sql, params)
        return result["cnt"] if result else 0

    def cleanup_data(self, table, before_date):
        self.delete(table, "created_at < ?", (before_date,))
