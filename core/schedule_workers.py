import os
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from core.config import get_data_path


class ScheduleWorker:
    _instance = None

    def __new__(cls, db_instance, executor_instance):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, db_instance, executor_instance):
        if self._initialized:
            return
        self._initialized = True
        self.db = db_instance
        self.executor = executor_instance

        self.start_scan()

    def start_scan(self):
        threading.Thread(target=self._scan_loop, daemon=True).start()

    def _scan_loop(self):
        while True:
            try:
                self.scan_tasks()
                time.sleep(60)
            except Exception as e:
                print('任务扫描出现错误', e)

    def scan_tasks(self):
        if not self.db or not self.executor:
            return

        tasks = self.db.query("SELECT * FROM task WHERE schedule_type = 'cron' AND enabled = 1")
        current_minute = datetime.now().minute

        for task in tasks:
            task_id = task["id"]
            interval = int(task.get("cron_expression", 3))

            if current_minute % interval != 0:
                continue

            if task_id not in self.executor.get_active_tasks():
                self.executor.execute_task(task_id)

    def shutdown(self):
        if self.pool:
            self.pool.shutdown(wait=True)