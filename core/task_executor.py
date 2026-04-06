import threading
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

from crawler import CRAWLER_DICT
from crawler.car_crawler import BaseCrawler
from crawler.boche_crawler import BoCheCrawler


class TaskExecutor:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self.db = None
        self.scheduler = BackgroundScheduler()
        self.active_tasks = {}
        self._running = False

    def set_database(self, db):
        self.db = db

    def start(self):
        if not self.scheduler.running:
            self.scheduler.start()
            self._running = True

    def stop(self):
        if self.scheduler.running:
            self.scheduler.shutdown()
            self._running = False

    def execute_task(self, task_id):
        if self.db is None:
            return

        task = self.db.query_one("SELECT * FROM task WHERE id = :id", {"id": task_id})
        if not task:
            return

        account = self.db.query_one("SELECT * FROM account_config WHERE id = :id", {"id": task["account_id"]})
        if not account:
            return

        execution_id = self.db.insert("task_execution", {
            "task_id": task_id,
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "running"
        })

        self.active_tasks[task_id] = execution_id
        try:
            site_name = account.get("site_name", "")
            crawler = CRAWLER_DICT.get(site_name)
            if not crawler:
                print(f"错误。未找到网站 [{site_name}] 对应的爬虫实例")
                return
            
            if task["task_type"] == "accident":
                cars = crawler.get_accident_cars(max_count=task["max_count"])
                if cars:
                    for car in cars:
                        car["task_id"] = task_id
                        car["execution_id"] = execution_id
                    self.db.batch_insert("accident_car", cars)
                    
            elif task["task_type"] == "used":
                cars = crawler.get_used_cars(max_count=task["max_count"])
                if cars:
                    for car in cars:
                        car["task_id"] = task_id
                        car["execution_id"] = execution_id
                    self.db.batch_insert("used_car", cars)

            self.db.update("task_execution", {
                "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "status": "success"
            }, "id = :id", {"id": execution_id})
        except Exception as e:
            self.db.update("task_execution", {
                "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "status": "failed",
                "message": str(e)
            }, "id = :id", {"id": execution_id})
        finally:
            self.active_tasks.pop(task_id, None)

    def enable_task(self, task_id):
        self.db.update("task", {"enabled": 1, "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}, "id = :id", {"id": task_id})
        task = self.db.query_one("SELECT * FROM task WHERE id = :id", {"id": task_id})
        if task and task.get("schedule_type") == "cron" and task.get("cron_expression"):
            self.scheduler.add_job(
                self.execute_task,
                "cron",
                args=[task_id],
                id=f"task_{task_id}",
                replace_existing=True,
                **self._parse_cron(task["cron_expression"])
            )
        self.start()

    def disable_task(self, task_id):
        self.db.update("task", {"enabled": 0, "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}, "id = :id", {"id": task_id})
        job_id = f"task_{task_id}"
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)
        self.active_tasks.pop(task_id, None)

    def is_task_running(self, task_id):
        return task_id in self.active_tasks

    def get_active_tasks(self):
        return list(self.active_tasks.keys())

    def _parse_cron(self, cron_expr):
        parts = cron_expr.split()
        if len(parts) == 5:
            return {
                "minute": parts[0],
                "hour": parts[1],
                "day": parts[2],
                "month": parts[3],
                "day_of_week": parts[4]
            }
        return {"minute": "0", "hour": "0"}
