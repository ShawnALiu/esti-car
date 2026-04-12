import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from core import config
from crawler import CRAWLER_DICT


class TaskExecutor:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, db):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self, db):
        if self._initialized:
            return
        self._initialized = True
        self.db = db
        self.task_pool = ThreadPoolExecutor(max_workers=5)
        self.image_pool = ThreadPoolExecutor(max_workers=10)
        self.active_tasks = {}
        self.last_error = None

    def execute_task(self, task_id):
        if self.db is None:
            return

        if task_id in self.active_tasks:
            return

        self.task_pool.submit(self._execute_task_internal, task_id)

    def _execute_task_internal(self, task_id):
        task = self.db.query_one("SELECT * FROM task WHERE id = :id", {"id": task_id})
        if not task:
            return

        account = self.db.query_one("SELECT * FROM account_config WHERE id = :id", {"id": task["account_id"]})
        if not account:
            return

        site_name = account.get("site_name", "")
        crawler = CRAWLER_DICT.get(site_name)
        if not crawler:
            self.last_error = f"未找到网站 [{site_name}] 对应的爬虫实例"
            return
        if not crawler.base_url:
            self.last_error = f"网站 [{site_name}] 未登录"
            return

        execution_id = self.db.insert("task_execution", {
            "task_id": task_id,
            "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "running"
        })

        self.active_tasks[task_id] = execution_id
        try:
            if task["task_type"] == "accident":
                cars = crawler.get_accident_cars(max_count=task["max_count"])
                if cars:
                    self.db.batch_upsert_sqlite("accident_car", cars)
                    self._download_images(cars)
            elif task["task_type"] == "used":
                cars = crawler.get_used_cars(max_count=task["max_count"])
                if cars:
                    self.db.batch_upsert_sqlite("used_car", cars)
                    self._download_images(cars)

            self.db.update("task_execution", {
                "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "status": "success",
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }, "id = :id", {"id": execution_id})
        except Exception as e:
            self.last_error = str(e)
            self.db.update("task_execution", {
                "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "status": "failed",
                "message": str(e),
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }, "id = :id", {"id": execution_id})
        finally:
            self.active_tasks.pop(task_id, None)

    def _download_images(self, cars):
        if not cars:
            return
        for car in cars:
            car_id = car.get("car_id")
            images = car.get("detail_urls")
            if not images or not car_id:
                continue
            self.image_pool.submit(self._download_single_car_images, car_id, images)

    def _download_single_car_images(self, car_id, images):
        import json
        try:
            images = json.loads(images)
        except:
            return

        # 做多保存4张
        images = images[:4]

        save_path = os.path.join(config.get_data_path(), "images", str(car_id))
        os.makedirs(save_path, exist_ok=True)

        for img in images:
            middle_file_id = img.get("middleFileid", "")
            image_id = img.get("imageId", "")
            if not middle_file_id or not image_id:
                continue

            file_path = os.path.join(save_path, image_id)
            if os.path.exists(file_path):
                continue

            try:
                import requests
                resp = requests.get(middle_file_id, timeout=3)
                if resp.status_code == 200:
                    with open(file_path, "wb") as f:
                        f.write(resp.content)
            except Exception as e:
                print(f"下载图片失败: {middle_file_id}, error: {e}")

    def is_task_running(self, task_id):
        return task_id in self.active_tasks

    def get_active_tasks(self):
        return list(self.active_tasks.keys())

    def get_last_error(self):
        return self.last_error

    def clear_last_error(self):
        self.last_error = None

    def shutdown(self):
        self.task_pool.shutdown(wait=True)
        self.image_pool.shutdown(wait=True)
