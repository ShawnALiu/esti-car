import threading

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from core import get_logger
from crawler import CRAWLER_DICT

logger = get_logger("task_executor")


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
        self.active_tasks = {}
        self.last_error = None


    def execute_task(self, task_id):
        if self.db is None:
            return

        if task_id in self.active_tasks:
            return

        self.task_pool.submit(self._execute_task_internal, task_id)

    def _execute_task_internal(self, task_id):
        logger.info(f"开始执行任务, task_id={task_id}")
        
        task = self.db.query_one("SELECT * FROM task WHERE id = :id", {"id": task_id})
        if not task:
            logger.warning(f"任务不存在, task_id={task_id}")
            return

        account = self.db.query_one("SELECT * FROM account_config WHERE id = :id", {"id": task["account_id"]})
        if not account:
            logger.warning(f"账号配置不存在, task_id={task_id}")
            return

        site_name = account.get("site_name", "")
        crawler = CRAWLER_DICT.get(site_name)
        if not crawler:
            self.last_error = f"未找到网站 [{site_name}] 对应的爬虫实例"
            logger.error(self.last_error)
            return
        if not crawler.base_url:
            self.last_error = f"网站 [{site_name}] 未登录"
            logger.error(self.last_error)
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
                    self._add_image_tasks(cars)
            elif task["task_type"] == "used":
                cars = crawler.get_used_cars(max_count=task["max_count"])
                if cars:
                    self.db.batch_upsert_sqlite("used_car", cars)
                    self._add_image_tasks(cars)

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
            logger.info(f"任务执行结束, task_id={task_id}")

    def _add_image_tasks(self, cars):
        if not cars:
            return
        
        logger.info(f"添加图片任务，共 {len(cars)} 辆车")

        image_task_list = []
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for car in cars:
            site_name = car.get("site_name")
            pai_mai_id = car.get("pai_mai_id", "")
            car_id = car.get("car_id")
            if not site_name or not pai_mai_id or not car_id:
                continue
            image_task_list.append({
                "site_name": site_name,
                "pai_mai_id": pai_mai_id,
                "car_id": car_id,
                "status": 0,
                "created_at": now,
                "updated_at": now
            })
        
        if image_task_list:
            try:
                self.db.batch_insert_or_ignore("image_task", image_task_list)
                logger.info(f"图片任务插入完成，共 {len(image_task_list)} 条")
            except Exception as e:
                logger.error(f"批量插入图片任务失败: {e}", exc_info=True)

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
