import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import requests

from core import config, get_logger
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
        self.image_pool = ThreadPoolExecutor(max_workers=20)
        self.active_tasks = {}
        self.last_error = None


        self.download_session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=1)
        self.download_session.mount('http://', adapter)
        self.download_session.mount('https://', adapter)


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
            logger.info(f"任务执行结束, task_id={task_id}")

    def _download_images(self, cars):
        if not cars:
            return

        logger.info(f"开始准备下载图片，共 {len(cars)} 辆车")
        futures = []

        for car in cars:
            car_id = car.get("car_id")
            images_json = car.get("detail_urls")
            if not images_json or not car_id:
                logger.error(f"非法参数。car_id={car_id}, images_json={images_json}")
                continue
            try:
                images = json.loads(images_json)
            except:
                logger.error(f"images_json反序列化失败。car_id={car_id}, images_json={images_json}")
                continue

            # 最多保存4张
            images = images[:4]
            save_path = os.path.join(config.get_data_path(), "images", str(car_id))
            os.makedirs(save_path, exist_ok=True)

            for img in images:
                # 提交任务并保存 future 对象
                future = self.image_pool.submit(self._download_single_image, save_path, img)
                futures.append(future)

        # 【关键修复】等待所有下载任务完成
        # 如果不加这个，主程序会在下载完成前就结束
        logger.info(f"已提交 {len(futures)} 个下载任务，正在等待完成...")
        from concurrent.futures import as_completed
        for _ in as_completed(futures):
            pass  # 只要不报错就行，具体的错误在 _download_single_image 里处理了

        logger.info("所有图片下载任务处理完毕。")

    def _download_single_image(self, save_path, img):
        # 这里的 logger 最好用全局的或者传入的，防止多线程下 logger 为 None
        global logger
        if logger is None:
            from core.logger import get_logger
            logger = get_logger("task_executor")

        middle_file_id = img.get("middleFileid", "")
        image_id = img.get("imageId", "")

        if not middle_file_id or not image_id:
            return

        file_path = os.path.join(save_path, image_id)

        # 双重检查锁（虽然这里是单线程提交，但在多线程执行时很有用）
        if os.path.exists(file_path):
            return

        max_retries = 3
        temp_file_path = file_path + ".tmp"

        # 可以在这里打印一条调试日志，确认任务真的进来了
        # logger.debug(f"开始下载: {image_id}")

        for attempt in range(max_retries):
            try:
                with self.download_session.get(middle_file_id, timeout=10, stream=True) as resp:
                    if resp.status_code == 200:
                        with open(temp_file_path, "wb") as f:
                            for chunk in resp.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                        os.replace(temp_file_path, file_path)
                        return
                    elif resp.status_code == 404:
                        logger.warning(f"资源不存在 (404): {image_id}")
                        return
                    else:
                        logger.warning(
                            f"下载失败 (状态码 {resp.status_code}): {image_id}, 尝试 {attempt + 1}/{max_retries}")
            except Exception as e:
                # 记录具体的异常类型，方便调试
                logger.warning(f"下载异常: {image_id}, 错误: {type(e).__name__}: {e}, 尝试 {attempt + 1}/{max_retries}")

            # 退避策略：重试等待时间递增
            time.sleep(1 * (attempt + 1))

        # 最终失败
        if os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
            except:
                pass
        logger.error(f"最终下载失败: {image_id} (URL: {middle_file_id})")


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
