import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from queue import Queue
from core import config
from core.logger import get_logger

logger = get_logger("task_queue")


class ImageQueue:
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
        self.queue = Queue()
        self.pool = ThreadPoolExecutor(max_workers=10)
        self.running = True

        self.download_session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=1)
        self.download_session.mount('http://', adapter)
        self.download_session.mount('https://', adapter)
        
        self.start_worker()
        logger.info("ImageQueue 初始化完成")

    def add_task(self, image_task):
        try:
            self.queue.put(image_task)
            logger.info(f"【图片队列】添加图片下载任务: image_task={image_task}")
        except Exception as e:
            logger.error(f"【图片队列】添加图片任务失败: {e}", exc_info=True)

    def start_worker(self):
        threading.Thread(target=self._worker_loop, daemon=True).start()
        logger.info("图片下载工作线程已启动")

    def _worker_loop(self):
        while self.running:
            try:
                self._process_queue()
            except Exception as e:
                logger.error(f"图片任务处理异常: {e}", exc_info=True)

    def _process_queue(self):
        task = self.queue.get()
        self._download_images(task['car_id'], task['images'], task['vin_str'])

    def _download_images(self, car_id, images, vin_str):
        data_path = config.get_data_path()
        image_dir = os.path.join(data_path, "images", str(car_id))
        os.makedirs(image_dir, exist_ok=True)
        
        try:
            futures = []
            for img in images:
                future = self.pool.submit(self._download_single_image, image_dir, img)
                futures.append(future)

            for _ in as_completed(futures):
                pass

            if car_id:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                update_list = [
                    {
                        "car_id": cid,
                        "vin_str": vin_str[i],
                        "status": 1,
                        "updated_at": now
                    } for i, cid in enumerate(car_id)
                ]
                self.db.batch_update("image_task", update_list)
            return
        except Exception as e:
            logger.error(f"图片下载异常: car_ids={car_id}, {e}", exc_info=True)

    def _download_single_image(self, save_path, img):
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

    def shutdown(self):
        self.running = False
        self.pool.shutdown(wait=True)
        logger.info("ImageQueue 已关闭")