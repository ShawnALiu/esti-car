import os
import json
import time
import threading
from datetime import datetime

from core import get_logger
from crawler import CRAWLER_DICT

logger = get_logger("schedule_workers")


class ScheduleWorker:
    _instance = None

    def __new__(cls, db_ins, executor_ins, image_queue_ins):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, db_ins, executor_ins, image_queue_ins):
        if self._initialized:
            return
        logger.info("ScheduleWorker 初始化")
        
        self._initialized = True
        self.db = db_ins
        self.executor = executor_ins
        self.image_queue = image_queue_ins

        self.start_scan()

    def start_scan(self):
        # 爬虫任务
        threading.Thread(target=self._scan_crawler_loop, daemon=True).start()
        # 图片下载任务
        threading.Thread(target=self._scan_image_loop, daemon=True).start()
        logger.info("定时任务扫描线程已启动")

    def _scan_crawler_loop(self):
        while True:
            try:
                self._scan_crawler_tasks()
                time.sleep(60)
            except Exception as e:
                logger.error(f"爬虫任务扫描异常: {type(e).__name__}: {e}", exc_info=True)

    def _scan_image_loop(self):
        while True:
            try:
                self._scan_image_download_tasks()
                time.sleep(60*10)
            except Exception as e:
                logger.error(f"图片任务处理异常: {e}", exc_info=True)

    def _scan_crawler_tasks(self):
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
                logger.info(f"触发定时任务, task_id={task_id}, interval={interval}")
                self.executor.execute_task(task_id)

    def _scan_image_download_tasks(self):
        sql = """
                    SELECT * FROM image_task 
                    WHERE status = 0 
                    ORDER BY id 
                    LIMIT :limit OFFSET :offset
                """
        offset = 0
        page_size = 5
        logger.info(f"【定时任务】开始扫描 image_task 表，每次处理 {page_size} 条...")
        while True:
            tasks = self.db.query(sql, {"limit": page_size, "offset": offset})
            if not tasks:
                logger.info("【定时任务】所有 status=0 的任务已处理完毕。")
                break
            # 查询图片
            for task in tasks:
                site_name = task.get("site_name")
                car_id = task.get("car_id")
                crawler = CRAWLER_DICT.get(site_name)
                if not crawler:
                    logger.error(f"【定时任务】未找到网站 [{site_name}] 对应的爬虫实例")
                    continue
                if not crawler.base_url:
                    logger.error(f"【定时任务】网站 [{site_name}] 未登录")
                    continue
                data = crawler.get_images(task)
                images = data['images']
                vin_str = data['vin_str']
                if images and len(images) > 4:
                    images = images[:4]
                image_task = {
                    'car_id': car_id,
                    'images': images,
                    'vin_str': vin_str
                }
                # 添加下载图片队列
                self.image_queue.add_task(image_task)
                # 避免限流
                time.sleep(1)

            offset += page_size
