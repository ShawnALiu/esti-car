import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

_loggers = {}

def get_logger(name: str = None):
    global _loggers
    
    if name is None:
        name = "esti-car"
    
    if name in _loggers:
        return _loggers[name]
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    logs_dir = os.path.join(project_root, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    log_file = os.path.join(logs_dir, f"{name}.log")
    
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler = TimedRotatingFileHandler(
        log_file,
        when='h',
        interval=1,
        backupCount=720,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    _loggers[name] = logger
    return logger


def setup_logging():
    import core.task_executor
    import core.schedule_workers
    import core.config
    import db.database
    import ui.main_window
    
    core.task_executor.logger = get_logger("task_executor")
    core.schedule_workers.logger = get_logger("schedule_workers")
    core.config.logger = get_logger("config")
    db.database.logger = get_logger("database")
    ui.main_window.logger = get_logger("ui")
    
    logger = get_logger()
    logger.info("=" * 50)
    logger.info("日志系统初始化完成")
    logger.info("=" * 50)


class LogCapture:
    def __init__(self, logger_name=None):
        self.logger = get_logger(logger_name)
        self.records = []
        
    def __enter__(self):
        self._old_handlers = self.logger.handlers[:]
        self.logger.handlers = []
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        for handler in self._old_handlers:
            self.logger.handlers.append(handler)
        
        if exc_type is not None:
            self.logger.error(f"异常: {exc_type.__name__}: {exc_val}", exc_info=(exc_type, exc_val, exc_tb))