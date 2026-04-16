import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

_loggers = {}
_log_handler = None

def get_logger(name: str = None):
    global _loggers
    
    if name is None:
        name = "esti-car"
    
    if name in _loggers:
        return _loggers[name]
    
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        from core.config import get_data_path
        data_path = get_data_path()
        logs_dir = os.path.join(data_path, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        log_file = os.path.join(logs_dir, f"esti-car.log")
        
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
        
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)
        root_logger.setLevel(logging.DEBUG)
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = True
    
    _loggers[name] = logger
    return logger


def setup_logging():
    logger = get_logger()
    logger.info("=" * 50)
    logger.info("日志系统初始化完成")
    logger.info("=" * 50)


class LogHandler(logging.Handler):
    def __init__(self, callback=None):
        super().__init__()
        self.callback = callback
        self._buffer = []

    def emit(self, record):
        try:
            msg = self.format(record)
            if self.callback:
                self.callback(msg)
            else:
                self._buffer.append(msg)
        except:
            pass
    
    def flush(self):
        self._buffer.clear()


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
