import sys
import os

from core.config import GLOBAL_APP_NAME, GLOBAL_APP_VERSION
from core.image_task_queue import ImageQueue

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core import setup_logging
from PyQt5.QtWidgets import QApplication
from PyQt5.QtGui import QIcon
from db.database import Database
from core.task_executor import TaskExecutor
from core.schedule_workers import ScheduleWorker
from ui.main_window import MainWindow


def main():
    setup_logging()
    
    app = QApplication(sys.argv)
    app.setApplicationName(GLOBAL_APP_NAME)
    app.setApplicationVersion(GLOBAL_APP_VERSION)

    icon_path = os.path.join("data", "icon", "AE86.webp")
    app.setWindowIcon(QIcon(icon_path))

    db = Database()
    executor = TaskExecutor(db)
    image_queue = ImageQueue(db)
    schedule_worker = ScheduleWorker(db, executor, image_queue)

    window = MainWindow(db, executor)
    window.show()

    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
