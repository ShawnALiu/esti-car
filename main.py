import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from PyQt5.QtWidgets import QApplication
from PyQt5.QtGui import QIcon
from db.database import Database
from core.task_executor import TaskExecutor
from core.schedule_workers import ScheduleWorker
from ui.main_window import MainWindow


def main():
    app = QApplication(sys.argv)
    app.setApplicationName("估车侠 EstiCar")
    app.setApplicationVersion("1.0.0")

    icon_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "icon", "AE86.webp")
    app.setWindowIcon(QIcon(icon_path))

    db = Database()
    executor = TaskExecutor(db)

    schedule_worker = ScheduleWorker(db, executor)

    window = MainWindow(db, executor)
    window.show()

    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
