import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from PyQt5.QtWidgets import (QMainWindow, QTabWidget, QVBoxLayout, QWidget,
                              QLabel, QApplication)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QFont
from ui.tab_valuation import ValuationTab
from ui.tab_account import AccountTab
from ui.tab_task import TaskTab
from ui.tab_data import DataTab


class MainWindow(QMainWindow):
    def __init__(self, db, executor):
        super().__init__()
        self.db = db
        self.executor = executor
        self.executor.set_database(db)
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle("估车侠 EstiCar")
        self.setMinimumSize(1200, 800)
        self.resize(1400, 900)

        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        header = QLabel("估车侠 EstiCar")
        header.setFont(QFont("Microsoft YaHei", 18, QFont.Bold))
        header.setAlignment(Qt.AlignCenter)
        layout.addWidget(header)

        self.tabs = QTabWidget()
        self.tabs.setFont(QFont("Microsoft YaHei", 10))

        self.tab_account = AccountTab(self.db)
        self.tab_valuation = ValuationTab(self.db)
        self.tab_task = TaskTab(self.db, self.executor)
        self.tab_data = DataTab(self.db)

        self.tabs.addTab(self.tab_account, "账号配置")
        self.tabs.addTab(self.tab_valuation, "车辆估价")
        self.tabs.addTab(self.tab_task, "任务配置")
        self.tabs.addTab(self.tab_data, "数据管理")

        layout.addWidget(self.tabs)
