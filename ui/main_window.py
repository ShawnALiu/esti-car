import sys
import os
import logging

from core.config import GLOBAL_APP_VERSION, GLOBAL_APP_NAME

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from PyQt5.QtWidgets import (QMainWindow, QTabWidget, QVBoxLayout, QWidget,
                              QLabel, QApplication, QPushButton, QHBoxLayout,
                              QTextEdit, QDockWidget)
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QFont, QTextCursor
from ui.tab_valuation import ValuationTab
from ui.tab_account import AccountTab
from ui.tab_task import TaskTab
from ui.tab_data import DataTab
from core.logger import get_logger, LogHandler


logger = get_logger()


class MainWindow(QMainWindow):
    def __init__(self, db, executor):
        super().__init__()

        self._log_messages = []
        self._max_log_lines = 200
        self.init_log_handler()

        self.db = db
        self.executor = executor
        self.init_ui()

    def init_log_handler(self):
        import logging
        root_logger = logging.getLogger()
        self._log_handler = LogHandler(self.append_log)
        self._log_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%H:%M:%S'))
        root_logger.addHandler(self._log_handler)
        
        self._log_timer = QTimer()
        self._log_timer.timeout.connect(self.flush_logs)
        self._log_timer.start(500)

    def append_log(self, msg):
        self._log_messages.append(msg)
        if len(self._log_messages) > self._max_log_lines:
            self._log_messages = self._log_messages[-self._max_log_lines:]

    def flush_logs(self):
        if self._log_messages and hasattr(self, 'log_text'):
            try:
                self.log_text.append("\n".join(self._log_messages))
                self._log_messages = []
                self.log_text.moveCursor(self.log_text.textCursor().End)
                
                doc = self.log_text.document()
                while doc.blockCount() > self._max_log_lines:
                    cursor = QTextCursor(doc.findBlockByLineNumber(0))
                    cursor.select(QTextCursor.BlockUnderCursor)
                    cursor.removeSelectedText()
                    cursor.deleteChar()
            except:
                pass

    def init_ui(self):
        self.setWindowTitle(GLOBAL_APP_NAME)
        self.setMinimumSize(1800, 900)
        self.resize(1800, 1000)
        self.setWindowFlags(Qt.Window)

        self.log_panel = None
        self.log_panel_visible = False

        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QHBoxLayout(central)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        content_widget = QWidget()
        content_layout = QVBoxLayout(content_widget)
        content_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.addWidget(content_widget, 1)

        header_layout = QHBoxLayout()

        header = QLabel(f"{GLOBAL_APP_NAME} v{GLOBAL_APP_VERSION}")
        header.setFont(QFont("Microsoft YaHei", 18, QFont.Bold))
        header.setAlignment(Qt.AlignCenter)
        header_layout.addWidget(header, 1)

        log_btn = QPushButton("日志")
        log_btn.setFixedWidth(60)
        log_btn.clicked.connect(self.toggle_log_panel)
        header_layout.addWidget(log_btn)
        
        content_layout.addLayout(header_layout)

        self.tabs = QTabWidget()
        self.tabs.setFont(QFont("Microsoft YaHei", 10))

        self.tab_account = AccountTab(self.db)
        self.tab_valuation = ValuationTab(self.db)
        self.tab_task = TaskTab(self.db, self.executor)
        self.tab_data = DataTab(self.db)

        self.tabs.addTab(self.tab_account, "账号配置")
        self.tabs.addTab(self.tab_data, "数据管理")
        self.tabs.addTab(self.tab_task, "任务配置")
        self.tabs.addTab(self.tab_valuation, "车辆估价")

        content_layout.addWidget(self.tabs)

    def toggle_log_panel(self):
        if self.log_panel_visible:
            if self.log_panel:
                self.log_panel.hide()
            self.log_panel_visible = False
        else:
            if not self.log_panel:
                self.create_log_panel()
            self.log_panel.show()
            self.log_panel_visible = True

    def create_log_panel(self):
        self.log_panel = QDockWidget("日志", self)
        self.log_panel.setWidget(QWidget())
        log_layout = QVBoxLayout(self.log_panel.widget())
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setLineWrapMode(QTextEdit.NoWrap)
        self.log_text.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self.log_text.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        log_layout.addWidget(self.log_text)
        
        toolbar = QHBoxLayout()
        
        wrap_check = QPushButton("自动换行")
        wrap_check.setCheckable(True)
        wrap_check.clicked.connect(lambda checked: self.log_text.setLineWrapMode(
            QTextEdit.WidgetWidth if checked else QTextEdit.NoWrap))
        toolbar.addWidget(wrap_check)
        
        clear_btn = QPushButton("清空")
        clear_btn.clicked.connect(self.log_text.clear)
        toolbar.addWidget(clear_btn)
        
        toolbar.addStretch()
        
        close_btn = QPushButton("关闭")
        close_btn.clicked.connect(self.toggle_log_panel)
        toolbar.addWidget(close_btn)
        
        log_layout.addLayout(toolbar)
        
        self.log_panel.setFeatures(QDockWidget.DockWidgetClosable | QDockWidget.DockWidgetFloatable | QDockWidget.DockWidgetVerticalTitleBar)
        self.log_panel.setAllowedAreas(Qt.RightDockWidgetArea)
        self.addDockWidget(Qt.RightDockWidgetArea, self.log_panel)
        
        self.log_panel.visibilityChanged.connect(self._on_log_panel_visibility_changed)
        self.log_panel.topLevelChanged.connect(self._on_log_panel_toplevel_changed)
        
        self._log_panel_floating = False
    
    def _on_log_panel_visibility_changed(self, visible):
        if not self._log_panel_floating:
            self.log_panel_visible = visible
    
    def _on_log_panel_toplevel_changed(self, floating):
        self._log_panel_floating = floating
        if not floating and not self.log_panel.isVisible():
            self.log_panel_visible = False
        elif not floating and self.log_panel.isVisible():
            self.log_panel_visible = True
