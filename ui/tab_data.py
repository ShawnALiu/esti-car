import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTableWidget,
                              QTableWidgetItem, QPushButton, QLineEdit, QLabel,
                              QHeaderView, QAbstractItemView, QMessageBox, QGroupBox,
                              QComboBox, QDateEdit, QTabWidget, QFormLayout)
from PyQt5.QtCore import Qt, QDate
from datetime import datetime


class DataTab(QWidget):
    def __init__(self, db):
        super().__init__()
        self.db = db
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        self.tab_widget = QTabWidget()

        self.create_stats_tab()
        self.create_cleanup_tab()

        self.tab_widget.addTab(self.stats_panel, "数据统计")
        self.tab_widget.addTab(self.cleanup_panel, "数据清理")

        layout.addWidget(self.tab_widget)

    def create_stats_tab(self):
        self.stats_panel = QWidget()
        layout = QVBoxLayout(self.stats_panel)

        refresh_btn = QPushButton("刷新统计")
        refresh_btn.clicked.connect(self.load_stats)
        layout.addWidget(refresh_btn)

        self.stats_table = QTableWidget()
        self.stats_table.setColumnCount(3)
        self.stats_table.setHorizontalHeaderLabels(["数据表", "记录数", "最后更新"])
        self.stats_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.stats_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.stats_table)

        self.load_stats()

    def create_cleanup_tab(self):
        self.cleanup_panel = QWidget()
        layout = QFormLayout(self.cleanup_panel)

        self.table_combo = QComboBox()
        self.table_combo.addItem("事故车表", "accident_car")
        self.table_combo.addItem("二手车表", "used_car")
        self.table_combo.addItem("任务执行表", "task_execution")
        self.table_combo.addItem("任务表", "task")
        layout.addRow("选择数据表:", self.table_combo)

        self.date_edit = QDateEdit()
        self.date_edit.setCalendarPopup(True)
        self.date_edit.setDate(QDate.currentDate().addDays(-30))
        self.date_edit.setDisplayFormat("yyyy-MM-dd")
        layout.addRow("清理日期之前:", self.date_edit)

        cleanup_btn = QPushButton("执行清理")
        cleanup_btn.clicked.connect(self.cleanup_data)
        layout.addRow("", cleanup_btn)

        self.result_label = QLabel("")
        layout.addRow("清理结果:", self.result_label)

    def load_stats(self):
        tables = [
            ("accident_car", "事故车表"),
            ("used_car", "二手车表"),
            ("task", "任务表"),
            ("task_execution", "任务执行表"),
            ("account_config", "账号配置表")
        ]

        self.stats_table.setRowCount(len(tables))
        for i, (table, name) in enumerate(tables):
            count = self.db.count(table)
            last = self.db.query_one(f"SELECT MAX(created_at) as last_update FROM {table}")
            last_update = last["last_update"] if last and last["last_update"] else "无记录"

            self.stats_table.setItem(i, 0, QTableWidgetItem(name))
            self.stats_table.setItem(i, 1, QTableWidgetItem(str(count)))
            self.stats_table.setItem(i, 2, QTableWidgetItem(last_update))

    def cleanup_data(self):
        table = self.table_combo.currentData()
        date_str = self.date_edit.date().toString("yyyy-MM-dd") + " 00:00:00"

        reply = QMessageBox.question(self, "确认",
                                     f"确定要清理 {self.table_combo.currentText()} 中 {date_str} 之前的数据吗?",
                                     QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            self.db.cleanup_data(table, date_str)
            self.result_label.setText(f"已清理 {table} 中 {date_str} 之前的数据")
            QMessageBox.information(self, "成功", "数据清理完成")
            self.load_stats()
