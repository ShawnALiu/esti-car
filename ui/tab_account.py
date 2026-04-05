import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTableWidget,
                              QTableWidgetItem, QPushButton, QLineEdit, QLabel,
                              QHeaderView, QAbstractItemView, QMessageBox, QGroupBox)
from PyQt5.QtCore import Qt
from datetime import datetime


class AccountTab(QWidget):
    def __init__(self, db):
        super().__init__()
        self.db = db
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        toolbar = QHBoxLayout()

        self.site_name_input = QLineEdit()
        self.site_name_input.setPlaceholderText("网站名称")
        toolbar.addWidget(QLabel("网站名:"))
        toolbar.addWidget(self.site_name_input)

        self.site_url_input = QLineEdit()
        self.site_url_input.setPlaceholderText("https://example.com")
        toolbar.addWidget(QLabel("网站地址:"))
        toolbar.addWidget(self.site_url_input)

        self.username_input = QLineEdit()
        self.username_input.setPlaceholderText("账号")
        toolbar.addWidget(QLabel("账号:"))
        toolbar.addWidget(self.username_input)

        self.password_input = QLineEdit()
        self.password_input.setPlaceholderText("密码")
        self.password_input.setEchoMode(QLineEdit.Password)
        toolbar.addWidget(QLabel("密码:"))
        toolbar.addWidget(self.password_input)

        add_btn = QPushButton("新增")
        add_btn.clicked.connect(self.add_account)
        toolbar.addWidget(add_btn)

        update_btn = QPushButton("修改")
        update_btn.clicked.connect(self.update_account)
        toolbar.addWidget(update_btn)

        delete_btn = QPushButton("删除")
        delete_btn.clicked.connect(self.delete_account)
        toolbar.addWidget(delete_btn)

        refresh_btn = QPushButton("刷新")
        refresh_btn.clicked.connect(self.load_accounts)
        toolbar.addWidget(refresh_btn)

        layout.addLayout(toolbar)

        self.table = QTableWidget()
        self.table.setColumnCount(7)
        self.table.setHorizontalHeaderLabels(["序号", "网站名", "网站地址", "账号", "密码", "创建时间", "更新时间"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeToContents)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.cellClicked.connect(self.on_row_selected)
        layout.addWidget(self.table)

        self.selected_id = None
        self.load_accounts()

    def load_accounts(self):
        accounts = self.db.query("SELECT * FROM account_config ORDER BY id ASC")
        self.table.setRowCount(len(accounts))
        for i, acc in enumerate(accounts):
            self.table.setItem(i, 0, QTableWidgetItem(str(acc["id"])))
            self.table.setItem(i, 1, QTableWidgetItem(acc["site_name"]))
            self.table.setItem(i, 2, QTableWidgetItem(acc["site_url"]))
            self.table.setItem(i, 3, QTableWidgetItem(acc["username"]))
            self.table.setItem(i, 4, QTableWidgetItem(acc["password"]))
            self.table.setItem(i, 5, QTableWidgetItem(acc["created_at"]))
            self.table.setItem(i, 6, QTableWidgetItem(acc["updated_at"]))

    def on_row_selected(self, row, col):
        self.selected_id = int(self.table.item(row, 0).text())
        self.site_name_input.setText(self.table.item(row, 1).text())
        self.site_url_input.setText(self.table.item(row, 2).text())
        self.username_input.setText(self.table.item(row, 3).text())
        self.password_input.setText(self.table.item(row, 4).text())

    def add_account(self):
        site_name = self.site_name_input.text().strip()
        site_url = self.site_url_input.text().strip()
        username = self.username_input.text().strip()
        password = self.password_input.text().strip()

        if not site_name or not site_url or not username or not password:
            QMessageBox.warning(self, "错误", "请填写完整信息")
            return

        self.db.insert("account_config", {
            "site_name": site_name,
            "site_url": site_url,
            "username": username,
            "password": password,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        self.clear_inputs()
        self.load_accounts()
        QMessageBox.information(self, "成功", "账号配置已添加")

    def update_account(self):
        if not self.selected_id:
            QMessageBox.warning(self, "错误", "请先选择要修改的记录")
            return

        site_name = self.site_name_input.text().strip()
        site_url = self.site_url_input.text().strip()
        username = self.username_input.text().strip()
        password = self.password_input.text().strip()

        if not site_name or not site_url or not username or not password:
            QMessageBox.warning(self, "错误", "请填写完整信息")
            return

        self.db.update("account_config", {
            "site_name": site_name,
            "site_url": site_url,
            "username": username,
            "password": password,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }, "id = ?", (self.selected_id,))

        self.clear_inputs()
        self.load_accounts()
        QMessageBox.information(self, "成功", "账号配置已更新")

    def delete_account(self):
        if not self.selected_id:
            QMessageBox.warning(self, "错误", "请先选择要删除的记录")
            return

        reply = QMessageBox.question(self, "确认", "确定要删除此账号配置吗?",
                                     QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            self.db.delete("account_config", "id = ?", (self.selected_id,))
            self.clear_inputs()
            self.load_accounts()
            QMessageBox.information(self, "成功", "账号配置已删除")

    def clear_inputs(self):
        self.selected_id = None
        self.site_name_input.clear()
        self.site_url_input.clear()
        self.username_input.clear()
        self.password_input.clear()
