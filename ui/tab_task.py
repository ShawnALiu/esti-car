import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTableWidget,
                              QTableWidgetItem, QPushButton, QLineEdit, QLabel,
                              QHeaderView, QAbstractItemView, QMessageBox, QGroupBox,
                              QDialog, QComboBox, QSpinBox, QRadioButton, QButtonGroup,
                              QFormLayout, QTabWidget)
from PyQt5.QtCore import Qt, QTimer
from datetime import datetime


class TaskTab(QWidget):
    def __init__(self, db, executor):
        super().__init__()
        self.db = db
        self.executor = executor
        self.current_page = 0
        self.page_size = 20
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        self.tab_widget = QTabWidget()

        self.create_task_list_tab()
        self.create_active_tasks_tab()
        self.create_history_tab()

        self.tab_widget.addTab(self.task_list_panel, "创建任务")
        self.tab_widget.addTab(self.active_tasks_panel, "存活任务")
        self.tab_widget.addTab(self.history_panel, "历史任务")

        layout.addWidget(self.tab_widget)

    def create_task_list_tab(self):
        self.task_list_panel = QWidget()
        layout = QVBoxLayout(self.task_list_panel)

        toolbar = QHBoxLayout()
        create_btn = QPushButton("创建任务")
        create_btn.clicked.connect(self.show_create_dialog)
        toolbar.addWidget(create_btn)

        refresh_btn = QPushButton("刷新")
        refresh_btn.clicked.connect(self.load_tasks)
        toolbar.addWidget(refresh_btn)

        self.page_label = QLabel("第 1 页")
        toolbar.addWidget(self.page_label)

        prev_btn = QPushButton("上一页")
        prev_btn.clicked.connect(self.prev_page)
        toolbar.addWidget(prev_btn)

        next_btn = QPushButton("下一页")
        next_btn.clicked.connect(self.next_page)
        toolbar.addWidget(next_btn)

        layout.addLayout(toolbar)

        self.task_table = QTableWidget()
        self.task_table.setColumnCount(11)
        self.task_table.setHorizontalHeaderLabels(["序号", "任务名", "类型", "网站", "最多数量", "执行方式", "执行间隔", "是否启用", "状态切换", "操作", "编辑"])
        self.task_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.task_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.task_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.task_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.task_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self.task_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self.task_table.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeToContents)
        self.task_table.horizontalHeader().setSectionResizeMode(7, QHeaderView.ResizeToContents)
        self.task_table.horizontalHeader().setSectionResizeMode(8, QHeaderView.ResizeToContents)
        self.task_table.horizontalHeader().setSectionResizeMode(9, QHeaderView.ResizeToContents)
        self.task_table.horizontalHeader().setSectionResizeMode(10, QHeaderView.ResizeToContents)
        self.task_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.task_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.task_table)

        self.load_tasks()

        self.refresh_timer = QTimer()
        self.refresh_timer.timeout.connect(self.load_tasks)
        self.refresh_timer.start(3000)

    def create_active_tasks_tab(self):
        self.active_tasks_panel = QWidget()
        layout = QVBoxLayout(self.active_tasks_panel)

        refresh_btn = QPushButton("刷新")
        refresh_btn.clicked.connect(self.load_active_tasks)
        layout.addWidget(refresh_btn)

        self.active_table = QTableWidget()
        self.active_table.setColumnCount(5)
        self.active_table.setHorizontalHeaderLabels(["任务ID", "任务名", "类型", "执行ID", "状态"])
        self.active_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.active_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.active_table)

        self.timer = QTimer()
        self.timer.timeout.connect(self.load_active_tasks)
        self.timer.start(5000)

        self.load_active_tasks()

    def create_history_tab(self):
        self.history_panel = QWidget()
        layout = QVBoxLayout(self.history_panel)

        refresh_btn = QPushButton("刷新")
        refresh_btn.clicked.connect(self.load_history)
        layout.addWidget(refresh_btn)

        self.history_table = QTableWidget()
        self.history_table.setColumnCount(6)
        self.history_table.setHorizontalHeaderLabels(["执行ID", "任务ID", "状态", "开始时间", "结束时间", "状态描述"])
        self.history_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.history_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.history_table)

        self.load_history()

    def load_tasks(self):
        total = self.db.count("task")
        tasks = self.db.query(
            "SELECT * FROM task ORDER BY id DESC LIMIT :limit OFFSET :offset",
            {"limit": self.page_size, "offset": self.current_page * self.page_size}
        )

        self.task_table.setRowCount(len(tasks))
        active_task_ids = set(self.executor.get_active_tasks())
        for i, task in enumerate(tasks):
            self.task_table.setItem(i, 0, QTableWidgetItem(str(task["id"])))
            self.task_table.setItem(i, 1, QTableWidgetItem(task["name"]))
            type_text = "事故车爬取" if task["task_type"] == "accident" else "二手车爬取"
            self.task_table.setItem(i, 2, QTableWidgetItem(type_text))
            self.task_table.setItem(i, 3, QTableWidgetItem(task.get("account_site_name", "")))
            self.task_table.setItem(i, 4, QTableWidgetItem(str(task["max_count"])))

            schedule_type = task.get("schedule_type", "manual")
            schedule_text = "手动执行" if schedule_type == "manual" else "定时执行"
            self.task_table.setItem(i, 5, QTableWidgetItem(schedule_text))

            interval = task.get("cron_expression", "3")
            interval_text = "-" if schedule_type == "manual" else f"{interval}分钟"
            self.task_table.setItem(i, 6, QTableWidgetItem(interval_text))

            enabled_text = "是" if task.get("enabled") == 1 else "否"
            self.task_table.setItem(i, 7, QTableWidgetItem(enabled_text))

            task_id = task["id"]
            is_running = task_id in active_task_ids
            is_manual = schedule_type == "manual"

            enabled = task.get("enabled") == 1
            toggle_btn = QPushButton("停用" if enabled else "启用")
            if is_manual or is_running:
                toggle_btn.setEnabled(False)
            else:
                toggle_btn.clicked.connect(lambda checked, tid=task_id: self.toggle_task_enabled(tid))
            self.task_table.setCellWidget(i, 8, toggle_btn)

            btn = QPushButton("执行")
            edit_btn = QPushButton("编辑")
            btn.setEnabled(not is_running)
            edit_btn.setEnabled(not is_running)
            btn.clicked.connect(lambda checked, tid=task_id, b=btn, e=edit_btn: self.execute_task(tid, b, e))
            self.task_table.setCellWidget(i, 9, btn)
            edit_btn.clicked.connect(lambda checked, tid=task_id: self.show_edit_dialog(tid))
            self.task_table.setCellWidget(i, 10, edit_btn)

        self.page_label.setText(f"第 {self.current_page + 1} 页 / 共 {(total + self.page_size - 1) // self.page_size if total else 1} 页")

    def toggle_task_enabled(self, task_id):
        task = self.db.query_one("SELECT enabled FROM task WHERE id = :id", {"id": task_id})
        if task:
            new_enabled = 0 if task["enabled"] == 1 else 1
            self.db.update("task", {"enabled": new_enabled, "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}, "id = :id", {"id": task_id})
            self.load_tasks()

    def load_active_tasks(self):
        active_ids = self.executor.get_active_tasks()
        self.active_table.setRowCount(len(active_ids))
        for i, task_id in enumerate(active_ids):
            task = self.db.query_one("SELECT * FROM task WHERE id = :id", {"id": task_id})
            exec_id = self.executor.active_tasks.get(task_id)
            if task:
                self.active_table.setItem(i, 0, QTableWidgetItem(str(task_id)))
                self.active_table.setItem(i, 1, QTableWidgetItem(task["name"]))
                type_text = "事故车爬取" if task["task_type"] == "accident" else "二手车爬取"
                self.active_table.setItem(i, 2, QTableWidgetItem(type_text))
                self.active_table.setItem(i, 3, QTableWidgetItem(str(exec_id) if exec_id else ""))
                self.active_table.setItem(i, 4, QTableWidgetItem("运行中"))

    def load_history(self):
        executions = self.db.query(
            "SELECT * FROM task_execution ORDER BY id DESC LIMIT 100"
        )
        self.history_table.setRowCount(len(executions))
        for i, exe in enumerate(executions):
            self.history_table.setItem(i, 0, QTableWidgetItem(str(exe["id"])))
            self.history_table.setItem(i, 1, QTableWidgetItem(exe.get("task_id", "")))
            self.history_table.setItem(i, 2, QTableWidgetItem(exe.get("status", "")))
            self.history_table.setItem(i, 3, QTableWidgetItem(exe.get("start_time", "")))
            self.history_table.setItem(i, 4, QTableWidgetItem(exe.get("end_time", "")))
            status_text = "成功" if exe.get("status") == "success" else ("失败" if exe.get("status") == "failed" else "运行中")
            self.history_table.setItem(i, 5, QTableWidgetItem(status_text))

    def execute_task(self, task_id, btn, edit_btn):
        if task_id in self.executor.get_active_tasks():
            return
        
        self.executor.clear_last_error()
        btn.setEnabled(False)
        edit_btn.setEnabled(False)
        self.executor.execute_task(task_id)
        
        QTimer.singleShot(100, lambda: self.check_task_error(task_id, btn, edit_btn))
    
    def check_task_error(self, task_id, btn, edit_btn):
        error = self.executor.get_last_error()
        if error:
            self.executor.clear_last_error()
            if not btn.isVisible():
                return
            try:
                btn.setEnabled(True)
                edit_btn.setEnabled(True)
            except:
                pass
            QMessageBox.warning(self, "错误", error)

    def show_edit_dialog(self, task_id):
        dialog = CreateTaskDialog(self.db, self, task_id)
        if dialog.exec_() == QDialog.Accepted:
            self.load_tasks()

    def prev_page(self):
        if self.current_page > 0:
            self.current_page -= 1
            self.load_tasks()

    def next_page(self):
        total = self.db.count("task")
        max_page = (total - 1) // self.page_size
        if self.current_page < max_page:
            self.current_page += 1
            self.load_tasks()

    def show_create_dialog(self):
        dialog = CreateTaskDialog(self.db, self)
        if dialog.exec_() == QDialog.Accepted:
            self.load_tasks()


class CreateTaskDialog(QDialog):
    def __init__(self, db, parent=None, task_id=None):
        super().__init__(parent)
        self.db = db
        self.task_id = task_id
        self.setWindowTitle("编辑任务" if task_id else "创建任务")
        self.setMinimumWidth(500)
        self.init_ui()
        if task_id:
            self.load_task_data()

    def load_task_data(self):
        task = self.db.query_one("SELECT * FROM task WHERE id = :id", {"id": self.task_id})
        if task:
            self.name_input.setText(task.get("name", ""))
            self.type_combo.setCurrentText("事故车爬取" if task.get("task_type") == "accident" else "二手车爬取")
            idx = self.account_combo.findData(task.get("account_id"))
            if idx >= 0:
                self.account_combo.setCurrentIndex(idx)
            self.max_count_spin.setValue(task.get("max_count", 100))
            if task.get("schedule_type") == "cron":
                self.cron_radio.setChecked(True)
                self.interval_spin.setValue(int(task.get("cron_expression", 3)))
                self.interval_spin.setEnabled(True)
            else:
                self.manual_radio.setChecked(True)
            self.enabled_combo.setCurrentText("是" if task.get("enabled") == 1 else "否")
            self.on_schedule_type_changed()

    def init_ui(self):
        layout = QFormLayout(self)

        self.name_input = QLineEdit()
        layout.addRow("任务名:", self.name_input)

        self.type_combo = QComboBox()
        self.type_combo.addItem("事故车爬取", "accident")
        self.type_combo.addItem("二手车爬取", "used")
        layout.addRow("任务类型:", self.type_combo)

        self.account_combo = QComboBox()
        accounts = self.db.query("SELECT id, site_name FROM account_config ORDER BY id")
        for acc in accounts:
            self.account_combo.addItem(acc["site_name"], acc["id"])
        layout.addRow("账号配置:", self.account_combo)

        self.max_count_spin = QSpinBox()
        self.max_count_spin.setRange(1, 10000)
        self.max_count_spin.setValue(100)
        layout.addRow("最多数量:", self.max_count_spin)

        self.schedule_group = QButtonGroup(self)
        self.manual_radio = QRadioButton("手动执行")
        self.cron_radio = QRadioButton("定时执行")
        self.schedule_group.addButton(self.manual_radio)
        self.schedule_group.addButton(self.cron_radio)
        self.manual_radio.setChecked(True)
        self.manual_radio.toggled.connect(self.on_schedule_type_changed)

        schedule_layout = QHBoxLayout()
        schedule_layout.addWidget(self.manual_radio)
        schedule_layout.addWidget(self.cron_radio)
        layout.addRow("执行方式:", schedule_layout)

        self.enabled_combo = QComboBox()
        self.enabled_combo.addItem("是", 1)
        self.enabled_combo.addItem("否", 0)
        self.enabled_combo.setCurrentIndex(0)
        layout.addRow("是否启用:", self.enabled_combo)

        self.interval_spin = QSpinBox()
        self.interval_spin.setRange(1, 1440)
        self.interval_spin.setValue(3)
        self.interval_spin.setSuffix(" 分钟")
        self.interval_spin.setEnabled(False)
        self.cron_radio.toggled.connect(self.interval_spin.setEnabled)
        layout.addRow("执行间隔:", self.interval_spin)

        btn_layout = QHBoxLayout()
        save_btn = QPushButton("保存")
        save_btn.clicked.connect(self.save_task)
        btn_layout.addWidget(save_btn)

        cancel_btn = QPushButton("取消")
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(cancel_btn)

        layout.addRow(btn_layout)

    def on_schedule_type_changed(self):
        is_manual = self.manual_radio.isChecked()
        self.enabled_combo.setEnabled(not is_manual)
        if is_manual:
            self.enabled_combo.setCurrentIndex(0)

    def save_task(self):
        name = self.name_input.text().strip()
        if not name:
            QMessageBox.warning(self, "错误", "请输入任务名")
            return

        task_type = self.type_combo.currentData()
        account_id = self.account_combo.currentData()
        account = self.db.query_one("SELECT site_name FROM account_config WHERE id = :id", {"id": account_id})
        max_count = self.max_count_spin.value()
        schedule_type = "cron" if self.cron_radio.isChecked() else "manual"
        interval = str(self.interval_spin.value()) if self.cron_radio.isChecked() else None
        enabled = self.enabled_combo.currentData()

        task_data = {
            "name": name,
            "task_type": task_type,
            "account_id": account_id,
            "account_site_name": account.get("site_name", "") if account else "",
            "max_count": max_count,
            "schedule_type": schedule_type,
            "cron_expression": interval,
            "enabled": enabled,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        if self.task_id:
            self.db.update("task", task_data, "id = :id", {"id": self.task_id})
        else:
            task_data["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.db.insert("task", task_data)

        self.accept()
