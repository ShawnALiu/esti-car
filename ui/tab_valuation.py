import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from PyQt5.QtWidgets import (QApplication, QWidget, QVBoxLayout, QHBoxLayout,
                              QTableWidget, QTableWidgetItem, QLabel, QPushButton,
                              QComboBox, QLineEdit, QGroupBox, QHeaderView, QAbstractItemView,
                              QMessageBox, QFrame, QScrollArea)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QFont, QPixmap
import json


class ValuationTab(QWidget):
    def __init__(self, db):
        super().__init__()
        self.db = db
        self.init_ui()

    def init_ui(self):
        layout = QHBoxLayout(self)

        left_panel = QVBoxLayout()

        search_group = QGroupBox("搜索条件")
        search_layout = QVBoxLayout()

        brand_layout = QHBoxLayout()
        brand_layout.addWidget(QLabel("品牌:"))
        self.brand_input = QComboBox()
        self.brand_input.setEditable(True)
        self.load_brands()
        brand_layout.addWidget(self.brand_input)
        search_layout.addLayout(brand_layout)

        model_layout = QHBoxLayout()
        model_layout.addWidget(QLabel("型号:"))
        self.model_input = QComboBox()
        self.model_input.setEditable(True)
        model_layout.addWidget(self.model_input)
        search_layout.addLayout(model_layout)

        year_layout = QHBoxLayout()
        year_layout.addWidget(QLabel("年份:"))
        self.year_from = QComboBox()
        self.year_from.setEditable(True)
        for y in range(2026, 2000, -1):
            self.year_from.addItem(str(y))
        year_layout.addWidget(self.year_from)
        year_layout.addWidget(QLabel("至"))
        self.year_to = QComboBox()
        self.year_to.setEditable(True)
        for y in range(2026, 2000, -1):
            self.year_to.addItem(str(y))
        year_layout.addWidget(self.year_to)
        search_layout.addLayout(year_layout)

        price_layout = QHBoxLayout()
        price_layout.addWidget(QLabel("价格范围:"))
        self.price_min = QLineEdit()
        self.price_min.setPlaceholderText("最低")
        price_layout.addWidget(self.price_min)
        price_layout.addWidget(QLabel("至"))
        self.price_max = QLineEdit()
        self.price_max.setPlaceholderText("最高")
        price_layout.addWidget(self.price_max)
        search_layout.addLayout(price_layout)

        search_btn = QPushButton("搜索")
        search_btn.setFont(QFont("Microsoft YaHei", 10, QFont.Bold))
        search_btn.clicked.connect(self.search_cars)
        search_layout.addWidget(search_btn)

        search_group.setLayout(search_layout)
        left_panel.addWidget(search_group)

        self.car_list = QTableWidget()
        self.car_list.setColumnCount(7)
        self.car_list.setHorizontalHeaderLabels(["序号", "品牌", "型号", "年份", "起拍价", "拍卖时间", "网站"])
        self.car_list.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.car_list.setSelectionMode(QAbstractItemView.SingleSelection)
        self.car_list.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.car_list.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.car_list.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self.car_list.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.car_list.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self.car_list.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self.car_list.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeToContents)
        self.car_list.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.car_list.cellClicked.connect(self.on_car_selected)
        left_panel.addWidget(self.car_list)

        layout.addLayout(left_panel, 1)

        right_panel = QVBoxLayout()

        detail_group = QGroupBox("车辆详情与报价")
        detail_layout = QVBoxLayout()

        self.detail_label = QLabel("请选择左侧车辆查看详情")
        self.detail_label.setAlignment(Qt.AlignCenter)
        self.detail_label.setFont(QFont("Microsoft YaHei", 10))
        detail_layout.addWidget(self.detail_label)

        self.car_image_label = QLabel()
        self.car_image_label.setAlignment(Qt.AlignCenter)
        self.car_image_label.setFixedSize(400, 300)
        self.car_image_label.setStyleSheet("background-color: #f0f0f0; border: 1px solid #ccc;")
        detail_layout.addWidget(self.car_image_label)

        self.price_info = QGroupBox("参考报价")
        price_layout = QVBoxLayout()
        self.price_table = QTableWidget()
        self.price_table.setColumnCount(5)
        self.price_table.setHorizontalHeaderLabels(["来源", "品牌", "型号", "年份", "参考价"])
        self.price_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.price_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        price_layout.addWidget(self.price_table)
        self.price_info.setLayout(price_layout)
        detail_layout.addWidget(self.price_info)

        detail_group.setLayout(detail_layout)
        right_panel.addWidget(detail_group)

        layout.addLayout(right_panel, 1)

    def load_brands(self):
        brands = ["丰田", "本田", "日产", "大众", "宝马", "奔驰", "奥迪", "别克", "福特", "雪佛兰",
                  "现代", "起亚", "马自达", "三菱", "铃木", "雷克萨斯", "沃尔沃", "路虎", "捷豹", "保时捷"]
        self.brand_input.addItems(brands)

    def search_cars(self):
        filters = {}
        brand = self.brand_input.currentText().strip()
        model = self.model_input.currentText().strip()
        if brand:
            filters["brand"] = brand
        if model:
            filters["model"] = model

        where_clauses = []
        params = []

        if brand:
            where_clauses.append("brand LIKE ?")
            params.append(f"%{brand}%")
        if model:
            where_clauses.append("model LIKE ?")
            params.append(f"%{model}%")

        year_from = int(self.year_from.currentText())
        year_to = int(self.year_to.currentText())
        if year_from and year_to:
            where_clauses.append("year BETWEEN ? AND ?")
            params.extend([min(year_from, year_to), max(year_from, year_to)])

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        accident_cars = self.db.query(
            f"SELECT * FROM accident_car WHERE {where_sql} ORDER BY created_at DESC", params
        )
        used_cars = self.db.query(
            f"SELECT * FROM used_car WHERE {where_sql} ORDER BY created_at DESC", params
        )

        all_cars = []
        for car in accident_cars:
            car["_type"] = "事故车"
            all_cars.append(car)
        for car in used_cars:
            car["_type"] = "二手车"
            all_cars.append(car)

        self.car_list.setRowCount(len(all_cars))
        for i, car in enumerate(all_cars):
            self.car_list.setItem(i, 0, QTableWidgetItem(str(i + 1)))
            self.car_list.setItem(i, 1, QTableWidgetItem(car.get("brand", "")))
            self.car_list.setItem(i, 2, QTableWidgetItem(car.get("model", "")))
            self.car_list.setItem(i, 3, QTableWidgetItem(str(car.get("year", ""))))
            self.car_list.setItem(i, 4, QTableWidgetItem(f"¥{car.get('start_price', 0):,.2f}"))
            self.car_list.setItem(i, 5, QTableWidgetItem(car.get("auction_time", "")))
            self.car_list.setItem(i, 6, QTableWidgetItem(car.get("site_name", "")))
            self.car_list.item(i, 0).setData(Qt.UserRole, car)

        if not all_cars:
            QMessageBox.information(self, "提示", "未找到匹配的车辆数据，请先运行爬虫任务。")

    def on_car_selected(self, row, col):
        car = self.car_list.item(row, 0).data(Qt.UserRole)
        if not car:
            return

        car_type = car.get("_type", "")
        detail_text = f"类型: {car_type}\n"
        detail_text += f"品牌: {car.get('brand', '')}\n"
        detail_text += f"型号: {car.get('model', '')}\n"
        detail_text += f"年份: {car.get('year', '')}\n"
        detail_text += f"起拍价: ¥{car.get('start_price', 0):,.2f}\n"
        detail_text += f"拍卖时间: {car.get('auction_time', '')}\n"
        detail_text += f"网站: {car.get('site_name', '')}\n"
        detail_text += f"链接: {car.get('detail_urls', '')}"

        self.detail_label.setText(detail_text)
        self.detail_label.setAlignment(Qt.AlignLeft | Qt.AlignTop)

        brand = car.get("brand", "")
        model = car.get("model", "")
        year = car.get("year", 0)

        where_clauses = []
        params = []
        if brand:
            where_clauses.append("brand LIKE ?")
            params.append(f"%{brand}%")
        if model:
            where_clauses.append("model LIKE ?")
            params.append(f"%{model}%")
        if year:
            where_clauses.append("year = ?")
            params.append(year)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        similar_cars = self.db.query(
            f"SELECT site_name, brand, model, year, start_price FROM accident_car WHERE {where_sql} LIMIT 20", params
        )

        self.price_table.setRowCount(len(similar_cars))
        for i, sc in enumerate(similar_cars):
            self.price_table.setItem(i, 0, QTableWidgetItem(sc.get("site_name", "")))
            self.price_table.setItem(i, 1, QTableWidgetItem(sc.get("brand", "")))
            self.price_table.setItem(i, 2, QTableWidgetItem(sc.get("model", "")))
            self.price_table.setItem(i, 3, QTableWidgetItem(str(sc.get("year", ""))))
            self.price_table.setItem(i, 4, QTableWidgetItem(f"¥{sc.get('start_price', 0):,.2f}"))
