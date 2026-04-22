import datetime
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from PyQt5.QtWidgets import (QApplication, QWidget, QVBoxLayout, QHBoxLayout,
                              QTableWidget, QTableWidgetItem, QLabel, QPushButton,
                              QComboBox, QLineEdit, QGroupBox, QHeaderView, QAbstractItemView,
                              QMessageBox, QFrame, QScrollArea, QSpinBox, QSizePolicy, QDialog)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QFont, QPixmap
import json
from core import config


class ValuationTab(QWidget):
    def __init__(self, db):
        super().__init__()
        self.db = db
        self.current_page = 0
        self.page_size = 50
        self.total_count = 0
        self.current_cars = []
        self.similar_current_page = 0
        self.similar_total_count = 0
        self.similar_cars = []
        self.init_ui()

    def init_ui(self):
        main_layout = QHBoxLayout()

        left_panel = QWidget()
        left_layout = QVBoxLayout()
        left_layout.setContentsMargins(3, 3, 3, 3)

        search_group = QGroupBox("搜索条件")
        search_layout = QVBoxLayout()

        type_layout = QHBoxLayout()
        type_layout.addWidget(QLabel("车辆类型:"))
        self.car_type = QComboBox()
        self.car_type.addItems(["事故车", "二手车"])
        self.car_type.setCurrentIndex(0)
        type_layout.addWidget(self.car_type)
        search_layout.addLayout(type_layout)

        brand_layout = QHBoxLayout()
        brand_layout.addWidget(QLabel("品牌:"))
        self.brand_input = QComboBox()
        self.brand_input.setEditable(True)
        self.brand_input.addItem("")
        self.load_brands()
        brand_layout.addWidget(self.brand_input)
        search_layout.addLayout(brand_layout)

        model_layout = QHBoxLayout()
        model_layout.addWidget(QLabel("车型:"))
        self.model_input = QComboBox()
        self.model_input.setEditable(True)
        self.model_input.addItem("")
        model_layout.addWidget(self.model_input)
        search_layout.addLayout(model_layout)

        self.current_year = datetime.datetime.now().year
        year_layout = QHBoxLayout()
        year_layout.addWidget(QLabel("出厂日期:"))
        self.year_from = QLineEdit()
        self.year_from.setPlaceholderText(f"开始年份(默认{self.current_year - 5})")
        self.year_from.setText(str(self.current_year - 5))
        year_layout.addWidget(self.year_from)
        year_layout.addWidget(QLabel("至"))
        self.year_to = QLineEdit()
        self.year_to.setPlaceholderText(f"结束年份(默认{self.current_year})")
        self.year_to.setText(str(self.current_year))
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
        left_layout.addWidget(search_group)

        self.car_list = QTableWidget()
        self.car_list.setColumnCount(8)
        self.car_list.setHorizontalHeaderLabels(["ID", "品牌", "车型", "出厂年份", "起拍价", "最新出价", "拍卖时间", "是否新能源"])
        self.car_list.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.car_list.setSelectionMode(QAbstractItemView.SingleSelection)
        self.car_list.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.car_list.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.car_list.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self.car_list.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.car_list.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self.car_list.horizontalHeader().setSectionResizeMode(5, QHeaderView.Stretch)
        self.car_list.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeToContents)
        self.car_list.horizontalHeader().setSectionResizeMode(7, QHeaderView.ResizeToContents)
        self.car_list.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.car_list.setStyleSheet("QHeaderView::section { font-weight: bold; }")
        self.car_list.cellClicked.connect(self.on_car_selected)
        left_layout.addWidget(self.car_list)

        self.page_layout = QHBoxLayout()
        self.page_label = QLabel("第 1 页 / 共 1 页")
        self.page_layout.addWidget(self.page_label)
        
        self.prev_btn = QPushButton("上一页")
        self.prev_btn.clicked.connect(self.prev_page)
        self.page_layout.addWidget(self.prev_btn)
        
        self.next_btn = QPushButton("下一页")
        self.next_btn.clicked.connect(self.next_page)
        self.page_layout.addWidget(self.next_btn)
        
        left_layout.addLayout(self.page_layout)

        left_panel.setLayout(left_layout)

        right_panel = QWidget()
        right_layout = QVBoxLayout()
        right_layout.setContentsMargins(3, 3, 3, 3)
        right_layout.setSpacing(5)

        detail_group = QGroupBox("车辆详情")
        detail_group.setMinimumHeight(400)
        detail_layout = QHBoxLayout()

        self.accident_detail_widget = QWidget()
        accident_main_layout = QVBoxLayout()
        accident_main_layout.setContentsMargins(5, 5, 5, 5)

        accident_info_layout = QVBoxLayout()
        accident_info_layout.setSpacing(5)
        accident_label = QLabel("<b>车辆详情1</b>")
        accident_label.setStyleSheet("font-size: 14px; font-weight: bold;")
        accident_info_layout.addWidget(accident_label)

        self.accident_detail_items = {}
        common_fields = [
            ("id", "ID"), ("che_liang_pin_pai", "品牌"), ("xuan_ze_zi_xi_lie", "车型"),
            ("chu_chang_ri_qi", "出厂年份"), ("chesunyuanyin", "车损原因"), ("yi_kou_jia", "起拍价"),
            ("zui_xin_chu_jia", "最新出价"), ("is_xin_neng_yuan", "是否新能源"),
            ("gu_jia_ping_ji", "估价评级"), ("wai_guan_ping_ji", "外观评级"),
            ("pai_mai_hui_start_time", "拍卖开始时间"), ("pai_mai_jie_shu_date", "拍卖结束时间")
        ]
        for key, label in common_fields:
            row = QHBoxLayout()
            label_widget = QLabel(f"{label}:")
            label_widget.setFixedWidth(100)
            label_widget.setAlignment(Qt.AlignTop)
            row.addWidget(label_widget)
            value_label = QLabel("-")
            value_label.setWordWrap(True)
            value_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
            value_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
            self.accident_detail_items[key] = value_label
            row.addWidget(value_label)
            accident_info_layout.addLayout(row)

        accident_image_layout = QVBoxLayout()
        self.accident_main_image = QLabel()
        self.accident_main_image.setAlignment(Qt.AlignLeft)
        self.accident_main_image.setFixedSize(250, 150)
        self.accident_main_image.setScaledContents(True)
        self.accident_main_image.setStyleSheet("background-color: #f0f0f0; border: 1px solid #ccc;")
        self.accident_main_image.setCursor(Qt.PointingHandCursor)
        self.accident_main_image.mousePressEvent = lambda e: self.show_image_popup("accident")

        self.accident_thumb_scroll = QScrollArea()
        self.accident_thumb_scroll.setFixedWidth(100)
        self.accident_thumb_scroll.setFixedHeight(150)
        self.accident_thumb_scroll.setWidgetResizable(True)
        self.accident_thumb_widget = QWidget()
        self.accident_thumb_layout = QVBoxLayout()
        self.accident_thumb_layout.setSpacing(5)
        self.accident_thumb_widget.setLayout(self.accident_thumb_layout)
        self.accident_thumb_scroll.setWidget(self.accident_thumb_widget)

        image_top_layout = QHBoxLayout()
        image_top_layout.addWidget(self.accident_main_image)
        image_top_layout.addWidget(self.accident_thumb_scroll)

        car_id_label = QLabel("车辆ID:")
        car_id_label.setFixedWidth(60)
        self.accident_car_id_label = QLabel("-")
        self.accident_car_id_label.setWordWrap(True)
        self.accident_car_id_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.accident_car_id_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.accident_detail_items["car_id"] = self.accident_car_id_label

        car_id_layout = QHBoxLayout()
        car_id_layout.addWidget(car_id_label)
        car_id_layout.addWidget(self.accident_car_id_label)

        accident_image_layout.addLayout(image_top_layout)
        accident_image_layout.addLayout(car_id_layout)

        accident_main_layout.addLayout(accident_info_layout)
        accident_main_layout.addLayout(accident_image_layout)
        self.accident_detail_widget.setLayout(accident_main_layout)
        detail_layout.addWidget(self.accident_detail_widget)

        self.used_detail_widget = QWidget()
        used_main_layout = QVBoxLayout()
        used_main_layout.setContentsMargins(5, 5, 5, 5)

        used_info_layout = QVBoxLayout()
        used_info_layout.setSpacing(5)
        used_label = QLabel("<b>车辆详情2</b>")
        used_label.setStyleSheet("font-size: 14px; font-weight: bold;")
        used_info_layout.addWidget(used_label)

        self.used_detail_items = {}
        used_fields = [
            ("id", "ID"), ("che_liang_pin_pai", "品牌"), ("xuan_ze_zi_xi_lie", "车型"),
            ("chu_chang_ri_qi", "出厂年份"), ("chesunyuanyin", "车损原因"), ("yi_kou_jia", "起拍价"),
            ("zui_xin_chu_jia", "最新出价"), ("is_xin_neng_yuan", "是否新能源"),
            ("gu_jia_ping_ji", "估价评级"), ("wai_guan_ping_ji", "外观评级"),
            ("pai_mai_hui_start_time", "拍卖开始时间"), ("pai_mai_jie_shu_date", "拍卖结束时间")
        ]
        for key, label in used_fields:
            row = QHBoxLayout()
            label_widget = QLabel(f"{label}:")
            label_widget.setFixedWidth(100)
            label_widget.setAlignment(Qt.AlignTop)
            row.addWidget(label_widget)
            value_label = QLabel("-")
            value_label.setWordWrap(True)
            value_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
            value_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
            self.used_detail_items[key] = value_label
            row.addWidget(value_label)
            used_info_layout.addLayout(row)

        used_image_layout = QVBoxLayout()
        self.used_main_image = QLabel()
        self.used_main_image.setAlignment(Qt.AlignLeft)
        self.used_main_image.setFixedSize(250, 150)
        self.used_main_image.setScaledContents(True)
        self.used_main_image.setStyleSheet("background-color: #f0f0f0; border: 1px solid #ccc;")
        self.used_main_image.setCursor(Qt.PointingHandCursor)
        self.used_main_image.mousePressEvent = lambda e: self.show_image_popup("used")

        self.used_thumb_scroll = QScrollArea()
        self.used_thumb_scroll.setFixedWidth(100)
        self.used_thumb_scroll.setFixedHeight(150)
        self.used_thumb_scroll.setWidgetResizable(True)
        self.used_thumb_widget = QWidget()
        self.used_thumb_layout = QVBoxLayout()
        self.used_thumb_layout.setSpacing(5)
        self.used_thumb_widget.setLayout(self.used_thumb_layout)
        self.used_thumb_scroll.setWidget(self.used_thumb_widget)

        image_top_layout = QHBoxLayout()
        image_top_layout.addWidget(self.used_main_image)
        image_top_layout.addWidget(self.used_thumb_scroll)

        car_id_label = QLabel("车辆ID:")
        car_id_label.setFixedWidth(60)
        self.used_car_id_label = QLabel("-")
        self.used_car_id_label.setWordWrap(True)
        self.used_car_id_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.used_car_id_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.used_detail_items["car_id"] = self.used_car_id_label

        car_id_layout = QHBoxLayout()
        car_id_layout.addWidget(car_id_label)
        car_id_layout.addWidget(self.used_car_id_label)

        used_image_layout.addLayout(image_top_layout)
        used_image_layout.addLayout(car_id_layout)

        used_main_layout.addLayout(used_info_layout)
        used_main_layout.addLayout(used_image_layout)
        self.used_detail_widget.setLayout(used_main_layout)
        detail_layout.addWidget(self.used_detail_widget)

        detail_group.setLayout(detail_layout)
        right_layout.addWidget(detail_group)

        similar_group = QGroupBox("相似车辆参考")
        similar_layout = QVBoxLayout()
        self.similar_table = QTableWidget()
        self.similar_table.setColumnCount(9)
        self.similar_table.setHorizontalHeaderLabels(["ID", "品牌", "车型", "出厂年份", "起拍价", "最新出价", "车损原因", "估价评级", "外观评级"])
        self.similar_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.similar_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.similar_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.similar_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.similar_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self.similar_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.similar_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self.similar_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.Stretch)
        self.similar_table.horizontalHeader().setSectionResizeMode(6, QHeaderView.Stretch)
        self.similar_table.horizontalHeader().setSectionResizeMode(7, QHeaderView.ResizeToContents)
        self.similar_table.horizontalHeader().setSectionResizeMode(8, QHeaderView.ResizeToContents)
        self.similar_table.setStyleSheet("QHeaderView::section { font-weight: bold; }")
        self.similar_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.similar_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.similar_table.cellClicked.connect(self.on_similar_car_selected)
        similar_layout.addWidget(self.similar_table)

        self.similar_page_layout = QHBoxLayout()
        self.similar_page_label = QLabel("第 1 页 / 共 1 页")
        self.similar_page_layout.addWidget(self.similar_page_label)
        
        self.similar_prev_btn = QPushButton("上一页")
        self.similar_prev_btn.clicked.connect(self.prev_similar_page)
        self.similar_page_layout.addWidget(self.similar_prev_btn)
        
        self.similar_next_btn = QPushButton("下一页")
        self.similar_next_btn.clicked.connect(self.next_similar_page)
        self.similar_page_layout.addWidget(self.similar_next_btn)
        
        similar_layout.addLayout(self.similar_page_layout)
        similar_group.setLayout(similar_layout)
        right_layout.addWidget(similar_group)

        right_panel.setLayout(right_layout)

        main_layout.setStretchFactor(left_panel, 2)
        main_layout.setStretchFactor(right_panel, 1)
        main_layout.addWidget(left_panel)
        main_layout.addWidget(right_panel)
        self.setLayout(main_layout)

    def load_brands(self):
        brands = ["丰田", "本田", "日产", "大众", "宝马", "奔驰", "奥迪", "别克", "福特", "雪佛兰",
                  "现代", "起亚", "马自达", "三菱", "铃木", "雷克萨斯", "沃尔沃", "路虎", "捷豹", "保时捷"]
        for b in brands:
            self.brand_input.addItem(b)

    def search_cars(self):
        car_type = self.car_type.currentText()
        brand = self.brand_input.currentText().strip()
        model = self.model_input.currentText().strip()

        where_clauses = []
        params = {}

        if brand:
            where_clauses.append("che_liang_pin_pai LIKE :brand")
            params["brand"] = f"%{brand}%"
        if model:
            where_clauses.append("xuan_ze_zi_xi_lie LIKE :model")
            params["model"] = f"%{model}%"
        try:
            year_from_val = int(self.year_from.text()) if self.year_from.text().strip() else None
            year_to_val = int(self.year_to.text()) if self.year_to.text().strip() else None
            if year_from_val and year_to_val:
                where_clauses.append("chu_chang_ri_qi BETWEEN :year_from AND :year_to")
                params["year_from"] = year_from_val
                params["year_to"] = year_to_val
            elif not year_from_val and year_to_val:
                where_clauses.append("chu_chang_ri_qi <= :year_to")
                params["year_to"] = year_to_val
            elif year_from_val and not year_to_val:
                where_clauses.append("chu_chang_ri_qi >= :year_from")
                params["year_from"] = year_from_val
        except:
            pass
        if self.price_min.text().strip():
            where_clauses.append("yi_kou_jia >= :price_min")
            params["price_min"] = float(self.price_min.text().strip())
        if self.price_max.text().strip():
            where_clauses.append("yi_kou_jia <= :price_max")
            params["price_max"] = float(self.price_max.text().strip())
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        self.where_sql = where_sql
        self.search_params = params

        table = "accident_car" if car_type == "事故车" else "used_car"
        self.total_count = self.db.query_one(f"SELECT COUNT(*) as cnt FROM {table} WHERE {where_sql}", params)["cnt"]
        self.current_page = 0
        self.load_page()

    def load_page(self):
        car_type = self.car_type.currentText()
        table = "accident_car" if car_type == "事故车" else "used_car"
        
        params = self.search_params.copy()
        params["limit"] = self.page_size
        params["offset"] = self.current_page * self.page_size
        
        self.current_cars = self.db.query(
            f"SELECT * FROM {table} WHERE {self.where_sql} ORDER BY id DESC LIMIT :limit OFFSET :offset", params
        )

        self.car_list.setRowCount(len(self.current_cars))
        for i, car in enumerate(self.current_cars):
            self.car_list.setItem(i, 0, QTableWidgetItem(str(car.get("id", ""))))
            self.car_list.setItem(i, 1, QTableWidgetItem(car.get("che_liang_pin_pai", "")))
            self.car_list.setItem(i, 2, QTableWidgetItem(car.get("xuan_ze_zi_xi_lie", "")))
            self.car_list.setItem(i, 3, QTableWidgetItem(str(car.get("chu_chang_ri_qi", ""))))
            self.car_list.setItem(i, 4, QTableWidgetItem(f"¥{car.get('yi_kou_jia', 0)}"))
            self.car_list.setItem(i, 5, QTableWidgetItem(f"¥{car.get('zui_xin_chu_jia', 0)}"))
            self.car_list.setItem(i, 6, QTableWidgetItem(car.get("pai_mai_hui_start_time", "")))
            is_xny = "是" if car.get("is_xin_neng_yuan") == 1 else "否"
            self.car_list.setItem(i, 7, QTableWidgetItem(is_xny))
            self.car_list.item(i, 0).setData(Qt.UserRole, car)

        total_pages = max(1, (self.total_count + self.page_size - 1) // self.page_size)
        self.page_label.setText(f"第 {self.current_page + 1} 页 / 共 {total_pages} 页 (共 {self.total_count} 条)")
        self.prev_btn.setEnabled(self.current_page > 0)
        self.next_btn.setEnabled(self.current_page < total_pages - 1)

        if not self.current_cars and self.total_count == 0:
            QMessageBox.information(self, "提示",  "未找到匹配的车辆数据，请先运行爬虫任务。")

    def prev_page(self):
        if self.current_page > 0:
            self.current_page -= 1
            self.load_page()

    def next_page(self):
        total_pages = (self.total_count + self.page_size - 1) // self.page_size
        if self.current_page < total_pages - 1:
            self.current_page += 1
            self.load_page()

    def parse_date(self, value):
        if not value:
            return "-"
        if isinstance(value, str) and value.startswith("/Date("):
            try:
                import re
                match = re.search(r"/Date\((\d+)\)/", value)
                if match:
                    import time
                    timestamp = int(match.group(1)) / 1000
                    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
            except:
                pass
        return str(value)

    def on_car_selected(self, row, col):
        if row >= len(self.current_cars):
            return
        car = self.current_cars[row]
        if not car:
            return

        car_type = self.car_type.currentText()

        if car_type == "事故车":
            for key in self.used_detail_items:
                self.used_detail_items[key].setText("-")
            self.used_main_image.setText("无图片")
            while self.used_thumb_layout.count():
                w = self.used_thumb_layout.takeAt(0).widget()
                if w: w.deleteLater()

            for key in self.accident_detail_items:
                self.accident_detail_items[key].setText("")

            self.load_car_images(car, "accident")

            for key, label in self.accident_detail_items.items():
                if key == "id":
                    value = str(car.get("id", "-"))
                elif key == "yi_kou_jia":
                    value = f"¥{car.get('yi_kou_jia', 0):,.2f}"
                elif key == "is_xin_neng_yuan":
                    value = "是" if car.get("is_xin_neng_yuan") == 1 else "否"
                elif key in ("pai_mai_hui_start_time", "pai_mai_jie_shu_date"):
                    value = self.parse_date(car.get(key, ""))
                else:
                    value = str(car.get(key, "-"))
                self.accident_detail_items[key].setText(value)
        else:
            for key in self.accident_detail_items:
                self.accident_detail_items[key].setText("-")
            self.accident_main_image.setText("无图片")
            while self.accident_thumb_layout.count():
                w = self.accident_thumb_layout.takeAt(0).widget()
                if w: w.deleteLater()

            for key in self.used_detail_items:
                self.used_detail_items[key].setText("")

            self.load_car_images(car, "used")

            for key, label in self.used_detail_items.items():
                if key == "id":
                    value = str(car.get("id", "-"))
                elif key == "yi_kou_jia":
                    value = f"¥{car.get('yi_kou_jia', 0):,.2f}"
                elif key == "is_xin_neng_yuan":
                    value = "是" if car.get("is_xin_neng_yuan") == 1 else "否"
                elif key in ("pai_mai_hui_start_time", "pai_mai_jie_shu_date"):
                    value = self.parse_date(car.get(key, ""))
                else:
                    value = str(car.get(key, "-"))
                self.used_detail_items[key].setText(value)

        brand = car.get("che_liang_pin_pai", "")
        model = car.get("xuan_ze_zi_xi_lie", "")
        year = car.get("chu_chang_ri_qi", 0)
        car_id = car.get("car_id", "")

        if not brand:
            self.similar_cars = []
            self.similar_table.setRowCount(0)
            self.similar_total_count = 0
            self.update_similar_page_label()
            return

        car_type = self.car_type.currentText()
        self.cur_table = "accident_car" if car_type == "事故车" else "used_car"

        self.similar_where_clauses = ["che_liang_pin_pai LIKE :brand"]
        self.similar_params = {"brand": f"%{brand}%"}
        if model:
            self.similar_where_clauses.append("xuan_ze_zi_xi_lie LIKE :model")
            self.similar_params["model"] = f"%{model}%"
        if car_id:
            self.similar_where_clauses.append("car_id != :car_id")
            self.similar_params["car_id"] = car_id

        self.similar_where_sql = " AND ".join(self.similar_where_clauses)
        
        self.similar_total_count = self.db.query_one(
            f"SELECT COUNT(*) as cnt FROM {self.cur_table} WHERE {self.similar_where_sql}", self.similar_params
        )["cnt"]

        self.similar_current_page = 0
        self.load_similar_page()

    def load_car_images(self, car, car_type):
        # 1. 定义配置映射，消除重复代码
        config_map = {
            "accident": {
                "main": self.accident_main_image,
                "thumb_layout": self.accident_thumb_layout
            },
            "used": {
                "main": self.used_main_image,
                "thumb_layout": self.used_thumb_layout
            }
        }

        if car_type not in config_map:
            return

        ui = config_map[car_type]
        main_image = ui["main"]
        thumb_layout = ui["thumb_layout"]

        # 2. 清理旧数据
        main_image.clear()
        main_image.setText("无图片")

        while thumb_layout.count():
            item = thumb_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        # 3. 准备路径
        car_id = car.get("car_id", "")
        if not car_id:
            return

        data_path = config.get_data_path()
        image_dir = os.path.join(data_path, "images", str(car_id))

        # 检查目录是否存在
        if not os.path.exists(image_dir):
            main_image.setText("图片目录不存在")
            return

        # 4. 【核心修改】直接扫描目录下的所有文件
        # 获取目录下所有文件名
        all_files = os.listdir(image_dir)
        # 简单的图片格式白名单过滤
        valid_images = [f for f in all_files if f.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif'))]

        # 排序（可选）：让图片按文件名顺序显示，比如 1.jpg, 2.jpg...
        valid_images.sort()

        if not valid_images:
            main_image.setText("目录中无图片")
            return

        first_image_loaded = False

        # 5. 遍历生成缩略图
        for filename in valid_images:
            # 拼接完整路径
            img_path = os.path.join(image_dir, filename)

            # 确保是文件而不是子目录
            if not os.path.isfile(img_path):
                continue

            # 尝试加载图片
            pixmap = QPixmap(img_path)
            if pixmap.isNull():
                continue  # 跳过损坏的文件

            # --- 生成缩略图 ---
            thumb_pixmap = pixmap.scaled(60, 60, Qt.KeepAspectRatio, Qt.SmoothTransformation)

            thumb_label = QLabel()
            thumb_label.setPixmap(thumb_pixmap)
            thumb_label.setCursor(Qt.PointingHandCursor)
            thumb_label.setStyleSheet("border: 1px solid #ccc; padding: 2px;")

            # 绑定点击事件
            thumb_label.mousePressEvent = lambda e, path=img_path, m=main_image: self.show_main_image(path, m)

            thumb_layout.addWidget(thumb_label)

            # --- 设置第一张为主图 ---
            if not first_image_loaded:
                main_pixmap = pixmap.scaled(250, 200, Qt.KeepAspectRatio, Qt.SmoothTransformation)
                main_image.setPixmap(main_pixmap)
                main_image.setText("")  # 清除"无图片"文字
                first_image_loaded = True

    def show_main_image(self, image_path, main_image):
        main_image.setPixmap(
            QPixmap(image_path).scaled(250, 200, Qt.KeepAspectRatio, Qt.SmoothTransformation))

    def load_similar_page(self):
        params = self.similar_params.copy()
        params["limit"] = self.page_size
        params["offset"] = self.similar_current_page * self.page_size
        
        self.similar_cars = self.db.query(
            f"SELECT * FROM {self.cur_table} WHERE {self.similar_where_sql} ORDER BY id DESC LIMIT :limit OFFSET :offset", params
        )

        self.similar_table.setRowCount(len(self.similar_cars))
        for i, sc in enumerate(self.similar_cars):
            self.similar_table.setItem(i, 0, QTableWidgetItem(str(sc.get("id", ""))))
            self.similar_table.setItem(i, 1, QTableWidgetItem(sc.get("che_liang_pin_pai", "")))
            self.similar_table.setItem(i, 2, QTableWidgetItem(sc.get("xuan_ze_zi_xi_lie", "")))
            self.similar_table.setItem(i, 3, QTableWidgetItem(str(sc.get("chu_chang_ri_qi", ""))))
            self.similar_table.setItem(i, 4, QTableWidgetItem(f"¥{sc.get('yi_kou_jia', 0)}"))
            self.similar_table.setItem(i, 5, QTableWidgetItem(f"¥{sc.get('zui_xin_chu_jia', 0)}"))
            self.similar_table.setItem(i, 6, QTableWidgetItem(sc.get("chesunyuanyin", "")))
            self.similar_table.setItem(i, 7, QTableWidgetItem(sc.get("gu_jia_ping_ji", "")))
            self.similar_table.setItem(i, 8, QTableWidgetItem(sc.get("wai_guan_ping_ji", "")))
            self.similar_table.setItem(i, 7, QTableWidgetItem(sc.get("wai_guan_ping_ji", "")))

        self.update_similar_page_label()

    def update_similar_page_label(self):
        total_pages = max(1, (self.similar_total_count + self.page_size - 1) // self.page_size)
        self.similar_page_label.setText(f"第 {self.similar_current_page + 1} 页 / 共 {total_pages} 页 (共 {self.similar_total_count} 条)")
        self.similar_prev_btn.setEnabled(self.similar_current_page > 0)
        self.similar_next_btn.setEnabled(self.similar_current_page < total_pages - 1)

    def prev_similar_page(self):
        if self.similar_current_page > 0:
            self.similar_current_page -= 1
            self.load_similar_page()

    def next_similar_page(self):
        total_pages = (self.similar_total_count + self.page_size - 1) // self.page_size
        if self.similar_current_page < total_pages - 1:
            self.similar_current_page += 1
            self.load_similar_page()

    def on_similar_car_selected(self, row, col):
        if row >= len(self.similar_cars):
            return
        car = self.similar_cars[row]
        if not car:
            return

        for key, label in self.used_detail_items.items():
            if key == "id":
                value = str(car.get("id", "-"))
            elif key == "yi_kou_jia":
                value = f"¥{car.get('yi_kou_jia', 0):,.2f}"
            elif key == "is_xin_neng_yuan":
                value = "是" if car.get("is_xin_neng_yuan") == 1 else "否"
            elif key in ("pai_mai_hui_start_time", "pai_mai_jie_shu_date"):
                value = self.parse_date(car.get(key, ""))
            else:
                value = str(car.get(key, "-"))
            self.used_detail_items[key].setText(value)

        self.load_car_images(car, "used")

    def show_image_popup(self, car_type):
        if car_type == "accident":
            pixmap = self.accident_main_image.pixmap()
            car_id = self.accident_car_id_label.text()
        else:
            pixmap = self.used_main_image.pixmap()
            car_id = self.used_car_id_label.text()

        if pixmap is None or pixmap.isNull():
            return

        self.current_zoom = 1.0
        self._original_pixmap = pixmap

        dialog = QDialog(self)
        dialog.setWindowTitle(f"车辆图片 - {car_id}")
        dialog.setMinimumSize(600, 450)
        
        layout = QVBoxLayout()
        
        self.popup_scroll = QScrollArea()
        self.popup_scroll.setWidgetResizable(True)
        self.popup_image_label = QLabel()
        self._current_pixmap = pixmap.scaled(580, 400, Qt.KeepAspectRatio, Qt.SmoothTransformation)
        self.popup_image_label.setPixmap(self._current_pixmap)
        self.popup_image_label.setAlignment(Qt.AlignCenter)
        self.popup_scroll.setWidget(self.popup_image_label)
        layout.addWidget(self.popup_scroll)
        
        btn_layout = QHBoxLayout()
        
        zoom_in_btn = QPushButton("放大")
        zoom_in_btn.clicked.connect(lambda: self.zoom_image(1.2))
        btn_layout.addWidget(zoom_in_btn)
        
        zoom_out_btn = QPushButton("缩小")
        zoom_out_btn.clicked.connect(lambda: self.zoom_image(0.8))
        btn_layout.addWidget(zoom_out_btn)
        
        reset_btn = QPushButton("重置")
        reset_btn.clicked.connect(lambda: self.zoom_image(0, True))
        btn_layout.addWidget(reset_btn)
        
        btn_layout.addStretch()
        
        close_btn = QPushButton("关闭")
        close_btn.clicked.connect(dialog.close)
        btn_layout.addWidget(close_btn)
        
        layout.addLayout(btn_layout)
        dialog.setLayout(layout)
        self.popup_dialog = dialog
        dialog.exec_()

    def zoom_image(self, factor, reset=False):
        if not hasattr(self, '_original_pixmap') or not self._original_pixmap:
            return
            
        if reset:
            self.current_zoom = 1.0
            self._current_pixmap = self._original_pixmap
        else:
            self.current_zoom *= factor
            if self.current_zoom < 0.1:
                self.current_zoom = 0.1
            if self.current_zoom > 5.0:
                self.current_zoom = 5.0
        
        if hasattr(self, 'popup_image_label'):
            scaled = self._current_pixmap.scaled(
                int(self._current_pixmap.width() * self.current_zoom),
                int(self._current_pixmap.height() * self.current_zoom),
                Qt.KeepAspectRatio,
                Qt.SmoothTransformation
            )
            self.popup_image_label.setPixmap(scaled)
