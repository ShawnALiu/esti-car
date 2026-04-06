# 估车侠 EstCar

一款基于 PyQt5 的 Windows 桌面应用程序，用于抓取拍车网站的车辆拍卖信息，帮助用户快速获取事故车和二手车的市场报价，方便用户出价参考。

## 功能特性

### Tab1: 车辆估价

- 支持按品牌、型号、年份、价格范围搜索事故车和二手车
- 左侧展示搜索结果列表，右侧显示车辆详情与参考报价
- 点击具体车辆，自动在右侧报价栏显示同条件二手车的报价信息

### Tab2: 账号配置

- 管理爬虫目标网站的账号信息（网站名、网站地址、账号、密码）
- 表格形式展示，支持自增序号
- 支持新增、修改、删除操作
- 数据持久化保存在内置 SQLite 数据库中

### Tab3: 任务配置

- **创建任务**：历史任务列表分页展示（每页20条），支持启用/停用切换
- **创建弹窗**：任务名、任务类型（事故车爬取/二手车爬取）、选择账号配置、拍卖时间、最多数量、执行方式（手动/定时Cron）
- **存活任务**：实时显示当前正在执行的任务，每5秒自动刷新
- **历史任务**：展示历史执行记录，包含开始时间、结束时间、执行状态（成功/失败）

### Tab4: 数据管理

- **数据统计**：查看各数据表的记录数和最后更新时间
- **数据清理**：按日期清理指定数据表的历史数据

## 技术栈

- **Python 3.9+**
- **PyQt5** - GUI 框架
- **requests** - HTTP 请求
- **beautifulsoup4** - HTML 解析
- **APScheduler** - 定时任务调度
- **SQLite** - 本地数据库

## 快速开始

### 环境要求

- Windows 10/11
- Python 3.9 或更高版本

### 安装依赖

```bash
pip install -r requirements.txt
```

### 运行程序

```bash
python main.py
```

## 打包为独立 exe

```bash
pip install pyinstaller
pyinstaller build.spec
```

打包完成后，在 `dist/估车侠EstCar/` 目录下生成可独立运行的程序，无需安装 Python 环境即可在其他 Windows 电脑上使用。

## 项目结构

```
esti-car/
├── main.py                 # 程序入口
├── requirements.txt        # Python 依赖
├── build.spec              # PyInstaller 打包配置
├── db/
│   └── database.py         # SQLite 数据库管理（自动迁移）
├── core/
│   └── task_executor.py    # 任务执行器（含定时调度）
├── crawler/
│   └── car_crawler.py      # 爬虫模块（事故车/二手车）
└── ui/
    ├── main_window.py       # 主窗口
    ├── tab_valuation.py     # 车辆估价
    ├── tab_account.py       # 账号配置
    ├── tab_task.py          # 任务配置
    └── tab_data.py          # 数据管理
```

## 数据库

数据保存在用户目录 `C:\Users\<用户名>\.esticar\esticar.db`，包含以下表：

| 表名 | 说明 |
|------|------|
| account_config | 账号配置表 |
| task | 任务表 |
| task_execution | 任务执行记录表 |
| accident_car | 事故车数据表 |
| used_car | 二手车数据表 |

## 爬虫适配

`crawler/car_crawler.py` 提供了通用爬虫基类，需要根据实际目标网站修改 CSS 选择器。

`crawler/boche_crawler.py` 是博车网专用爬虫，支持：

- `BoCheCrawler` - API 方式爬取（需登录）
  - `login()` - 账号登录
  - `get_accident_cars()` - 获取事故车列表
  - `get_used_cars()` - 获取二手车列表
  - `get_auction_list()` - 获取拍卖会列表
  - `get_car_detail()` - 获取车辆详情

- `BoCheHtmlCrawler` - HTML 方式爬取（备用）

根据目标网站的实际 HTML 结构修改 CSS 选择器即可快速适配。

### 博车网配置示例

在「账号配置」中添加：

- 网站名：博车网
- 网站地址：https://appservice.bochewang.com.cn
- 账号：您的博车网账号
- 密码：您的博车网密码

系统会自动识别博车网并使用专用爬虫。

## License

MIT
