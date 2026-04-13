import cv2
import numpy as np
import time
import random

import requests

from core.logger import get_logger


logger = get_logger("captcha")


def download_image(url):
    """
    下载网络图片并返回二进制数据
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            return resp.content
        else:
            logger.warning(f"图片下载失败: {resp.status_code}")
            return None
    except Exception as e:
        logger.warning(f"下载异常: {e}")
        return None


def get_distance(bg_img_path, start_y, end_y, slide_img_path):
    """
    使用 OpenCV 模板匹配计算缺口距离
    :param bg_img_path: 背景图路径 (带缺口)
    :param start_y: 背景图开始高度
    :param end_y: 背景图结束高度
    :param slide_img_path: 滑块图路径 (凸起部分)
    :return: 缺口距离 (像素)
    """
    # 1. 读取图片
    bg_img_bytes = download_image(bg_img_path)
    bg_img = cv2.imdecode(np.frombuffer(bg_img_bytes, np.uint8), cv2.IMREAD_COLOR)
    bg_img_slice = bg_img[start_y: end_y, :]

    slide_img_bytes = download_image(slide_img_path)
    slide_img = cv2.imdecode(np.frombuffer(slide_img_bytes, np.uint8), cv2.IMREAD_COLOR)

    # 2. 预处理：转为灰度图
    bg_gray_slice = cv2.cvtColor(bg_img_slice, cv2.COLOR_BGR2GRAY)
    bg_edge_slice = cv2.Canny(bg_gray_slice, 30, 100)

    slide_gray = cv2.cvtColor(slide_img, cv2.COLOR_BGR2GRAY)
    slide_edge = cv2.Canny(slide_gray, 30, 100)

    # 4. 模板匹配
    # 在背景图中寻找滑块图的边缘
    # cv2.TM_CCOEFF_NORMED 是相关系数匹配，结果越接近1越相似
    res = cv2.matchTemplate(bg_img_slice, slide_img, cv2.TM_CCORR_NORMED)

    # 获取匹配度最高的位置
    min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(res)

    # max_loc 是一个元组 (x, y)，我们只需要 x (水平距离)
    distance = max_loc[0]

    return distance


def find_gap_by_histogram(bg_img_path, start_y, end_y, slide_img_path):
    """
    通过按列直方图统计寻找缺口
    逻辑：缺口处通常是纯色（如白色），统计每一列的纯色像素数量，
    如果某一段连续区域的纯色像素高度等于滑块高度，则判定为缺口。
    """
    # 1. 解码图片
    bg_img_bytes = download_image(bg_img_path)
    bg_img = cv2.imdecode(np.frombuffer(bg_img_bytes, np.uint8), cv2.IMREAD_COLOR)
    bg_img_slice = bg_img[start_y: end_y, :]

    slide_img_bytes = download_image(slide_img_path)
    slide_img = cv2.imdecode(np.frombuffer(slide_img_bytes, np.uint8), cv2.IMREAD_COLOR)
    slide_h, slide_w = slide_img.shape[:2]

    # 2. 处理背景图：提取“白色/纯色”区域
    bg_gray_slice = cv2.cvtColor(bg_img_slice, cv2.COLOR_BGR2GRAY)
    _, mask = cv2.threshold(bg_gray_slice, 240, 255, cv2.THRESH_BINARY)  # 缺口变成白色(255)，背景变成黑色(0)
    h, w = mask.shape

    col_counts = []
    for x in range(w):
        col_slice = mask[:, x]
        count = cv2.countNonZero(col_slice)
        col_counts.append(count)
    col_counts = np.array(col_counts)

    # 3. 寻找连续的白色区域

    # 找到所有满足条件的列的索引
    height_threshold = h * 0.9
    is_white_col = col_counts >= height_threshold

    # 我们需要找到一段连续的 True，且长度接近滑块宽度
    final_x = -1
    i = 0
    while i < w:
        if is_white_col[i]:
            # 发现了一个白色区域的起点
            start_x = i
            count = 0

            # 向后扫描，直到遇到非白色列或到达边界
            while i < w and is_white_col[i]:
                count += 1
                i += 1
            end_x = start_x + count - 1

            # 缺口宽度 ≈ 滑块宽度 (最常见)
            # 允许一点误差，比如滑块宽度的 0.9 到 1.1 倍
            if slide_w * 0.9 <= count <= slide_w * 1.1:
                logger.info(f"✅ 找到匹配缺口! 起点: {start_x}, 宽度: {count}, 滑块宽: {slide_w}")
                final_x = start_x
                break  # 找到第一个符合宽度的就返回
        else:
            i += 1

    if final_x == -1:
        logger.warning(f"❌ 未找到宽度匹配的缺口 (期望宽度: {slide_w})")

    return final_x


def get_track(distance):
    """
    模拟人类滑动轨迹
    :param distance: 缺口距离
    :return: 轨迹列表 (每一步移动的距离)
    """
    track = []
    current_x = 0
    # 随机初始速度
    current_status = 0

    # 简单的物理模拟：先加速，后减速
    # 假设总时间随机
    total_time = random.uniform(0.8, 1.5)
    mid_time = total_time * 0.8  # 80%的时间用来加速和匀速，20%用来减速

    start_time = time.time()

    while current_x < distance:
        # 计算当前时间
        t = time.time() - start_time

        if t < mid_time:
            # 加速阶段
            move = random.uniform(2, 5)
        else:
            # 减速阶段
            move = random.uniform(0.5, 2)

        track.append(int(move))
        current_x += move

        # 防止 overshoot (滑过头)
        if current_x > distance:
            track.append(int(distance - current_x))
            break

        time.sleep(random.uniform(0.02, 0.05))  # 模拟人手操作的微小停顿

    return track