import json
import threading
import time
import websocket
import ssl

from core import get_logger
from db.database import Database

logger = get_logger()


class AuctionMonitor:
    def __init__(self, db, car_type, prior_end_time, session_id, pai_mai_id):
        self.db = db
        self.car_type = car_type  # accident  used
        if prior_end_time is None:
            self.prior_end_time = time.time() + 86400
        else:
            self.prior_end_time = int(prior_end_time) / 1000.0

        # --- 1. 登录凭证 (请确保这里有值) ---
        self.session_id = session_id
        self.pai_mai_id = pai_mai_id  # 也就是 auctionID

        # --- 2. 构建 URL (完全模仿 JS 代码) ---
        # 注意：这里直接把参数拼接到 URL 后面
        # 假设 socketUrl 变量是 wss://auctionhub01.bochewang.cn:9505
        base_url = "wss://auctionhub01.bochewang.cn:9505"
        self.ws_url = f"{base_url}?auctionID={self.pai_mai_id}&sessionID={self.session_id}&deviceType=10"
        logger.info(f"🔗 目标连接地址: {self.ws_url}")

        # --- 3. 运行状态 ---
        self.ws = None
        self.is_running = False

    # ================= 核心：连接建立 =================
    def on_open(self, ws):
        logger.info("✅ WebSocket 连接已建立！")
        self._start_heartbeat()

    # ================= 核心：心跳保活 =================
    def _start_heartbeat(self):
        def heartbeat_loop():
            while self.is_running:
                try:
                    # 检测拍卖会是否结束
                    current_time = time.time()
                    if current_time >= self.prior_end_time:
                        logger.info(f"⏰ 时间到，关闭连接并停止心跳！当前时间 {int(current_time)} >= 结束时间 {int(self.prior_end_time)}")
                        self.is_running = False
                        if self.ws:
                            self.ws.close()
                        break

                    # 发送底层 Ping 帧 (协议层保活)
                    if self.ws:
                        self.ws.ping()
                except:
                    break
                time.sleep(20)
        t = threading.Thread(target=heartbeat_loop, daemon=True)
        t.start()

    # ================= 核心：接收消息 =================
    def on_message(self, ws, message):
        try:
            logger.info(f"【当前价】接收到消息，message={message}")
            data = json.loads(message)
            data_type = data.get('type', None)
            if data_type != 2:
                return
            res_data = json.loads(data['data'] if 'data' in data else data)
            message_type = res_data.get("messageType", None)

            # 判断是不是竞价消息 1
            if message_type == 1:
                car_id = res_data.get("vehicleid")
                zui_xin_chu_jia = res_data.get("price")
                # logger.info(f"【当前价】车辆={car_id}，最新价格={zui_xin_chu_jia}")

                if car_id and zui_xin_chu_jia:
                    update_data = {
                        "zui_xin_chu_jia": int(zui_xin_chu_jia)
                    }
                    params = {
                        "car_id": car_id
                    }
                    table = "accident_car" if self.car_type == "accident" else "used_car"
                    try:
                        self.db.update(table, update_data, 'car_id = :car_id', params)
                    except Exception as e:
                        logger.error(f"【当前价】更新失败。车辆={car_id}，最新价格={zui_xin_chu_jia}", e)
            elif message_type == 7 or message_type == 8:
                self.prior_end_time = time.time()
        except Exception as e:
            logger.error(f"【当前价】消息处理失败。message={message}", e)

    def on_error(self, ws, error):
        logger.error(f"❌ 错误: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.info(f"🔌 连接关闭: {close_status_code}")
        self.is_running = False

    # ================= 启动入口 =================
    def start(self):
        self.is_running = True

        # 关键配置：忽略 SSL 证书验证（因为是非标准端口 9505）
        self.ws = websocket.WebSocketApp(self.ws_url,
                                         on_open=self.on_open,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)

        # 启动连接
        self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})


if __name__ == "__main__":
    _db = Database()
    _session_id = "fa2fd946-f1ef-4e02-9bd2-9801ee506283"
    #  # 2379605c-71bd-40c1-9e1a-a0f8161c2482  07d5bca1-7bdd-4c4e-a46e-b70046390cf5
    _pai_mai_id = "07d5bca1-7bdd-4c4e-a46e-b70046390cf5"
    monitor = AuctionMonitor(_db, "accident", None, _session_id, _pai_mai_id)
    monitor.start()


