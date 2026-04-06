import requests
import json
import re
import time
from datetime import datetime
from crawler.car_crawler import BaseCrawler
import uuid

from utils import captcha_util


class BoCheCrawler(BaseCrawler):
    
    def __init__(self, site_name="博车网", base_url=None, username=None, password=None):
        super().__init__(site_name, base_url, username, password)
        self.session = requests.Session()

        self.session_id = None
        self.user_id = None
        self.pai_mai_id = None
        self.user_type = None
        self.mai_jia_zhuang_tai = None
        self.jiao_fei_deng_ji = None
        self.zi_zhi_zhuang_tai = None
        
        self.car_wins_session_id = None
        self.car_wins_session_key = None
        self.car_wins_user_id = None
        self.institution_id = None
        
        self.device_id = str(uuid.uuid4())

    def update_credentials(self, base_url, username, password):
        self.base_url = base_url
        self.username = username
        self.password = password


    def pre_login(self):
        if not self.username or not self.password:
            return None
        try:
            # 设置通用的请求头 (模拟手机或电脑浏览器)
            self.session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0',
                'Referer': self.base_url
            })

            # --- 第一步：GET获取验证码图片信息 (GetCaptchaImage) ---
            captcha_url = f"{self.base_url}/HttpService/GetCaptchaImage"
            captcha_resp = self.session.get(captcha_url)
            resp_json = captcha_resp.json()
            if not resp_json['Succeed']:
                raise Exception(f"获取验证码失败: {resp_json['Message']}")

            # 提取数据
            self.token = resp_json['Data']['token']
            new_pic_path = resp_json['Data']['images']['NewPicPath']
            small_pic_path = resp_json['Data']['images']['SmallPicPath']
            offset_y = resp_json['Data']['images']['OffSetY']
            cut_height = resp_json['Data']['images']['CutHeight']
            offset_x = captcha_util.find_gap_by_histogram(new_pic_path, offset_y, offset_y + cut_height, small_pic_path)
            print(f"获取到 Token: {self.token}")
            print(f"获取到缺口距离 OffSetX: {offset_x}")

            # --- 第二步：GET请求发送短信验证码 (GetTelCode) ---
            sms_url = f"{self.base_url}/HttpService/GetTelCode"
            sms_params = {
                'tel': self.username,
                'type': 'mobileLogin',
                'token': self.token,
                'isXinBanYanZhengMa': '1',
                'OffSetX': offset_x,
                'deviceid': self.device_id
            }
            print(f"请求短信接口: {sms_url}?{sms_params}")
            sms_resp = self.session.get(sms_url, params=sms_params)
            sms_json = sms_resp.json()
            print("短信接口原始响应:", sms_resp.text)
            if not sms_json['Succeed']:
                raise Exception(f"发送短信失败: {sms_json.get('Message', sms_resp.text)}")
            check_code_id = sms_json['Data']['checkCodeID']
            print(f"短信发送成功，获取到 CheckCodeID: {check_code_id}")
            return check_code_id
        except Exception as e:
            print(f"博车网登录失败: {e}")
            return None

    def login(self, check_code_id=None, verify_code=None):
        if not self.username or not self.password:
            return False

        try:
            # --- 模拟登录 (UserLogin) ---
            login_url = f"{self.base_url}/HttpService/UserLogin"
            login_data = {
                'account': self.username,
                'loginType': '20',
                'checkCodeID': check_code_id,
                'validateCode': verify_code,
                'deviceid': self.device_id
            }

            # 发送 POST 请求
            login_resp = self.session.post(login_url, data=login_data)
            login_json = login_resp.json()

            if login_json['Succeed']:
                data = login_json.get("Data", {})
                
                self.session_id = data.get("SessionID")
                self.user_id = data.get("UserID")
                self.pai_mai_id = data.get("PaiMaiID")
                self.user_type = data.get("UserType")
                self.mai_jia_zhuang_tai = data.get("MaiJiaZhuangTai")
                self.jiao_fei_deng_ji = data.get("JiaoFeiDengJi")
                self.zi_zhi_zhuang_tai = data.get("ZiZhiZhuangTai")
                
                car_wins_data = data.get("carWinsData", {})
                self.car_wins_session_id = car_wins_data.get("sessionID")
                self.car_wins_session_key = car_wins_data.get("sessionKey")
                self.car_wins_user_id = car_wins_data.get("carWinsUserID")
                self.institution_id = car_wins_data.get("institutionID")

                return True
            else:
                raise Exception(f"登录失败: {login_json.get('Message', '未知错误')}")

        except Exception as e:
            print(f"博车网登录失败: {e}")
            return False

    def get_accident_cars(self, max_count=1000):
        all_cars = []
        meets = self.get_auction_meet_list("accident")

        for meet in meets:
            if len(all_cars) >= max_count:
                break
            cars = self.get_sidebar_vehicle(meet["id"])
            for item in cars:
                car = self._convert_car_to_db_format(meet["id"], item, "accident")
                all_cars.append(car)

        return all_cars[:max_count]

    def get_used_cars(self, max_count=100):
        all_cars = []
        meets = self.get_auction_meet_list("used")

        for meet in meets:
            if len(all_cars) >= max_count:
                break
            cars = self.get_auction_cars(meet["id"])
            for item in cars:
                car = self._convert_car_to_db_format(meet["id"], item, "used")
                all_cars.append(car)

        return all_cars[:max_count]

    def get_auction_meet_list(self, car_type="accident"):
        auctions = []
        try:
            server_time = self._get_server_time()
            
            if car_type == "accident":
                pai_mai_type = 10
            else:
                pai_mai_type = 20
            
            url = f"{self.base_url}/HttpService/GetPaiMaiList"
            params = {
                "SessionID": self.session_id or "",
                "UserID": self.user_id or "",
                "ServerTime": f"/Date({server_time})/",
                "UserType": self.user_type,
                "MaiJiaZhuangTai": self.mai_jia_zhuang_tai,
                "JiaoFeiDengJi": self.jiao_fei_deng_ji,
                "ZiZhiZhuangTai": self.zi_zhi_zhuang_tai,
                "deviceid": self.device_id or "",
                "paiMaiHuiLeiXing": str(pai_mai_type),
                "paiMaiZiLeiiXing": str(pai_mai_type)
            }
            
            response = self.session.get(url, params=params, timeout=5)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("Succeed"):
                    items = result.get("Data", [])
                    for item in items:
                        auctions.append({
                            "id": item.get("id", ""),
                            "name": item.get("paiMaiName", ""),
                            "title": item.get("title", ""),
                            "count": item.get("count", 0),
                            "start_date": item.get("startDate", ""),
                            "paimaihuiLeixing": item.get("paimaihuiLeixing", 0),
                        })
        except Exception as e:
            print(f"获取拍卖会列表失败: {e}")
        
        return auctions

    def get_auction_cars(self, pai_mai_id, filters=None):
        cars = []
        try:
            server_time = self._get_server_time()
            
            url = f"{self.base_url}/HttpService/SearchPaiMaiBiaoDiList"
            params = {
                "SessionID": self.session_id or "",
                "UserID": self.user_id or "",
                "PaiMaiID": pai_mai_id,
                "ServerTime": f"/Date({server_time})/",
                "UserType": self.user_type,
                "MaiJiaZhuangTai": self.mai_jia_zhuang_tai,
                "JiaoFeiDengJi": self.jiao_fei_deng_ji,
                "ZiZhiZhuangTai": self.zi_zhi_zhuang_tai,
                "deviceid": self.device_id or "",
                "type": "all",
                "query": json.dumps(filters or {}),
                "specialSearch": "",
                "orderFieldParam": ""
            }
            
            response = self.session.get(url, params=params, timeout=5)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("Succeed"):
                    data = result.get("Data", {})
                    items = data.get("list", [])
                    for item in items:
                        cars.append(item)
        except Exception as e:
            print(f"获取拍卖车辆列表失败: {e}")
        
        return cars

    def get_sidebar_vehicle(self, pai_mai_id, filters=None):
        cars = []
        url = f"{self.base_url}/HttpService/GetSidebarVehicle"
        try:
            params = {
                "SessionID": self.session_id or "",
                "UserID": self.user_id or "",
                "PaiMaiID": pai_mai_id,
                "ServerTime": f"/Date({self._get_server_time()})/",
                "UserType": self.user_type,
                "MaiJiaZhuangTai": self.mai_jia_zhuang_tai,
                "JiaoFeiDengJi": self.jiao_fei_deng_ji,
                "ZiZhiZhuangTai": self.zi_zhi_zhuang_tai,
                "deviceid": self.device_id or "",
                "type": "all",
                "query": json.dumps(filters or {}),
                "specialSearch": "",
                "orderFieldParam": ""
            }
            response = self.session.get(url, params=params, timeout=5)
            if response.status_code == 200:
                result = response.json()
                if result.get("Succeed"):
                    data = result.get("Data", {})
                    items = data.get("CarList", [])
                    for item in items:
                        cars.append(item)
        except Exception as e:
            print(f"获取侧边栏拍卖车辆列表失败: {e}")

        return cars

    def get_biao_di_info(self, pai_mai_id, biao_di_id):
        try:
            url = f"{self.base_url}/HttpService/GetBiaoDiInfo"
            params = {
                "SessionID": self.session_id or "",
                "UserID": self.user_id or "",
                "PaiMaiID": pai_mai_id,
                "ServerTime": f"/Date({self._get_server_time()})/",
                "UserType": self.user_type,
                "MaiJiaZhuangTai": self.mai_jia_zhuang_tai,
                "JiaoFeiDengJi": self.jiao_fei_deng_ji,
                "ZiZhiZhuangTai": self.zi_zhi_zhuang_tai,
                "deviceid": self.device_id or "",
                "biaoDiID": biao_di_id
            }
            response = self.session.get(url, params=params, timeout=5)

            if response.status_code == 200:
                result = response.json()
                if result.get("Succeed"):
                    data = result.get("Data", {})
                    return data
        except Exception as e:
            print(f"获取标的信息失败: {e}")
        return None

    def get_pai_pin_header_info(self, pai_mai_id, pai_pin_id):
        try:
            url = f"{self.base_url}/HttpService/GetPaiPinHeaderInfo"
            params = {
                "SessionID": self.session_id or "",
                "UserID": self.user_id or "",
                "PaiMaiID": pai_mai_id,
                "ServerTime": f"/Date({self._get_server_time()})/",
                "UserType": self.user_type,
                "MaiJiaZhuangTai": self.mai_jia_zhuang_tai,
                "JiaoFeiDengJi": self.jiao_fei_deng_ji,
                "ZiZhiZhuangTai": self.zi_zhi_zhuang_tai,
                "deviceid": self.device_id or "",
                "paiPinID": pai_pin_id
            }
            response = self.session.get(url, params=params, timeout=5)

            if response.status_code == 200:
                result = response.json()
                if result.get("Succeed"):
                    data = result.get("Data", {})
                    return data
        except Exception as e:
            print(f"获取拍品头信息失败: {e}")
        return None

    def _get_server_time(self):
        return int(time.time() * 1000)

    def _convert_car_to_db_format(self, pai_mai_id, item, car_type):
        images = [item.get("imageURL")] if item.get("imageURL") else []
        
        return {
            "pai_mai_id": pai_mai_id,
            "site_name": self.site_name,
            "detail_urls": json.dumps(images, ensure_ascii=False),
            
            "car_id": item.get("carID", ""),
            "che_liang_pin_pai": item.get("cheLiangPinPai", ""),
            "xuan_ze_xi_lie": item.get("xuanZeXiLie", ""),
            "xuan_ze_zi_xi_lie": item.get("xuanZeZiXiLie", ""),
            "pai_liang": item.get("paiLiang", ""),
            "chu_chang_ri_qi": item.get("chuChangRiQi", ""),
            "che_pai_hao": item.get("chePaiHao", ""),
            "che_liang_zan_cun_di": item.get("cheLiangZanCunDi", ""),
            "is_auction_finish": 1 if item.get("isAuctionFinish") else 0,
            "yu_zhan_shi_jian": item.get("YuZhanShiJian", ""),
            "attention": item.get("attention", ""),
            "chesunyuanyin": item.get("chesunyuanyin", ""),
            "pai_mai_hui_start_time": item.get("paiMaiHuiStartTime", ""),
            "pai_mai_hui_lei_xing": item.get("paiMaiHuiLeiXing", 0),
            "zui_xin_chu_jia": item.get("ZuiXinChuJia", ""),
            "yi_kou_jia": float(item.get("yiKouJia", 0) or 0),
            "gu_jia_ping_ji": item.get("guJiaPingJi", ""),
            "wai_guan_ping_ji": item.get("waiGuanPingJi", ""),
            "main_car": item.get("MainCar", 0),
            "is_new_chu_jia": item.get("IsNewChuJia", 0),
            "is_yi_kou_jia": item.get("IsYiKouJia", 0),
            "is_xian_pai": 1 if item.get("IsXianPai") else 0,
            "wu_zi_ming_cheng": item.get("wuZiMingCheng", ""),
            "che_liang_zhong_lei": item.get("cheLiangZhongLei", ""),
            "pei_jian_zhong_lei": item.get("peiJianZhongLei", ""),
            "pai_mai_jie_shu_date": item.get("paiMaiJieShuDate", ""),
            "vehicle_name": item.get("vehicleName", ""),
            "is_xin_neng_yuan": 1 if item.get("isXinNengYuan") else 0,
            "biao_di_type": item.get("BiaoDiType", 0),
            "image_url": item.get("imageURL", ""),
            "pai_pin_count": item.get("paiPinCount", 0),
            "wei_guan_count": item.get("weiGuanCount", 0),
            "bid_count": item.get("bidCount", 0),
        }

    def _parse_vehicle_name(self, name):
        year, brand, model = None, None, None
        match = re.match(r"(\d{4})?\s*(.+?)\s+(.+)", name or "")
        if match:
            year = match.group(1) if match.group(1) else ""
            brand = match.group(2).strip()
            model = match.group(3).strip()
        return year, brand, model

    def _parse_year(self, date_str):
        if not date_str:
            return 0
        try:
            if "/" in date_str:
                return int(date_str.split("/")[0])
            return int(date_str[:4])
        except:
            return 0

    def _parse_date(self, date_str):
        if not date_str:
            return ""
        try:
            match = re.search(r"\d+", date_str)
            if match:
                timestamp = int(match.group()) / 1000
                dt = datetime.fromtimestamp(timestamp)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            pass
        return ""


boche_crawler_ins = BoCheCrawler()



if __name__ == "__main__":
    print("=== 测试博车网爬虫 ===")
    _base_url = 'https://appservice.bochewang.com.cn'
    _username = '15105149161'
    _password = 'sal123456789'
    boche_crawler_ins.update_credentials(_base_url, _username, _password)
    
    print("需要登录才能获取数据，请先配置账号")
    _check_code_id = boche_crawler_ins.pre_login()
    _verify_code = input("请输入收到的短信验证码: ")
    session = boche_crawler_ins.login(_check_code_id, _verify_code)

    print("1. 事故车列表")
    print("2. 二手车列表")
    choice = input("请选择 (1/2): ").strip()
    
    if choice == "1":
        _cars = boche_crawler_ins.get_accident_cars(max_count=10)
        print(f"找到 {len(_cars)} 台事故车")
        for c in _cars[:3]:
            print(f"  - {c['che_liang_pin_pai']} {c['xuanZeZiXiLie']}: ¥{c['yi_kou_jia']}")
    elif choice == "2":
        _cars = boche_crawler_ins.get_used_cars(max_count=10)
        print(f"找到 {len(_cars)} 台二手车")
        for c in _cars[:3]:
            print(f"  - {c['che_liang_pin_pai']} {c['xuanZeZiXiLie']}: ¥{c['yi_kou_jia']}")
