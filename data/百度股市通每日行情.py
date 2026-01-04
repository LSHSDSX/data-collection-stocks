import requests
import logging
from concurrent.futures import ThreadPoolExecutor
import random

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 定义多个 User-Agent
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 SLBrowser/9.0.6.2081 SLBChan/105 SLBVPV/64-bit",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko",
    "Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/91.0.4472.102 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.59"
]

ab_sr = [
    "1.0.1_MjNkNDY1YTE1MTJhMWIxNDRhN2RmYjkwMWIxMDIwMWVlYWU5YTM4ZTQzMTMwYWMwZmJiN2VhMDM4ZjJlNjhlMDUyNzM0YjViOTA2NDkwNDdhNmE1NmU4MDMxZjhlYmZkYmUxZTM3ZTg4NDZjMzYxNTUxYjg4NmRlNDI4MWM1ZTA0NGViNzIyODZhMGJkOWE5NzRhYTFmMDdlZWE4OWE5Njg2YzI5NDMwMWFmZTU1NDAwN2NhY2ExNWNkYzhmNWY5MDYwNTlhYmEzYjU2Y2RmNTQ4NjEzMGUwNWFkMTc0MmY",
    "1.0.1_YzIyOWJjMjVhMTAwZTA0ZmFmMTA1Y2FiODQ3MTE4Y2NkZjA2Nzc1NTQ0MWU4ODkyN2EyYWI0YmUyNGFjNzExNWExOTc4OGYxODgyNTRkMWQxZTczYzJjMjM4YmM2ZjU2ZTJhOWExZDAwYmZmMjU4ZDU4YWRiMjI4Y2RhODEyYjhmMzAwMzdiMWIwNzI0YTFjMDljMGEyMTZhNmFhOTJmMGJkNDJjNzk5YjQ4MTBlYTg0MTAwZmFiZmIxMzc3Njg1MmE2MWI3NTBjMTVkZDY4OWFjM2NhMDliMzBkMzQxNDI",
    "1.0.1_OTNhMDZiNjYwNzdlMWVlYmUyMmIzYzgyYTZmYTZlZDUyZDViMTRlNWI4Mjc1ZWYzZmZjNjNjM2ZhNTM2Mzg1YjM2MTY2NWE4ODc0ODM0NGFmNGY3MmM4ZmQyNWZjOTg1MjFiYzU0ZmI0MzhmOTcxZDYxNGRjMGNlNDZhNTlhNzhlYzAxZDMxMjIzMmFmN2EzODY2NzI2MWJhYzU0NmNlZQ=="
]


class OptimizedFinanceDataFetcher:
    def __init__(self, stock_code):
        self.session = requests.Session()
        self.stock_code = stock_code.replace("sh", "").replace("sz", "")
        self.cookies = {
            # "PSTM": "1738670965",
            # "BIDUPSID": "6809D4BFC0C6AB1FB5DD4D1C172778F1",
            # "BAIDUID": "577E7142C54C907E28E7FDD71F4DD49B:FG=1",
            # "BAIDUID_BFESS": "577E7142C54C907E28E7FDD71F4DD49B:FG=1",
            # "BA_HECTOR": "a425210la485a524a4a4208l8hah0g1ju5a5v23",
            # "ZFY": "HGb:B6CDqjTYg4XbOa4a5u5LQiaR3wOgfUuntgXBggak:C",
            # "H_PS_PSSID": "61027_62325_62336_62346_62373_62426_62475_62485_62456_62455_62453_62451_62586_62611_62638_62673_62676_62618_62693_62714_62519",
            # "BDORZ": "FFFB88E999055A3F8A630C64834BD6D0",
            # "H_WISE_SIDS": "61027_62325_62336_62346_62373_62426_62475_62485_62456_62455_62453_62451_62586_62611_62638_62673_62676_62618_62693_62714_62519",
            # "ab_sr": "1.0.1_MjNkNDY1YTE1MTJhMWIxNDRhN2RmYjkwMWIxMDIwMWVlYWU5YTM4ZTQzMTMwYWMwZmJiN2VhMDM4ZjJlNjhlMDUyNzM0YjViOTA2NDkwNDdhNmE1NmU4MDMxZjhlYmZkYmUxZTM3ZTg4NDZjMzYxNTUxYjg4NmRlNDI4MWM1ZTA0NGViNzIyODZhMGJkOWE5NzRhYTFmMDdlZWE4OWE5Njg2YzI5NDMwMWFmZTU1NDAwN2NhY2ExNWNkYzhmNWY5MDYwNTlhYmEzYjU2Y2RmNTQ4NjEzMGUwNWFkMTc0MmY"
            "ab_sr": "1.0.1_OTNhMDZiNjYwNzdlMWVlYmUyMmIzYzgyYTZmYTZlZDUyZDViMTRlNWI4Mjc1ZWYzZmZjNjNjM2ZhNTM2Mzg1YjM2MTY2NWE4ODc0ODM0NGFmNGY3MmM4ZmQyNWZjOTg1MjFiYzU0ZmI0MzhmOTcxZDYxNGRjMGNlNDZhNTlhNzhlYzAxZDMxMjIzMmFmN2EzODY2NzI2MWJhYzU0NmNlZQ=="
        }
        self.url = "https://finance.pae.baidu.com/vapi/v1/getquotation"
        self.params = {
            "srcid": "5353",
            "all": "1",
            "code": stock_code,
            "query": stock_code,
            "eprop": "min",
            "stock_type": "ab",
            "chartType": "minute",
            "group": "quotation_minute_ab",
            "finClientType": "pc"
        }
        self.timeout = 10  # 设置请求超时时间为10秒

    def fetch_data(self):
        # 随机选择一个 User-Agent
        user_agent = random.choice(USER_AGENTS)
        headers = {
            "User-Agent": user_agent,
            "Referer": "https://gushitong.baidu.com/",  # 最重要的反爬头
            "Origin": "https://gushitong.baidu.com",
            "Host": "finance.pae.baidu.com",
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "Connection": "keep-alive"
        }
        try:
            response = self.session.get(self.url, 
                                        headers=headers, 
                                        cookies=self.cookies, 
                                        params=self.params,
                                        timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                logging.error(f"百度接口 403 禁止访问 (Cookie可能已过期): {self.stock_code}")
            else:
                logging.error(f"HTTP 错误: {e}")
            return None
        except requests.RequestException as e:
            logging.error(f"请求网络错误: {e}")
            return None
        except ValueError as e:
            logging.error(f"解析 JSON 出错: {e}")
            return None

    def get_data(self):
        data = self.fetch_data()
        if not data or "Result" not in data: # Added check for "Result" key
            return {}

        result_dict = {}
        try:
            # 解析当前价格信息
            if "cur" in data["Result"]:
                result_dict["今收"] = data["Result"]["cur"]["price"]
                # 补充其他可能需要的字段，防止报错
                result_dict["今开"] = data["Result"]["cur"].get("open", 0)
                result_dict["最高"] = data["Result"]["cur"].get("high", 0)
                result_dict["最低"] = data["Result"]["cur"].get("low", 0)
                result_dict["成交量"] = data["Result"]["cur"].get("volume", 0)
                result_dict["成交额"] = data["Result"]["cur"].get("amount", 0)
                result_dict["昨收"] = data["Result"]["cur"].get("preClose", 0)

            # 解析盘口信息（如果有）
            if "pankouinfos" in data["Result"] and "list" in data["Result"]["pankouinfos"]:
                for item in data["Result"]["pankouinfos"]["list"]:
                    key = item["name"]
                    value = item["originValue"]
                    if key in ["成交量", "成交额", "总市值", "总股本", "流通值", "流通股"]:
                        value = self._convert_numeric(value)
                    result_dict[key] = value
        except KeyError as e:
            logging.error(f"数据解析缺少键: {e}")
        except Exception as e:
            logging.error(f"数据处理未知错误: {e}")

        # print(result_dict)
        return result_dict

    @staticmethod
    def _convert_numeric(value):
        try:
            if isinstance(value, str): # Add check to ensure value is string before replace
                if '亿' in value:
                    return float(value.replace('亿', '')) * 1e8
                elif '万' in value:
                    return float(value.replace('万', '')) * 1e4
                elif '%' in value:
                    return float(value.replace('%', '')) / 100
                # 处理 '-' 或其他非数字字符
                if value == '-' or not value.strip():
                    return 0.0
            return float(value)
        except ValueError:
            return value


def batch_fetch_codes(codes, workers=5):
    results = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(OptimizedFinanceDataFetcher(code).get_data): code for code in codes}
        for future in futures:
            code = futures[future]
            try:
                results[code] = future.result()
            except Exception as e:
                logging.error(f"获取股票 {code} 数据时出错: {e}")
    return results


if __name__ == "__main__":
    # 单股票获取
    fetcher = OptimizedFinanceDataFetcher("000421")
    result = fetcher.get_data()
    print(f"Fetch Result: {result}")

    # 批量获取示例
    # codes = ["000421", "600036", "601398", "002594"]
    # batch_results = batch_fetch_codes(codes)
    # logging.info(f"批量获取股票数据结果: {batch_results}")
