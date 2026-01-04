import sys
import os
import json
from django.conf import settings
import mysql.connector
from datetime import datetime

from web_interface.models import Stock, StockRealTimeData


class StockDataService:
    def __init__(self):
        # 加载配置
        config_path = os.path.join(settings.BASE_DIR, 'config', 'config.json')
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)

        # 连接到MySQL
        mysql_config = self.config.get('mysql_config', {})
        self.mysql_conn = mysql.connector.connect(
            host=mysql_config.get('host', '127.0.0.1'),
            port=mysql_config.get('port', 3306),
            user=mysql_config.get('user', 'root'),
            password=mysql_config.get('password', ''),
            database=mysql_config.get('database', 'stock_analysis')
        )

    def check_table_exists(self, table_name):
        """检查表是否存在"""
        cursor = self.mysql_conn.cursor(dictionary=True)
        try:
            check_query = """
            SELECT COUNT(*) as count
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_name = %s
            """
            cursor.execute(check_query, (table_name,))
            result = cursor.fetchone()
            return result and result['count'] > 0
        except Exception as e:
            print(f"检查表 {table_name} 是否存在时出错: {str(e)}")
            return False
        finally:
            cursor.close()

    def format_stock_code(self, code):
        """格式化股票代码"""
        if not code.startswith(('sh', 'sz')):
            if code.startswith('6'):
                return f'sh{code}'
            elif code.startswith(('0', '3')):
                return f'sz{code}'
        return code

    async def get_realtime_data_from_mysql(self, stock_code=None):
        """直接从MySQL获取实时数据"""
        cursor = self.mysql_conn.cursor(dictionary=True)
        try:
            if stock_code:
                # 获取指定股票的实时数据
                formatted_code = self.format_stock_code(stock_code)
                table_name = f"stock_{formatted_code}_realtime"

                # 检查表是否存在
                if not self.check_table_exists(table_name):
                    print(f"表 {table_name} 不存在，跳过")
                    return None

                query = f"SELECT * FROM `{table_name}` ORDER BY `时间` DESC LIMIT 1"
                cursor.execute(query)
                result = cursor.fetchone()

                if result:
                    return self._format_stock_data(result, stock_code)
                return None
            else:
                # 获取所有股票的实时数据
                all_stocks = []
                for stock_info in self.config.get('stocks', []):
                    formatted_code = self.format_stock_code(stock_info['code'])
                    table_name = f"stock_{formatted_code}_realtime"

                    # 检查表是否存在
                    if not self.check_table_exists(table_name):
                        # print(f"表 {table_name} 不存在，跳过股票 {stock_info['code']}")
                        continue

                    try:
                        query = f"SELECT * FROM `{table_name}` ORDER BY `时间` DESC LIMIT 1"
                        cursor.execute(query)
                        result = cursor.fetchone()

                        if result:
                            all_stocks.append(self._format_stock_data(result, stock_info['code']))
                    except Exception as e:
                        print(f"获取股票 {stock_info['code']} 数据时出错: {str(e)}")

                return all_stocks
        finally:
            cursor.close()

    def _format_stock_data(self, raw_data, stock_code):
        """格式化股票数据"""
        # 获取股票名称和行业
        stock_info = next((s for s in self.config.get('stocks', []) if s['code'] == stock_code), None)
        name = stock_info['name'] if stock_info else 'Unknown'
        industry = stock_info.get('industry', '') if stock_info else ''

        # 计算涨跌和涨跌幅
        current_price = float(raw_data['当前价格'])
        last_close = float(raw_data['昨日收盘价'])
        change = current_price - last_close
        change_percent = (change / last_close) * 100 if last_close else 0

        return {
            'code': stock_code,
            'name': name,
            'industry': industry,
            'current_price': current_price,
            'open_price': float(raw_data['今日开盘价']),
            'last_close': last_close,
            'high_price': float(raw_data['今日最高价']),
            'low_price': float(raw_data['今日最低价']),
            'change': change,
            'change_percent': change_percent,
            'volume': int(raw_data['成交量(手)']),
            'amount': float(raw_data['成交额(元)']),
            'time': raw_data['时间']
        }

    def get_stock_industry(self, stock_code):
        """从配置获取股票行业"""
        for stock in self.config.get('stocks', []):
            if stock['code'] == stock_code:
                return stock.get('industry', '')
        return ''

    def get_realtime_data_sync(self, formatted_code):
        """同步获取实时数据（非异步版本）"""
        cursor = self.mysql_conn.cursor(dictionary=True)
        try:
            table_name = f"stock_{formatted_code}_realtime"

            # 检查表是否存在
            if not self.check_table_exists(table_name):
                # 表不存在，静默返回None（不打印错误，避免日志污染）
                return None

            query = f"SELECT * FROM `{table_name}` ORDER BY `时间` DESC LIMIT 1"
            cursor.execute(query)
            result = cursor.fetchone()

            if result:
                # 提取原始股票代码（去掉sh/sz前缀）
                stock_code = formatted_code[2:] if formatted_code.startswith(('sh', 'sz')) else formatted_code
                return self._format_stock_data(result, stock_code)
            return None
        except Exception as e:
            print(f"获取股票 {formatted_code} 数据出错: {str(e)}")
            return None
        finally:
            cursor.close()