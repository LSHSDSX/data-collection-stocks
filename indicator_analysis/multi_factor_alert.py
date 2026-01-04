#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
多因子预警系统
整合价格、技术指标、新闻情感、GPR预测的综合预警系统
"""
import json
import logging
import mysql.connector
import redis
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import os

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('multi_factor_alert.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MultiFactorAlertSystem:
    """多因子预警系统"""

    def __init__(self, config_path=None):
        """初始化预警系统"""
        if config_path is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)
            config_path = os.path.join(project_root, 'config', 'config.json')

        self.config_path = config_path
        self.load_config()

        # 连接MySQL
        self.mysql_conn = mysql.connector.connect(
            host=self.config['mysql_config']['host'],
            user=self.config['mysql_config']['user'],
            password=self.config['mysql_config']['password'],
            database=self.config['mysql_config']['database']
        )

        # 连接Redis
        self.redis_client = redis.Redis(
            host=self.config['redis_config'].get('host', 'localhost'),
            port=self.config['redis_config'].get('port', 6379),
            db=self.config['redis_config'].get('db', 0),
            password=self.config['redis_config'].get('password'),
            decode_responses=True
        )

        # 预警阈值配置
        self.alert_thresholds = {
            # 价格波动预警
            'price_change_warning': 3.0,      # 涨跌幅超过3%预警
            'price_change_critical': 5.0,     # 涨跌幅超过5%严重预警

            # 技术指标预警
            'rsi_overbought': 70,             # RSI超买
            'rsi_oversold': 30,               # RSI超卖
            'macd_divergence': True,          # MACD背离

            # 情感预警
            'sentiment_extreme_positive': 0.7,  # 极度正面情感
            'sentiment_extreme_negative': -0.7, # 极度负面情感
            'sentiment_rapid_change': 0.5,      # 情感快速变化

            # GPR预测偏离预警
            'gpr_deviation_warning': 0.05,     # 实际价格偏离预测5%
            'gpr_deviation_critical': 0.10,    # 实际价格偏离预测10%

            # 异动预警
            'volume_spike': 2.0,               # 成交量突增2倍
            'correlation_high': 0.7            # 新闻-价格关联度高
        }

        # 创建预警表
        self.create_alert_table()

    def load_config(self):
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            logger.info("配置文件加载成功")
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            raise

    def create_alert_table(self):
        """创建预警记录表"""
        try:
            cursor = self.mysql_conn.cursor()

            create_table_sql = """
            CREATE TABLE IF NOT EXISTS multi_factor_alerts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
                stock_name VARCHAR(50) COMMENT '股票名称',
                alert_time DATETIME NOT NULL COMMENT '预警时间',
                alert_type VARCHAR(50) NOT NULL COMMENT '预警类型',
                alert_level VARCHAR(20) NOT NULL COMMENT '预警级别(INFO/WARNING/CRITICAL)',
                alert_message TEXT COMMENT '预警消息',
                alert_details JSON COMMENT '预警详情',

                -- 触发预警的数据
                current_price DECIMAL(10,4) COMMENT '当前价格',
                price_change_pct DECIMAL(8,4) COMMENT '涨跌幅',
                rsi_value DECIMAL(8,4) COMMENT 'RSI值',
                macd_value DECIMAL(10,6) COMMENT 'MACD值',
                sentiment_score DECIMAL(5,4) COMMENT '情感评分',
                gpr_predicted_price DECIMAL(10,4) COMMENT 'GPR预测价格',

                is_read BOOLEAN DEFAULT FALSE COMMENT '是否已读',
                is_handled BOOLEAN DEFAULT FALSE COMMENT '是否已处理',
                handler_note TEXT COMMENT '处理备注',

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                INDEX idx_stock_time (stock_code, alert_time),
                INDEX idx_alert_level (alert_level),
                INDEX idx_alert_type (alert_type),
                INDEX idx_is_read (is_read)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='多因子预警记录表';
            """

            cursor.execute(create_table_sql)
            self.mysql_conn.commit()
            logger.info("多因子预警表创建成功")
            cursor.close()

        except Exception as e:
            logger.error(f"创建预警表失败: {e}")

    def check_price_alerts(self, stock_code: str, stock_name: str) -> List[Dict]:
        """检查价格异动预警"""
        alerts = []

        try:
            cursor = self.mysql_conn.cursor(dictionary=True)
            formatted_code = self._format_stock_code(stock_code)
            realtime_table = f"stock_{formatted_code}_realtime"

            # 获取最新价格数据
            query = f"""
            SELECT 当前价格 as current_price, 涨跌幅_百分比 as change_pct,
                   成交量_手 as volume, 时间 as time
            FROM {realtime_table}
            ORDER BY 时间 DESC
            LIMIT 10
            """

            cursor.execute(query)
            data = cursor.fetchall()
            cursor.close()

            if not data:
                return alerts

            latest = data[0]
            change_pct = abs(float(latest.get('change_pct', 0)))

            # 检查涨跌幅预警
            if change_pct >= self.alert_thresholds['price_change_critical']:
                alerts.append({
                    'type': 'PRICE_CHANGE',
                    'level': 'CRITICAL',
                    'message': f"价格剧烈波动: {change_pct:.2f}%",
                    'details': {
                        'current_price': float(latest['current_price']),
                        'change_pct': change_pct,
                        'direction': '上涨' if latest.get('change_pct', 0) > 0 else '下跌'
                    }
                })
            elif change_pct >= self.alert_thresholds['price_change_warning']:
                alerts.append({
                    'type': 'PRICE_CHANGE',
                    'level': 'WARNING',
                    'message': f"价格显著波动: {change_pct:.2f}%",
                    'details': {
                        'current_price': float(latest['current_price']),
                        'change_pct': change_pct,
                        'direction': '上涨' if latest.get('change_pct', 0) > 0 else '下跌'
                    }
                })

            # 检查成交量突增
            if len(data) >= 5:
                recent_volumes = [d['volume'] for d in data[:5]]
                avg_volume = np.mean(recent_volumes[1:])
                current_volume = recent_volumes[0]

                if avg_volume > 0 and current_volume / avg_volume >= self.alert_thresholds['volume_spike']:
                    alerts.append({
                        'type': 'VOLUME_SPIKE',
                        'level': 'WARNING',
                        'message': f"成交量异常放大: {current_volume / avg_volume:.2f}倍",
                        'details': {
                            'current_volume': current_volume,
                            'avg_volume': avg_volume,
                            'spike_ratio': current_volume / avg_volume
                        }
                    })

        except Exception as e:
            logger.error(f"检查价格预警失败: {e}")

        return alerts

    def check_technical_alerts(self, stock_code: str, stock_name: str) -> List[Dict]:
        """检查技术指标预警"""
        alerts = []

        try:
            cursor = self.mysql_conn.cursor(dictionary=True)

            # 获取实时技术指标 - 使用stock_name（表已经是用名称创建的）
            # 但需要先检查表是否存在
            realtime_technical_table = f"realtime_technical_{stock_name}"

            # 检查表是否存在
            check_query = """
            SELECT COUNT(*) as count
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_name = %s
            """
            cursor.execute(check_query, (realtime_technical_table,))
            result = cursor.fetchone()

            if not result or result['count'] == 0:
                logger.warning(f"表 {realtime_technical_table} 不存在，跳过技术指标预警")
                cursor.close()
                return alerts

            query = f"""
            SELECT RSI, MACD, MACD_Hist, `Signal`, 时间 as time
            FROM `{realtime_technical_table}`
            ORDER BY 时间 DESC
            LIMIT 5
            """

            cursor.execute(query)
            data = cursor.fetchall()
            cursor.close()

            if not data:
                return alerts

            latest = data[0]

            # 检查RSI预警
            if 'RSI' in latest and latest['RSI'] is not None:
                rsi = float(latest['RSI'])

                if rsi >= self.alert_thresholds['rsi_overbought']:
                    alerts.append({
                        'type': 'RSI_OVERBOUGHT',
                        'level': 'WARNING',
                        'message': f"RSI超买: {rsi:.2f}",
                        'details': {
                            'rsi_value': rsi,
                            'threshold': self.alert_thresholds['rsi_overbought']
                        }
                    })
                elif rsi <= self.alert_thresholds['rsi_oversold']:
                    alerts.append({
                        'type': 'RSI_OVERSOLD',
                        'level': 'WARNING',
                        'message': f"RSI超卖: {rsi:.2f}",
                        'details': {
                            'rsi_value': rsi,
                            'threshold': self.alert_thresholds['rsi_oversold']
                        }
                    })

            # 检查MACD金叉/死叉
            if len(data) >= 2 and 'MACD' in latest and 'Signal' in latest:
                prev = data[1]

                if prev.get('MACD') and prev.get('Signal'):
                    # 金叉: MACD从下方穿过Signal
                    if prev['MACD'] < prev['Signal'] and latest['MACD'] > latest['Signal']:
                        alerts.append({
                            'type': 'MACD_GOLDEN_CROSS',
                            'level': 'INFO',
                            'message': "MACD金叉形成",
                            'details': {
                                'macd': float(latest['MACD']),
                                'signal': float(latest['Signal'])
                            }
                        })
                    # 死叉: MACD从上方穿过Signal
                    elif prev['MACD'] > prev['Signal'] and latest['MACD'] < latest['Signal']:
                        alerts.append({
                            'type': 'MACD_DEATH_CROSS',
                            'level': 'WARNING',
                            'message': "MACD死叉形成",
                            'details': {
                                'macd': float(latest['MACD']),
                                'signal': float(latest['Signal'])
                            }
                        })

        except Exception as e:
            logger.error(f"检查技术指标预警失败: {e}")

        return alerts

    def check_sentiment_alerts(self, stock_code: str, stock_name: str) -> List[Dict]:
        """检查新闻情感预警"""
        alerts = []

        try:
            cursor = self.mysql_conn.cursor(dictionary=True)

            # 获取最近24小时的新闻情感
            query = """
            SELECT sentiment_score, confidence, news_datetime, news_content
            FROM price_news_correlation
            WHERE stock_code = %s
                AND news_datetime >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            ORDER BY news_datetime DESC
            LIMIT 10
            """

            cursor.execute(query, (stock_code,))
            data = cursor.fetchall()
            cursor.close()

            if not data:
                return alerts

            # 检查极端情感
            for item in data:
                if item['sentiment_score'] is not None:
                    score = float(item['sentiment_score'])

                    if score >= self.alert_thresholds['sentiment_extreme_positive']:
                        alerts.append({
                            'type': 'SENTIMENT_EXTREME_POSITIVE',
                            'level': 'INFO',
                            'message': f"极度正面新闻情感: {score:.2f}",
                            'details': {
                                'sentiment_score': score,
                                'news_time': str(item['news_datetime']),
                                'news_preview': item['news_content'][:100] if item['news_content'] else ''
                            }
                        })
                    elif score <= self.alert_thresholds['sentiment_extreme_negative']:
                        alerts.append({
                            'type': 'SENTIMENT_EXTREME_NEGATIVE',
                            'level': 'WARNING',
                            'message': f"极度负面新闻情感: {score:.2f}",
                            'details': {
                                'sentiment_score': score,
                                'news_time': str(item['news_datetime']),
                                'news_preview': item['news_content'][:100] if item['news_content'] else ''
                            }
                        })

            # 检查情感快速变化
            if len(data) >= 3:
                recent_scores = [float(d['sentiment_score']) for d in data[:3] if d['sentiment_score'] is not None]
                if len(recent_scores) >= 2:
                    sentiment_change = abs(recent_scores[0] - recent_scores[-1])

                    if sentiment_change >= self.alert_thresholds['sentiment_rapid_change']:
                        alerts.append({
                            'type': 'SENTIMENT_RAPID_CHANGE',
                            'level': 'WARNING',
                            'message': f"情感快速变化: {sentiment_change:.2f}",
                            'details': {
                                'from_score': recent_scores[-1],
                                'to_score': recent_scores[0],
                                'change': sentiment_change
                            }
                        })

        except Exception as e:
            logger.error(f"检查情感预警失败: {e}")

        return alerts

    def check_gpr_deviation_alerts(self, stock_code: str, stock_name: str) -> List[Dict]:
        """检查GPR预测偏离预警"""
        alerts = []

        try:
            cursor = self.mysql_conn.cursor(dictionary=True)

            # 先检查预测表是否存在
            check_table_query = """
            SELECT COUNT(*) as count
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_name = 'stock_price_predictions'
            """
            cursor.execute(check_table_query)
            table_result = cursor.fetchone()

            if not table_result or table_result['count'] == 0:
                logger.warning("GPR预测表不存在，跳过GPR偏离预警")
                cursor.close()
                return alerts

            # 获取今天的GPR预测
            query = """
            SELECT predicted_price, price_lower_bound, price_upper_bound
            FROM stock_price_predictions
            WHERE stock_code = %s
                AND target_date = CURDATE()
            ORDER BY prediction_date DESC
            LIMIT 1
            """

            cursor.execute(query, (stock_code,))
            prediction = cursor.fetchone()

            if not prediction:
                return alerts

            # 获取当前实际价格
            formatted_code = self._format_stock_code(stock_code)
            realtime_table = f"stock_{formatted_code}_realtime"

            price_query = f"""
            SELECT 当前价格 as current_price
            FROM {realtime_table}
            ORDER BY 时间 DESC
            LIMIT 1
            """

            cursor.execute(price_query)
            price_data = cursor.fetchone()
            cursor.close()

            if not price_data:
                return alerts

            current_price = float(price_data['current_price'])
            predicted_price = float(prediction['predicted_price'])
            lower_bound = float(prediction['price_lower_bound'])
            upper_bound = float(prediction['price_upper_bound'])

            # 计算偏离程度
            deviation_pct = abs(current_price - predicted_price) / predicted_price

            # 检查是否超出置信区间
            if current_price > upper_bound:
                alerts.append({
                    'type': 'GPR_DEVIATION_UPPER',
                    'level': 'WARNING',
                    'message': f"价格超出预测上界: {current_price:.2f} > {upper_bound:.2f}",
                    'details': {
                        'current_price': current_price,
                        'predicted_price': predicted_price,
                        'upper_bound': upper_bound,
                        'deviation_pct': deviation_pct * 100
                    }
                })
            elif current_price < lower_bound:
                alerts.append({
                    'type': 'GPR_DEVIATION_LOWER',
                    'level': 'WARNING',
                    'message': f"价格低于预测下界: {current_price:.2f} < {lower_bound:.2f}",
                    'details': {
                        'current_price': current_price,
                        'predicted_price': predicted_price,
                        'lower_bound': lower_bound,
                        'deviation_pct': deviation_pct * 100
                    }
                })
            elif deviation_pct >= self.alert_thresholds['gpr_deviation_critical']:
                alerts.append({
                    'type': 'GPR_DEVIATION_CRITICAL',
                    'level': 'CRITICAL',
                    'message': f"价格严重偏离预测: {deviation_pct*100:.2f}%",
                    'details': {
                        'current_price': current_price,
                        'predicted_price': predicted_price,
                        'deviation_pct': deviation_pct * 100
                    }
                })

        except Exception as e:
            logger.error(f"检查GPR偏离预警失败: {e}")

        return alerts

    def _format_stock_code(self, code: str) -> str:
        """格式化股票代码"""
        if not code.startswith(('sh', 'sz')):
            if code.startswith('6'):
                return f'sh{code}'
            elif code.startswith(('0', '3')):
                return f'sz{code}'
        return code

    def save_alert(self, stock_code: str, stock_name: str, alert: Dict):
        """保存预警记录"""
        try:
            cursor = self.mysql_conn.cursor()

            insert_sql = """
            INSERT INTO multi_factor_alerts
            (stock_code, stock_name, alert_time, alert_type, alert_level,
             alert_message, alert_details)
            VALUES (%s, %s, NOW(), %s, %s, %s, %s)
            """

            cursor.execute(insert_sql, (
                stock_code,
                stock_name,
                alert['type'],
                alert['level'],
                alert['message'],
                json.dumps(alert.get('details', {}), ensure_ascii=False)
            ))

            self.mysql_conn.commit()

            # 同时发送到Redis供实时推送
            alert_data = {
                'stock_code': stock_code,
                'stock_name': stock_name,
                'alert_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                **alert
            }

            self.redis_client.lpush(
                'stock:alerts:realtime',
                json.dumps(alert_data, ensure_ascii=False)
            )

            # 保持最新100条
            self.redis_client.ltrim('stock:alerts:realtime', 0, 99)

            logger.info(f"保存预警: {stock_name}({stock_code}) - {alert['message']}")
            cursor.close()

        except Exception as e:
            logger.error(f"保存预警失败: {e}")
            self.mysql_conn.rollback()

    def analyze_stock(self, stock_code: str, stock_name: str):
        """分析单只股票的所有预警"""
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"分析股票预警: {stock_name}({stock_code})")
            logger.info(f"{'='*60}")

            all_alerts = []

            # 1. 价格预警
            price_alerts = self.check_price_alerts(stock_code, stock_name)
            all_alerts.extend(price_alerts)

            # 2. 技术指标预警
            technical_alerts = self.check_technical_alerts(stock_code, stock_name)
            all_alerts.extend(technical_alerts)

            # 3. 情感预警
            sentiment_alerts = self.check_sentiment_alerts(stock_code, stock_name)
            all_alerts.extend(sentiment_alerts)

            # 4. GPR偏离预警
            gpr_alerts = self.check_gpr_deviation_alerts(stock_code, stock_name)
            all_alerts.extend(gpr_alerts)

            # 保存所有预警
            for alert in all_alerts:
                self.save_alert(stock_code, stock_name, alert)

                # 打印预警
                level_icon = {
                    'INFO': 'ℹ️',
                    'WARNING': '⚠️',
                    'CRITICAL': '🚨'
                }
                icon = level_icon.get(alert['level'], '•')

                print(f"  {icon} [{alert['level']}] {alert['message']}")

            if not all_alerts:
                print(f"  ✓ 未发现异常")

            logger.info(f"完成预警分析, 发现 {len(all_alerts)} 条预警")

        except Exception as e:
            logger.error(f"分析股票预警失败: {e}")
            import traceback
            traceback.print_exc()

    def analyze_all_stocks(self):
        """分析所有股票的预警"""
        try:
            stocks = self.config.get('stocks', [])
            other_stocks = self.config.get('other_stocks', [])
            all_stocks = stocks + other_stocks

            logger.info(f"开始分析 {len(all_stocks)} 只股票的预警")

            for stock in all_stocks:
                self.analyze_stock(stock['code'], stock['name'])

            logger.info("\n所有股票预警分析完成!")

        except Exception as e:
            logger.error(f"分析所有股票预警失败: {e}")

    def get_recent_alerts(self, limit: int = 50, level: str = None) -> List[Dict]:
        """获取最近的预警记录"""
        try:
            cursor = self.mysql_conn.cursor(dictionary=True)

            if level:
                query = """
                SELECT * FROM multi_factor_alerts
                WHERE alert_level = %s
                ORDER BY alert_time DESC
                LIMIT %s
                """
                cursor.execute(query, (level, limit))
            else:
                query = """
                SELECT * FROM multi_factor_alerts
                ORDER BY alert_time DESC
                LIMIT %s
                """
                cursor.execute(query, (limit,))

            alerts = cursor.fetchall()
            cursor.close()

            return alerts

        except Exception as e:
            logger.error(f"获取预警记录失败: {e}")
            return []

    def close(self):
        """关闭连接"""
        if self.mysql_conn:
            self.mysql_conn.close()
        if self.redis_client:
            self.redis_client.close()


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='多因子预警系统')
    parser.add_argument('--stock', type=str, help='指定股票代码')
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("启动多因子预警系统")
    logger.info("=" * 60)

    alert_system = MultiFactorAlertSystem()

    try:
        if args.stock:
            # 分析单只股票
            stocks = alert_system.config.get('stocks', []) + alert_system.config.get('other_stocks', [])
            stock_info = next((s for s in stocks if s['code'] == args.stock), None)

            if stock_info:
                alert_system.analyze_stock(stock_info['code'], stock_info['name'])
            else:
                logger.error(f"未找到股票代码: {args.stock}")
        else:
            # 分析所有股票
            alert_system.analyze_all_stocks()

    finally:
        alert_system.close()

    logger.info("多因子预警系统结束")


if __name__ == '__main__':
    main()
