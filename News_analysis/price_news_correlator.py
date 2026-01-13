#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
异构关联算法模块
监控股价异动，触发新闻检索并计算关联度
"""
import json
import logging
import mysql.connector
import redis
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import os
import hashlib

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('price_news_correlation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PriceNewsCorrelator:
    """价格-新闻异构关联分析器"""
    def __init__(self, config_path=None):
        """初始化"""
        # 获取配置文件路径
        if config_path is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)
            config_path = os.path.join(project_root, 'config', 'config.json')

        self.config_path = config_path
        self.load_config()

        # 异动检测阈值
        self.thresholds = {
            'price_change_threshold': 3.0,  # 涨跌幅阈值（%）
            'volume_spike_threshold': 1.5,   # 成交量突增倍数
            'time_window_before': 2,         # 向前查找新闻的小时数
            'time_window_after': 1,          # 向后查找新闻的小时数
            'min_correlation_score': 0.3     # 最小关联分数
        }

        # 连接Redis
        self.redis_client = redis.Redis(
            host=self.config['redis_config'].get('host', 'localhost'),
            port=self.config['redis_config'].get('port', 6379),
            db=self.config['redis_config'].get('db', 0),
            password=self.config['redis_config'].get('password'),
            decode_responses=True
        )

        # 连接MySQL
        self.mysql_conn = mysql.connector.connect(
            host=self.config['mysql_config']['host'],
            user=self.config['mysql_config']['user'],
            password=self.config['mysql_config']['password'],
            database=self.config['mysql_config']['database']
        )

        # 创建关联数据表
        self.create_correlation_table()

    def load_config(self):
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            logger.info("配置文件加载成功")
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            raise

    def create_correlation_table(self):
        """创建价格-新闻关联表"""
        try:
            cursor = self.mysql_conn.cursor()

            # 价格异动记录表
            create_anomaly_table = """
            CREATE TABLE IF NOT EXISTS price_anomalies (
                id INT AUTO_INCREMENT PRIMARY KEY,
                stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
                stock_name VARCHAR(50) COMMENT '股票名称',
                anomaly_time DATETIME NOT NULL COMMENT '异动时间',
                price_change_pct DECIMAL(8,4) COMMENT '价格变动百分比',
                current_price DECIMAL(10,4) COMMENT '当前价格',
                volume BIGINT COMMENT '成交量',
                volume_spike_ratio DECIMAL(8,4) COMMENT '成交量突增倍数',
                anomaly_type VARCHAR(20) COMMENT '异动类型(surge/plunge/spike)',
                related_news_count INT DEFAULT 0 COMMENT '关联新闻数量',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_stock_time (stock_code, anomaly_time),
                INDEX idx_anomaly_time (anomaly_time)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股价异动记录表';
            """

            # 价格-新闻关联表
            create_correlation_table = """
            CREATE TABLE IF NOT EXISTS price_news_correlation (
                id INT AUTO_INCREMENT PRIMARY KEY,
                anomaly_id INT COMMENT '关联的异动记录ID',
                stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
                stock_name VARCHAR(50) COMMENT '股票名称',
                anomaly_time DATETIME COMMENT '异动时间',
                news_hash VARCHAR(64) COMMENT '新闻哈希',
                news_content TEXT COMMENT '新闻内容',
                news_datetime DATETIME COMMENT '新闻时间',
                time_delta_minutes INT COMMENT '新闻与异动的时间差(分钟)',
                correlation_score DECIMAL(5,4) COMMENT '关联度评分(0-1)',
                correlation_type VARCHAR(20) COMMENT '关联类型(cause/reaction/unrelated)',
                sentiment_score DECIMAL(5,4) COMMENT '新闻情感分数',
                confidence DECIMAL(5,4) COMMENT '置信度',
                reasoning TEXT COMMENT '关联判断理由',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_anomaly_id (anomaly_id),
                INDEX idx_stock_code (stock_code),
                INDEX idx_correlation_score (correlation_score),
                FOREIGN KEY (anomaly_id) REFERENCES price_anomalies(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='价格-新闻关联表';
            """

            cursor.execute(create_anomaly_table)
            cursor.execute(create_correlation_table)
            self.mysql_conn.commit()
            logger.info("关联分析数据表创建成功")

            cursor.close()
        except Exception as e:
            logger.error(f"创建关联表失败: {e}")

    def detect_price_anomalies(self, stock_code: str, stock_name: str) -> List[Dict]:
        """检测股价异动"""
        try:
            cursor = self.mysql_conn.cursor(dictionary=True)

            # 格式化股票代码
            formatted_code = self._format_stock_code(stock_code)
            realtime_table = f"stock_{formatted_code}_realtime"

            # 获取最近的实时数据
            query = f"""
            SELECT 时间 as time, 当前价格 as price, 昨日收盘价 as last_close,
                   成交量_手 as volume, 涨跌幅_百分比 as change_pct
            FROM {realtime_table}
            ORDER BY 时间 DESC
            LIMIT 100
            """

            cursor.execute(query)
            data = cursor.fetchall()
            cursor.close()

            if not data or len(data) < 2:
                return []

            anomalies = []

            # 计算平均成交量
            avg_volume = np.mean([d['volume'] for d in data[10:]])

            # 检测异动
            for i, record in enumerate(data[:10]):  # 检查最近10条
                price_change = abs(float(record.get('change_pct', 0)))
                current_volume = record.get('volume', 0)
                volume_spike = current_volume / avg_volume if avg_volume > 0 else 1

                # 判断是否为异动
                is_price_anomaly = price_change >= self.thresholds['price_change_threshold']
                is_volume_spike = volume_spike >= self.thresholds['volume_spike_threshold']

                if is_price_anomaly or (is_volume_spike and price_change >= 2.0):
                    anomaly_type = 'surge' if float(record.get('change_pct', 0)) > 0 else 'plunge'

                    anomalies.append({
                        'stock_code': stock_code,
                        'stock_name': stock_name,
                        'anomaly_time': record['time'],
                        'price_change_pct': float(record.get('change_pct', 0)),
                        'current_price': float(record.get('price', 0)),
                        'volume': current_volume,
                        'volume_spike_ratio': volume_spike,
                        'anomaly_type': anomaly_type
                    })

                    logger.info(f"检测到异动: {stock_name}({stock_code}) "
                              f"时间:{record['time']} 涨跌:{price_change:.2f}% "
                              f"成交量倍数:{volume_spike:.2f}x")

            return anomalies

        except Exception as e:
            logger.error(f"检测{stock_code}价格异动失败: {e}")
            return []

    def _format_stock_code(self, code: str) -> str:
        """格式化股票代码"""
        if not code.startswith(('sh', 'sz')):
            if code.startswith('6'):
                return f'sh{code}'
            elif code.startswith(('0', '3')):
                return f'sz{code}'
        return code

    def retrieve_related_news(self, anomaly_time: datetime, stock_code: str, stock_name: str) -> List[Dict]:
        """检索相关时间窗口的新闻"""
        try:
            # 计算时间窗口
            time_before = anomaly_time - timedelta(hours=self.thresholds['time_window_before'])
            time_after = anomaly_time + timedelta(hours=self.thresholds['time_window_after'])

            logger.info(f"检索新闻时间窗口: {time_before} ~ {time_after}")

            # 从Redis获取所有新闻
            news_list = self.redis_client.lrange('stock:hot_news', 0, -1)

            related_news = []

            for news_json in news_list:
                try:
                    news = json.loads(news_json)
                    news_content = news.get('content', '')
                    news_time_str = news.get('datetime', '')

                    # 解析新闻时间
                    try:
                        news_time = datetime.strptime(news_time_str, '%Y-%m-%d %H:%M:%S')
                    except:
                        continue

                    # 检查是否在时间窗口内
                    if time_before <= news_time <= time_after:
                        # 检查是否提到该股票
                        mentions_stock = (stock_code in news_content or
                                        stock_name in news_content)

                        # 计算时间差
                        time_delta = (news_time - anomaly_time).total_seconds() / 60  # 转为分钟

                        related_news.append({
                            'news': news,
                            'news_time': news_time,
                            'time_delta_minutes': int(time_delta),
                            'mentions_stock': mentions_stock,
                            'news_hash': hashlib.md5(
                                f"{news_content}|{news_time_str}".encode('utf-8')
                            ).hexdigest()
                        })

                except Exception as e:
                    logger.error(f"处理新闻时出错: {e}")
                    continue

            logger.info(f"找到 {len(related_news)} 条相关时间窗口内的新闻")
            return related_news

        except Exception as e:
            logger.error(f"检索相关新闻失败: {e}")
            return []

    def calculate_correlation_score(self, anomaly: Dict, news_item: Dict) -> Tuple[float, str, str]:
        """计算新闻与价格异动的关联度"""
        try:
            score = 0.0
            correlation_type = 'unrelated'
            reasoning = ''

            # 时间接近度
            time_delta_abs = abs(news_item['time_delta_minutes'])
            if time_delta_abs < 30:
                time_score = 0.4
            elif time_delta_abs < 60:
                time_score = 0.3
            elif time_delta_abs < 120:
                time_score = 0.2
            else:
                time_score = 0.1
            score += time_score

            # 是否明确提及股票
            if news_item['mentions_stock']:
                score += 0.3
                reasoning = f"新闻明确提及{anomaly['stock_name']}"

            # 新闻情感与价格变动方向一致性
            # 从数据库获取新闻情感
            sentiment_score = self._get_news_sentiment(news_item['news_hash'])
            if sentiment_score is not None:
                price_direction = 1 if anomaly['price_change_pct'] > 0 else -1
                sentiment_direction = 1 if sentiment_score > 0 else -1

                if price_direction == sentiment_direction:
                    score += 0.3
                    reasoning += f"; 情感方向一致({sentiment_score:.2f})"
                elif abs(sentiment_score) < 0.2:  # 中性新闻
                    score += 0.1

            # 判断关联类型
            if news_item['time_delta_minutes'] < 0:  # 新闻在异动之前
                if score >= 0.6:
                    correlation_type = 'cause'  # 可能是原因
                elif score >= 0.3:
                    correlation_type = 'potential_cause'
            else:  # 新闻在异动之后
                if score >= 0.6:
                    correlation_type = 'reaction'  # 可能是反应/报道
                elif score >= 0.3:
                    correlation_type = 'potential_reaction'

            # 添加时间差到推理
            if reasoning:
                reasoning += f"; 时间差{time_delta_abs}分钟"

            return round(score, 4), correlation_type, reasoning

        except Exception as e:
            logger.error(f"计算关联度失败: {e}")
            return 0.0, 'unrelated', ''

    def _get_news_sentiment(self, news_hash: str) -> Optional[float]:
        """从数据库获取新闻情感评分"""
        try:
            cursor = self.mysql_conn.cursor(dictionary=True)
            cursor.execute(
                "SELECT sentiment_score FROM news_sentiment WHERE news_hash = %s",
                (news_hash,)
            )
            result = cursor.fetchone()
            cursor.close()

            if result:
                return float(result['sentiment_score'])
            return None

        except:
            return None

    def save_anomaly_and_correlations(self, anomaly: Dict, related_news: List[Dict], correlations: List[Dict]):
        """保存异动和关联数据"""
        try:
            cursor = self.mysql_conn.cursor()

            # 插入异动记录
            insert_anomaly_sql = """
            INSERT INTO price_anomalies
            (stock_code, stock_name, anomaly_time, price_change_pct, current_price,
             volume, volume_spike_ratio, anomaly_type, related_news_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(insert_anomaly_sql, (
                anomaly['stock_code'],
                anomaly['stock_name'],
                anomaly['anomaly_time'],
                anomaly['price_change_pct'],
                anomaly['current_price'],
                anomaly['volume'],
                anomaly['volume_spike_ratio'],
                anomaly['anomaly_type'],
                len(correlations)
            ))

            anomaly_id = cursor.lastrowid

            # 插入关联记录
            insert_correlation_sql = """
            INSERT INTO price_news_correlation
            (anomaly_id, stock_code, stock_name, anomaly_time, news_hash, news_content,
             news_datetime, time_delta_minutes, correlation_score, correlation_type,
             sentiment_score, confidence, reasoning)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            for corr in correlations:
                news_data = corr['news_item']
                sentiment_score = self._get_news_sentiment(news_data['news_hash'])

                cursor.execute(insert_correlation_sql, (
                    anomaly_id,
                    anomaly['stock_code'],
                    anomaly['stock_name'],
                    anomaly['anomaly_time'],
                    news_data['news_hash'],
                    news_data['news'].get('content', ''),
                    news_data['news_time'],
                    news_data['time_delta_minutes'],
                    corr['correlation_score'],
                    corr['correlation_type'],
                    sentiment_score,
                    corr['correlation_score'],  # 使用关联度作为置信度
                    corr['reasoning']
                ))

            self.mysql_conn.commit()
            logger.info(f"保存异动记录和{len(correlations)}条关联数据成功")
            cursor.close()

        except Exception as e:
            logger.error(f"保存关联数据失败: {e}")
            self.mysql_conn.rollback()

    def analyze_stock(self, stock_code: str, stock_name: str):
        """分析单只股票的价格-新闻关联"""
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"分析股票: {stock_name}({stock_code})")
            logger.info(f"{'='*60}")

            # 检测价格异动
            anomalies = self.detect_price_anomalies(stock_code, stock_name)

            if not anomalies:
                logger.info(f"{stock_name} 未检测到价格异动")
                return

            # 对每个异动检索相关新闻
            for anomaly in anomalies:
                logger.info(f"\n处理异动: {anomaly['anomaly_time']} "
                          f"涨跌{anomaly['price_change_pct']:.2f}%")

                # 检索相关新闻
                related_news = self.retrieve_related_news(
                    anomaly['anomaly_time'],
                    stock_code,
                    stock_name
                )

                if not related_news:
                    logger.info("未找到相关新闻")
                    continue

                # 计算关联度
                correlations = []
                for news_item in related_news:
                    score, corr_type, reasoning = self.calculate_correlation_score(
                        anomaly, news_item
                    )

                    if score >= self.thresholds['min_correlation_score']:
                        correlations.append({
                            'news_item': news_item,
                            'correlation_score': score,
                            'correlation_type': corr_type,
                            'reasoning': reasoning
                        })

                        logger.info(f"  ✓ 关联度: {score:.2f} | 类型: {corr_type} | "
                                  f"新闻: {news_item['news'].get('content', '')[:50]}...")

                # 保存关联数据
                if correlations:
                    # 按关联度排序
                    correlations.sort(key=lambda x: x['correlation_score'], reverse=True)
                    self.save_anomaly_and_correlations(anomaly, related_news, correlations)
                    logger.info(f"保存了{len(correlations)}条高关联度新闻")

        except Exception as e:
            logger.error(f"分析股票{stock_code}失败: {e}")
            import traceback
            traceback.print_exc()

    def analyze_all_stocks(self):
        """分析所有股票"""
        try:
            stocks = self.config.get('stocks', [])
            other_stocks = self.config.get('other_stocks', [])
            all_stocks = stocks + other_stocks

            logger.info(f"开始分析{len(all_stocks)}只股票的价格-新闻关联")

            for stock in all_stocks:
                self.analyze_stock(stock['code'], stock['name'])

            logger.info("\n所有股票分析完成！")

        except Exception as e:
            logger.error(f"分析所有股票失败: {e}")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='价格-新闻异构关联分析')
    parser.add_argument('--stock', type=str, help='指定股票代码')
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("启动价格-新闻异构关联分析系统")
    logger.info("=" * 60)

    correlator = PriceNewsCorrelator()

    if args.stock:
        # 分析单只股票
        # 需要从配置中获取股票名称
        stocks = correlator.config.get('stocks', []) + correlator.config.get('other_stocks', [])
        stock_info = next((s for s in stocks if s['code'] == args.stock), None)

        if stock_info:
            correlator.analyze_stock(stock_info['code'], stock_info['name'])
        else:
            logger.error(f"未找到股票代码: {args.stock}")
    else:
        # 分析所有股票
        correlator.analyze_all_stocks()


if __name__ == '__main__':
    main()
