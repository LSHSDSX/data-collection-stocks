#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
增强版图表服务
支持双Y轴图表(价格+情感)和GPR预测阴影区
"""
import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Polygon
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import mysql.connector
import json
import os
import logging

logger = logging.getLogger(__name__)

import matplotlib
# 设置中文字体
matplotlib.rcParams['font.sans-serif'] = ['SimHei']
matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

class EnhancedChartService:
    """增强版图表服务"""

    def __init__(self, config_path=None):
        """初始化"""
        if config_path is None:
            # 从web_interface/services向上两级到项目根目录
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(script_dir))
            config_path = os.path.join(project_root, 'config', 'config.json')

        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)

        # 连接MySQL
        self.mysql_conn = mysql.connector.connect(
            host=self.config['mysql_config']['host'],
            user=self.config['mysql_config']['user'],
            password=self.config['mysql_config']['password'],
            database=self.config['mysql_config']['database']
        )

    def plot_price_sentiment_dual_axis(self, stock_code: str, stock_name: str, days: int = 30, save_path: str = None) -> str:
        """
        绘制双Y轴图表:价格+情感评分

        Args:
            stock_code: 股票代码
            stock_name: 股票名称
            days: 显示天数
            save_path: 保存路径

        Returns:
            图表文件路径
        """
        try:
            cursor = self.mysql_conn.cursor(dictionary=True)

            # 1. 获取价格数据
            formatted_code = self._format_stock_code(stock_code)
            history_table = f"{stock_name}_history"

            # 检查表是否存在
            check_query = """
            SELECT COUNT(*) as count
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_name = %s
            """
            cursor.execute(check_query, (history_table,))
            result = cursor.fetchone()

            if not result or result['count'] == 0:
                logger.warning(f"历史数据表 {history_table} 不存在")
                cursor.close()
                return None

            price_query = f"""
            SELECT `日期` as date, `收盘价` as close_price
            FROM `{history_table}`
            WHERE `日期` >= DATE_SUB(CURDATE(), INTERVAL {days} DAY)
            ORDER BY `日期` ASC
            """

            cursor.execute(price_query)
            price_data = cursor.fetchall()

            if not price_data:
                logger.warning(f"未找到{stock_name}的价格数据")
                return None

            # 转换为DataFrame
            price_df = pd.DataFrame(price_data)
            price_df['date'] = pd.to_datetime(price_df['date'])

            # 2. 获取情感评分数据
            sentiment_query = """
            SELECT
                DATE(news_datetime) as date,
                AVG(sentiment_score) as avg_sentiment,
                AVG(correlation_score) as avg_correlation,
                COUNT(*) as news_count
            FROM price_news_correlation
            WHERE stock_code = %s
                AND news_datetime >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            GROUP BY DATE(news_datetime)
            """

            cursor.execute(sentiment_query, (stock_code, days))
            sentiment_data = cursor.fetchall()

            cursor.close()

            # 3. 绘制双Y轴图表
            fig, ax1 = plt.subplots(figsize=(14, 7))

            # 左Y轴:价格
            ax1.set_xlabel('日期', fontsize=12)
            ax1.set_ylabel('股价(元)', color='tab:blue', fontsize=12)
            line1 = ax1.plot(price_df['date'], price_df['close_price'],
                           color='tab:blue', linewidth=2, label='收盘价')
            ax1.tick_params(axis='y', labelcolor='tab:blue')
            ax1.grid(True, alpha=0.3)

            # 右Y轴:情感评分
            ax2 = ax1.twinx()
            ax2.set_ylabel('情感评分', color='tab:red', fontsize=12)

            if sentiment_data:
                sentiment_df = pd.DataFrame(sentiment_data)
                sentiment_df['date'] = pd.to_datetime(sentiment_df['date'])

                # 绘制情感评分
                line2 = ax2.plot(sentiment_df['date'], sentiment_df['avg_sentiment'],
                               color='tab:red', linewidth=2, marker='o',
                               markersize=6, label='平均情感', alpha=0.7)

                # 添加情感强度柱状图(使用新闻数量表示)
                ax2.bar(sentiment_df['date'], sentiment_df['avg_correlation'],
                       width=0.5, alpha=0.3, color='orange', label='关联强度')

                ax2.tick_params(axis='y', labelcolor='tab:red')
                ax2.set_ylim(-1.1, 1.1)  # 情感评分范围

                # 添加零线
                ax2.axhline(y=0, color='gray', linestyle='--', alpha=0.5, linewidth=1)

                # 合并图例
                lines = line1 + line2
                labels = [l.get_label() for l in lines]
                ax1.legend(lines, labels, loc='upper left', fontsize=10)

            # 标题和格式
            plt.title(f'{stock_name}({stock_code}) 价格与情感双轴图 (近{days}天)', fontsize=14, fontweight='bold')

            # 格式化X轴日期
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            ax1.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, days//10)))
            plt.xticks(rotation=45)

            # 调整布局
            plt.tight_layout()

            # 保存图表
            if save_path is None:
                chart_dir = os.path.join('static', 'images', 'charts')
                os.makedirs(chart_dir, exist_ok=True)
                save_path = os.path.join(chart_dir, f"{stock_name}_price_sentiment.png")

            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            plt.close()

            logger.info(f"双Y轴图表生成成功: {save_path}")
            return save_path

        except Exception as e:
            logger.error(f"绘制双Y轴图表失败: {e}")
            import traceback
            traceback.print_exc()
            return None

    def plot_price_with_gpr_prediction(self, stock_code: str, stock_name: str, days: int = 30, save_path: str = None) -> str:
        """
        绘制带GPR预测阴影区的价格图表

        Args:
            stock_code: 股票代码
            stock_name: 股票名称
            days: 显示历史天数
            save_path: 保存路径

        Returns:
            图表文件路径
        """
        try:
            cursor = self.mysql_conn.cursor(dictionary=True)

            # 1. 获取历史价格数据
            history_table = f"{stock_name}_history"

            # 检查表是否存在
            check_query = """
            SELECT COUNT(*) as count
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_name = %s
            """
            cursor.execute(check_query, (history_table,))
            result = cursor.fetchone()

            if not result or result['count'] == 0:
                logger.warning(f"历史数据表 {history_table} 不存在")
                cursor.close()
                return None

            price_query = f"""
            SELECT `日期` as date, `收盘价` as close_price, `最高价` as high, `最低价` as low
            FROM `{history_table}`
            WHERE `日期` >= DATE_SUB(CURDATE(), INTERVAL {days} DAY)
            ORDER BY `日期` ASC
            """

            cursor.execute(price_query)
            price_data = cursor.fetchall()

            if not price_data:
                logger.warning(f"未找到{stock_name}的价格数据")
                cursor.close()
                return None

            price_df = pd.DataFrame(price_data)
            price_df['date'] = pd.to_datetime(price_df['date'])

            # 2. 获取GPR预测数据
            prediction_query = """
            SELECT target_date, predicted_price, price_lower_bound, price_upper_bound
            FROM stock_price_predictions
            WHERE stock_code = %s
                AND target_date >= CURDATE()
                AND prediction_date = (
                    SELECT MAX(prediction_date)
                    FROM stock_price_predictions
                    WHERE stock_code = %s
                )
            ORDER BY target_date ASC
            """

            cursor.execute(prediction_query, (stock_code, stock_code))
            prediction_data = cursor.fetchall()

            cursor.close()

            # 3. 绘制图表
            fig, ax = plt.subplots(figsize=(14, 7))

            # 绘制历史价格
            ax.plot(price_df['date'], price_df['close_price'],
                   color='#2E86DE', linewidth=2.5, label='历史收盘价', zorder=3)

            # 绘制高低价范围
            ax.fill_between(price_df['date'], price_df['low'], price_df['high'],
                           alpha=0.2, color='#2E86DE', label='日内波动范围')

            # 绘制GPR预测及置信区间
            if prediction_data:
                pred_df = pd.DataFrame(prediction_data)
                pred_df['target_date'] = pd.to_datetime(pred_df['target_date'])

                # 预测价格线
                ax.plot(pred_df['target_date'], pred_df['predicted_price'],
                       color='#E74C3C', linewidth=2.5, linestyle='--',
                       marker='o', markersize=7, label='GPR预测价格', zorder=3)

                # 置信区间阴影
                ax.fill_between(pred_df['target_date'],
                              pred_df['price_lower_bound'],
                              pred_df['price_upper_bound'],
                              alpha=0.3, color='#E74C3C', label='95%置信区间')

                # 标注预测值
                for idx, row in pred_df.iterrows():
                    ax.annotate(f"{row['predicted_price']:.2f}",
                              xy=(row['target_date'], row['predicted_price']),
                              xytext=(0, 10), textcoords='offset points',
                              fontsize=9, ha='center',
                              bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7))

            # 标题和标签
            plt.title(f'{stock_name}({stock_code}) 价格走势与GPR预测 (近{days}天+未来预测)',
                     fontsize=14, fontweight='bold')
            ax.set_xlabel('日期', fontsize=12)
            ax.set_ylabel('价格(元)', fontsize=12)

            # 图例
            ax.legend(loc='best', fontsize=10, framealpha=0.9)

            # 网格
            ax.grid(True, alpha=0.3, linestyle='--')

            # 格式化X轴
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            plt.xticks(rotation=45)

            # 添加当前时间分隔线
            today = datetime.now().date()
            ax.axvline(x=pd.Timestamp(today), color='gray', linestyle=':', linewidth=2, alpha=0.7, label='今日')

            # 调整布局
            plt.tight_layout()

            # 保存图表
            if save_path is None:
                chart_dir = os.path.join('static', 'images', 'charts')
                os.makedirs(chart_dir, exist_ok=True)
                save_path = os.path.join(chart_dir, f"{stock_name}_gpr_prediction.png")

            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            plt.close()

            logger.info(f"GPR预测图表生成成功: {save_path}")
            return save_path

        except Exception as e:
            logger.error(f"绘制GPR预测图表失败: {e}")
            import traceback
            traceback.print_exc()
            return None

    def plot_comprehensive_analysis(self, stock_code: str, stock_name: str, days: int = 30, save_path: str = None) -> str:
        """
        绘制综合分析图表:价格+情感+GPR预测

        三层图表:
        1. 价格走势 + GPR预测
        2. 情感评分
        3. 新闻-价格关联度
        """
        try:
            cursor = self.mysql_conn.cursor(dictionary=True)

            # 1. 获取历史价格
            history_table = f"{stock_name}_history"

            # 检查表是否存在
            check_query = """
            SELECT COUNT(*) as count
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_name = %s
            """
            cursor.execute(check_query, (history_table,))
            result = cursor.fetchone()

            if not result or result['count'] == 0:
                logger.warning(f"历史数据表 {history_table} 不存在")
                cursor.close()
                return None

            price_query = f"""
            SELECT `日期` as date, `收盘价` as close_price
            FROM `{history_table}`
            WHERE `日期` >= DATE_SUB(CURDATE(), INTERVAL {days} DAY)
            ORDER BY `日期` ASC
            """
            cursor.execute(price_query)
            price_data = cursor.fetchall()

            if not price_data:
                cursor.close()
                return None

            price_df = pd.DataFrame(price_data)
            price_df['date'] = pd.to_datetime(price_df['date'])

            # 2. 获取情感数据
            sentiment_query = """
            SELECT
                DATE(news_datetime) as date,
                AVG(sentiment_score) as avg_sentiment,
                AVG(correlation_score) as avg_correlation
            FROM price_news_correlation
            WHERE stock_code = %s AND news_datetime >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            GROUP BY DATE(news_datetime)
            """
            cursor.execute(sentiment_query, (stock_code, days))
            sentiment_data = cursor.fetchall()

            # 3. 获取GPR预测
            prediction_query = """
            SELECT target_date, predicted_price, price_lower_bound, price_upper_bound
            FROM stock_price_predictions
            WHERE stock_code = %s AND target_date >= CURDATE()
            ORDER BY target_date ASC
            """
            cursor.execute(prediction_query, (stock_code,))
            prediction_data = cursor.fetchall()

            cursor.close()

            # 创建三层子图
            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 10), sharex=True)

            # === 子图1: 价格 + GPR预测 ===
            ax1.plot(price_df['date'], price_df['close_price'],
                    color='#2E86DE', linewidth=2, label='历史价格')

            if prediction_data:
                pred_df = pd.DataFrame(prediction_data)
                pred_df['target_date'] = pd.to_datetime(pred_df['target_date'])

                ax1.plot(pred_df['target_date'], pred_df['predicted_price'],
                        color='#E74C3C', linewidth=2, linestyle='--', marker='o', label='GPR预测')

                ax1.fill_between(pred_df['target_date'],
                                pred_df['price_lower_bound'],
                                pred_df['price_upper_bound'],
                                alpha=0.3, color='#E74C3C')

            ax1.set_ylabel('价格(元)', fontsize=11)
            ax1.legend(loc='best', fontsize=9)
            ax1.grid(True, alpha=0.3)
            ax1.set_title(f'{stock_name}({stock_code}) 综合分析图表', fontsize=13, fontweight='bold')

            # === 子图2: 情感评分 ===
            if sentiment_data:
                sentiment_df = pd.DataFrame(sentiment_data)
                sentiment_df['date'] = pd.to_datetime(sentiment_df['date'])

                ax2.plot(sentiment_df['date'], sentiment_df['avg_sentiment'],
                        color='#27AE60', linewidth=2, marker='o', markersize=5)
                ax2.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
                ax2.fill_between(sentiment_df['date'], 0, sentiment_df['avg_sentiment'],
                                where=(sentiment_df['avg_sentiment'] > 0), alpha=0.3, color='green',
                                interpolate=True)
                ax2.fill_between(sentiment_df['date'], 0, sentiment_df['avg_sentiment'],
                                where=(sentiment_df['avg_sentiment'] < 0), alpha=0.3, color='red',
                                interpolate=True)

            ax2.set_ylabel('情感评分', fontsize=11)
            ax2.set_ylim(-1.1, 1.1)
            ax2.grid(True, alpha=0.3)

            # === 子图3: 新闻-价格关联度 ===
            if sentiment_data:
                ax3.bar(sentiment_df['date'], sentiment_df['avg_correlation'],
                       width=0.7, color='#F39C12', alpha=0.7)

            ax3.set_ylabel('关联强度', fontsize=11)
            ax3.set_xlabel('日期', fontsize=11)
            ax3.grid(True, alpha=0.3)

            # 格式化X轴
            ax3.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            plt.xticks(rotation=45)

            # 调整布局
            plt.tight_layout()

            # 保存图表
            if save_path is None:
                chart_dir = os.path.join('static', 'images', 'charts')
                os.makedirs(chart_dir, exist_ok=True)
                save_path = os.path.join(chart_dir, f"{stock_name}_comprehensive.png")

            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            plt.close()

            logger.info(f"综合分析图表生成成功: {save_path}")
            return save_path

        except Exception as e:
            logger.error(f"绘制综合分析图表失败: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _format_stock_code(self, code: str) -> str:
        """格式化股票代码"""
        if not code.startswith(('sh', 'sz')):
            if code.startswith('6'):
                return f'sh{code}'
            elif code.startswith(('0', '3')):
                return f'sz{code}'
        return code

    def close(self):
        """关闭数据库连接"""
        if self.mysql_conn:
            self.mysql_conn.close()


def main():
    """测试函数"""
    service = EnhancedChartService()

    try:
        # 测试双Y轴图表
        print("生成双Y轴图表...")
        service.plot_price_sentiment_dual_axis('600519', '贵州茅台', days=60)

        # 测试GPR预测图表
        print("生成GPR预测图表...")
        service.plot_price_with_gpr_prediction('600519', '贵州茅台', days=30)

        # 测试综合分析图表
        print("生成综合分析图表...")
        service.plot_comprehensive_analysis('600519', '贵州茅台', days=30)

        print("所有图表生成完成!")

    finally:
        service.close()


if __name__ == '__main__':
    main()
