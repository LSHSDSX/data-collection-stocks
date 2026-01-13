#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
GPR(高斯过程回归)股价预测模型
整合价格、技术指标、新闻情感进行预测
"""
import json
import logging
import mysql.connector
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import os
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF, ConstantKernel as C, WhiteKernel
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gpr_prediction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class GPRStockPredictor:
    """基于高斯过程回归的股价预测器"""

    def __init__(self, config_path=None):
        """初始化预测器"""
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

        # 数据标准化器
        self.scaler_X = StandardScaler()
        self.scaler_y = StandardScaler()

        # GPR模型
        self.gpr_model = None

        # 预测参数
        self.prediction_days = 5  # 预测未来5天
        self.training_window = 60  # 使用过去60天的数据训练

        # 创建预测结果表
        self.create_prediction_table()

    def load_config(self):
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            logger.info("配置文件加载成功")
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            raise

    def create_prediction_table(self):
        """创建股价预测结果表"""
        try:
            cursor = self.mysql_conn.cursor()

            create_table_sql = """
            CREATE TABLE IF NOT EXISTS stock_price_predictions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
                stock_name VARCHAR(50) COMMENT '股票名称',
                prediction_date DATE NOT NULL COMMENT '预测日期',
                target_date DATE NOT NULL COMMENT '目标日期(预测的是哪一天)',
                predicted_price DECIMAL(10,4) COMMENT '预测价格',
                price_lower_bound DECIMAL(10,4) COMMENT '价格预测下界(95%置信区间)',
                price_upper_bound DECIMAL(10,4) COMMENT '价格预测上界(95%置信区间)',
                prediction_std DECIMAL(10,4) COMMENT '预测标准差',
                actual_price DECIMAL(10,4) COMMENT '实际价格(用于回测)',
                model_version VARCHAR(50) COMMENT '模型版本',
                feature_importance TEXT COMMENT '特征重要性(JSON)',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_stock_target (stock_code, target_date),
                INDEX idx_prediction_date (prediction_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股价GPR预测结果表';
            """

            cursor.execute(create_table_sql)
            self.mysql_conn.commit()
            logger.info("GPR预测结果表创建成功")
            cursor.close()

        except Exception as e:
            logger.error(f"创建预测表失败: {e}")

    def prepare_training_data(self, stock_code: str, stock_name: str, days: int = 60) -> Tuple[Optional[np.ndarray], Optional[np.ndarray], Optional[pd.DataFrame]]:
        """准备训练数据:整合价格、技术指标、情感评分"""
        try:
            cursor = self.mysql_conn.cursor(dictionary=True)

            # 1. 获取历史价格和技术指标
            formatted_code = self._format_stock_code(stock_code)
            history_table = f"{stock_name}_history"
            technical_table = f"technical_indicators_{stock_name}"

            # 首先检查表是否存在
            check_query = """
            SELECT
                SUM(CASE WHEN table_name = %s THEN 1 ELSE 0 END) as history_exists,
                SUM(CASE WHEN table_name = %s THEN 1 ELSE 0 END) as technical_exists
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            """
            cursor.execute(check_query, (history_table, technical_table))
            table_check = cursor.fetchone()

            if not table_check['history_exists']:
                logger.warning(f"历史数据表 {history_table} 不存在")
                cursor.close()
                return None, None, None

            # 查询历史数据 - 使用反引号包裹表名和列名
            if table_check['technical_exists']:
                query = f"""
                SELECT
                    h.`日期` as date,
                    h.`收盘价` as close_price,
                    h.`开盘价` as open_price,
                    h.`最高价` as high_price,
                    h.`最低价` as low_price,
                    h.`成交量(手)` as volume,
                    h.`涨跌幅(%)` as change_pct,
                    t.MACD, t.MACD_Hist, t.`Signal`,
                    t.RSI, t.MA5, t.MA10, t.MA20,
                    t.Upper_Band, t.Lower_Band
                FROM `{history_table}` h
                LEFT JOIN `{technical_table}` t ON h.`日期` = t.`日期`
                WHERE h.`日期` >= DATE_SUB(CURDATE(), INTERVAL {days + 10} DAY)
                ORDER BY h.`日期` ASC
                """
            else:
                # 如果技术指标表不存在，只查询历史价格
                logger.warning(f"技术指标表 {technical_table} 不存在，仅使用价格数据")
                query = f"""
                SELECT
                    `日期` as date,
                    `收盘价` as close_price,
                    `开盘价` as open_price,
                    `最高价` as high_price,
                    `最低价` as low_price,
                    `成交量(手)` as volume,
                    `涨跌幅(%)` as change_pct
                FROM `{history_table}`
                WHERE `日期` >= DATE_SUB(CURDATE(), INTERVAL {days + 10} DAY)
                ORDER BY `日期` ASC
                """

            cursor.execute(query)
            price_data = cursor.fetchall()

            if not price_data or len(price_data) < 30:
                logger.warning(f"股票 {stock_name} 的历史数据不足")
                cursor.close()
                return None, None, None

            # 转换为DataFrame
            df = pd.DataFrame(price_data)
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date')

            # 2. 获取新闻情感评分(从price_news_correlation表)
            sentiment_query = """
            SELECT
                DATE(news_datetime) as date,
                AVG(sentiment_score) as avg_sentiment,
                COUNT(*) as news_count,
                AVG(correlation_score) as avg_correlation
            FROM price_news_correlation
            WHERE stock_code = %s
                AND news_datetime >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            GROUP BY DATE(news_datetime)
            """

            cursor.execute(sentiment_query, (stock_code, days + 10))
            sentiment_data = cursor.fetchall()

            # 转换为DataFrame
            if sentiment_data:
                sentiment_df = pd.DataFrame(sentiment_data)
                sentiment_df['date'] = pd.to_datetime(sentiment_df['date'])
                sentiment_df = sentiment_df.set_index('date')

                # 合并情感数据
                df = df.join(sentiment_df, how='left')

            # 填充缺失的情感数据
            df['avg_sentiment'] = df.get('avg_sentiment', pd.Series(dtype=float)).fillna(0)
            df['news_count'] = df.get('news_count', pd.Series(dtype=float)).fillna(0)
            df['avg_correlation'] = df.get('avg_correlation', pd.Series(dtype=float)).fillna(0)

            cursor.close()

            # 3. 构建特征矩阵
            # 只删除核心价格列有NaN的行（不删除技术指标或情感列的NaN）
            core_columns = ['close_price', 'open_price', 'high_price', 'low_price', 'volume']
            df = df.dropna(subset=core_columns)

            if len(df) < 30:
                logger.warning(f"股票 {stock_name} 清理后数据不足")
                return None, None, None

            # 填充可选列的NaN值
            if 'change_pct' in df.columns:
                df['change_pct'] = df['change_pct'].fillna(0)

            # 选择特征 - 优先使用技术指标，如果没有就只用价格
            base_features = ['open_price', 'high_price', 'low_price', 'volume']
            if 'change_pct' in df.columns and df['change_pct'].notna().any():
                base_features.append('change_pct')

            technical_features = ['MACD', 'MACD_Hist', 'Signal', 'RSI', 'MA5', 'MA10', 'MA20']
            sentiment_features = ['avg_sentiment', 'news_count', 'avg_correlation']

            # 构建可用特征列表
            available_features = base_features.copy()

            # 添加存在的技术指标（且有有效数据）
            for feat in technical_features:
                if feat in df.columns and df[feat].notna().sum() > 0:
                    # 填充NaN值
                    df[feat] = df[feat].fillna(df[feat].mean() if df[feat].notna().any() else 0)
                    available_features.append(feat)

            # 添加存在的情感特征（已经在前面fillna了，所以不需要再检查）
            for feat in sentiment_features:
                if feat in df.columns:
                    available_features.append(feat)

            # 确保至少有基本特征
            if len(available_features) < 3:
                logger.warning(f"股票 {stock_name} 可用特征太少: {available_features}")
                return None, None, None

            X = df[available_features].values
            y = df['close_price'].values

            # 确保数据类型为float
            X = X.astype(np.float64)
            y = y.astype(np.float64)

            # 最后检查并填充任何剩余的NaN值
            if np.isnan(X).any():
                logger.warning(f"特征矩阵中检测到NaN值，使用0填充")
                X = np.nan_to_num(X, nan=0.0)
            if np.isnan(y).any():
                logger.warning(f"目标值中检测到NaN值，使用均值填充")
                y = np.nan_to_num(y, nan=np.nanmean(y))

            logger.info(f"准备训练数据完成: {stock_name}, 样本数: {len(X)}, 特征数: {len(available_features)}")
            logger.info(f"使用特征: {available_features}")

            return X, y, df

        except Exception as e:
            logger.error(f"准备训练数据失败: {e}")
            import traceback
            traceback.print_exc()
            return None, None, None

    def _format_stock_code(self, code: str) -> str:
        """格式化股票代码"""
        if not code.startswith(('sh', 'sz')):
            if code.startswith('6'):
                return f'sh{code}'
            elif code.startswith(('0', '3')):
                return f'sz{code}'
        return code

    def train_gpr_model(self, X: np.ndarray, y: np.ndarray) -> bool:
        """训练GPR模型"""
        try:
            # 数据标准化
            X_scaled = self.scaler_X.fit_transform(X)
            y_scaled = self.scaler_y.fit_transform(y.reshape(-1, 1)).ravel()

            # 定义核函数
            # RBF核 + 常数核 + 白噪声核
            kernel = C(1.0, (1e-3, 1e3)) * RBF(length_scale=1.0, length_scale_bounds=(1e-2, 1e2)) + WhiteKernel(noise_level=0.1)

            # 创建GPR模型
            self.gpr_model = GaussianProcessRegressor(
                kernel=kernel,
                n_restarts_optimizer=10,
                alpha=1e-6,
                normalize_y=True
            )

            # 训练模型
            logger.info("开始训练GPR模型...")
            self.gpr_model.fit(X_scaled, y_scaled)

            logger.info(f"GPR模型训练完成, 优化后的核函数: {self.gpr_model.kernel_}")

            return True

        except Exception as e:
            logger.error(f"训练GPR模型失败: {e}")
            import traceback
            traceback.print_exc()
            return False

    def predict_future_prices(self, stock_code: str, stock_name: str, days: int = 5) -> Optional[List[Dict]]:
        """预测未来几天的价格"""
        try:
            # 准备训练数据
            X, y, df = self.prepare_training_data(stock_code, stock_name, self.training_window)

            if X is None or len(X) < 30:
                logger.warning(f"股票 {stock_name} 数据不足,无法预测")
                return None

            # 训练模型
            if not self.train_gpr_model(X, y):
                return None

            # 准备预测特征
            # 使用最近的数据作为预测起点
            last_features = X[-1:].copy()

            predictions = []
            prediction_date = datetime.now().date()

            for day_offset in range(1, days + 1):
                # 标准化特征
                X_pred_scaled = self.scaler_X.transform(last_features)

                # 进行预测
                y_pred_scaled, sigma_scaled = self.gpr_model.predict(X_pred_scaled, return_std=True)

                # 反标准化
                y_pred = self.scaler_y.inverse_transform(y_pred_scaled.reshape(-1, 1))[0, 0]
                sigma = sigma_scaled[0] * self.scaler_y.scale_[0]

                # 计算95%置信区间
                lower_bound = y_pred - 1.96 * sigma
                upper_bound = y_pred + 1.96 * sigma

                target_date = prediction_date + timedelta(days=day_offset)

                predictions.append({
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'prediction_date': prediction_date,
                    'target_date': target_date,
                    'predicted_price': round(float(y_pred), 4),
                    'price_lower_bound': round(float(lower_bound), 4),
                    'price_upper_bound': round(float(upper_bound), 4),
                    'prediction_std': round(float(sigma), 4)
                })

                # 更新特征用于下一天预测(简化版,实际应该根据预测结果更新)
                # 这里我们假设其他特征保持不变,只更新价格相关特征
                last_features = last_features.copy()

                logger.info(f"预测 {stock_name} {target_date}: {y_pred:.2f} ± {1.96*sigma:.2f}")

            return predictions

        except Exception as e:
            logger.error(f"预测失败: {e}")
            import traceback
            traceback.print_exc()
            return None

    def save_predictions(self, predictions: List[Dict]):
        """保存预测结果到数据库"""
        try:
            cursor = self.mysql_conn.cursor()

            for pred in predictions:
                # 检查是否已存在
                check_sql = """
                SELECT id FROM stock_price_predictions
                WHERE stock_code = %s AND target_date = %s AND prediction_date = %s
                """
                cursor.execute(check_sql, (
                    pred['stock_code'],
                    pred['target_date'],
                    pred['prediction_date']
                ))

                existing = cursor.fetchone()

                if existing:
                    # 更新
                    update_sql = """
                    UPDATE stock_price_predictions SET
                    predicted_price = %s,
                    price_lower_bound = %s,
                    price_upper_bound = %s,
                    prediction_std = %s
                    WHERE id = %s
                    """
                    cursor.execute(update_sql, (
                        pred['predicted_price'],
                        pred['price_lower_bound'],
                        pred['price_upper_bound'],
                        pred['prediction_std'],
                        existing[0]
                    ))
                else:
                    # 插入
                    insert_sql = """
                    INSERT INTO stock_price_predictions
                    (stock_code, stock_name, prediction_date, target_date,
                     predicted_price, price_lower_bound, price_upper_bound, prediction_std)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_sql, (
                        pred['stock_code'],
                        pred['stock_name'],
                        pred['prediction_date'],
                        pred['target_date'],
                        pred['predicted_price'],
                        pred['price_lower_bound'],
                        pred['price_upper_bound'],
                        pred['prediction_std']
                    ))

            self.mysql_conn.commit()
            logger.info(f"保存了 {len(predictions)} 条预测结果")
            cursor.close()

        except Exception as e:
            logger.error(f"保存预测结果失败: {e}")
            self.mysql_conn.rollback()

    def predict_all_stocks(self):
        """预测所有配置的股票"""
        try:
            stocks = self.config.get('stocks', [])
            other_stocks = self.config.get('other_stocks', [])
            all_stocks = stocks + other_stocks

            logger.info(f"开始预测 {len(all_stocks)} 只股票")

            for stock in all_stocks:
                code = stock['code']
                name = stock['name']

                logger.info(f"\n{'='*60}")
                logger.info(f"预测股票: {name}({code})")
                logger.info(f"{'='*60}")

                predictions = self.predict_future_prices(code, name, self.prediction_days)

                if predictions:
                    self.save_predictions(predictions)

                    # 打印预测结果
                    print(f"\n{name}({code}) 未来{self.prediction_days}天预测:")
                    for pred in predictions:
                        print(f"  {pred['target_date']}: "
                              f"{pred['predicted_price']:.2f} "
                              f"[{pred['price_lower_bound']:.2f}, {pred['price_upper_bound']:.2f}]")
                else:
                    logger.warning(f"股票 {name}({code}) 预测失败")

            logger.info("\n所有股票预测完成!")

        except Exception as e:
            logger.error(f"预测所有股票失败: {e}")

    def close(self):
        """关闭数据库连接"""
        if self.mysql_conn:
            self.mysql_conn.close()


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='GPR股价预测')
    parser.add_argument('--stock', type=str, help='指定股票代码')
    parser.add_argument('--days', type=int, default=5, help='预测天数')
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("启动GPR股价预测系统")
    logger.info("=" * 60)

    predictor = GPRStockPredictor()

    try:
        if args.stock:
            # 预测单只股票
            stocks = predictor.config.get('stocks', []) + predictor.config.get('other_stocks', [])
            stock_info = next((s for s in stocks if s['code'] == args.stock), None)

            if stock_info:
                predictor.prediction_days = args.days
                predictions = predictor.predict_future_prices(stock_info['code'], stock_info['name'], args.days)

                if predictions:
                    predictor.save_predictions(predictions)
            else:
                logger.error(f"未找到股票代码: {args.stock}")
        else:
            # 预测所有股票
            predictor.prediction_days = args.days
            predictor.predict_all_stocks()

    finally:
        predictor.close()

    logger.info("GPR预测系统结束")


if __name__ == '__main__':
    main()
