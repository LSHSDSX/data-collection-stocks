#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
系统优化阶段完整运行脚本
按顺序运行所有优化模块
"""
import os
import sys
import logging
import argparse
from datetime import datetime
import asyncio

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# 设置日志
# 修改点: 添加 encoding='utf-8' 防止日志文件乱码
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('system_optimization.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def print_header(title):
    """打印标题"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70 + "\n")


def run_stage_1():
    """阶段1: 数据基础 - LLM深度情感评分和异构关联算法"""
    print_header("阶段1: 数据基础")

    # 1. 深度情感分析
    logger.info("1.1 运行LLM深度情感分析...")
    
    # -------------------------------------------------------------------------
    # 修改点: 重构异步调用逻辑，解决 Event loop is closed 错误
    # -------------------------------------------------------------------------
    try:
        from News_analysis.sentiment_analyzer import DeepSentimentAnalyzer

        # 定义一个内部异步函数，管理完整的生命周期
        async def _execute_sentiment_analysis():
            analyzer = DeepSentimentAnalyzer()
            try:
                # 1. 初始化 (绑定到当前事件循环)
                await analyzer.initialize()
                
                # 2. 执行分析
                logger.info("开始分析最近的 30 条新闻...")
                await analyzer.analyze_all_news(limit=30)
                
            finally:
                # 3. 关闭资源 (在同一个事件循环中关闭)
                await analyzer.close()

        # 只调用一次 asyncio.run
        asyncio.run(_execute_sentiment_analysis())

        # 修改点: 替换特殊符号 ✓ 为 [OK]
        logger.info("[OK] 深度情感分析完成")

    except Exception as e:
        # 修改点: 替换特殊符号 ✗ 为 [ERROR]
        logger.error(f"[ERROR] 深度情感分析失败: {e}")
        import traceback
        traceback.print_exc()

    # 2. 异构关联分析
    logger.info("\n1.2 运行价格-新闻异构关联分析...")
    try:
        from News_analysis.price_news_correlator import PriceNewsCorrelator

        correlator = PriceNewsCorrelator()
        correlator.analyze_all_stocks()

        logger.info("[OK] 异构关联分析完成")
    except Exception as e:
        logger.error(f"[ERROR] 异构关联分析失败: {e}")


def run_stage_2():
    """阶段2: 预测与预警 - GPR建模和多因子预警"""
    print_header("阶段2: 预测与预警")

    # 1. GPR价格预测
    logger.info("2.1 运行GPR股价预测...")
    try:
        from indicator_analysis.gpr_predictor import GPRStockPredictor

        predictor = GPRStockPredictor()
        predictor.predict_all_stocks()
        predictor.close()

        logger.info("[OK] GPR价格预测完成")
    except Exception as e:
        logger.error(f"[ERROR] GPR价格预测失败: {e}")

    # 2. 多因子预警
    logger.info("\n2.2 运行多因子预警系统...")
    try:
        from indicator_analysis.multi_factor_alert import MultiFactorAlertSystem

        alert_system = MultiFactorAlertSystem()
        alert_system.analyze_all_stocks()
        alert_system.close()

        logger.info("[OK] 多因子预警完成")
    except Exception as e:
        logger.error(f"[ERROR] 多因子预警失败: {e}")


def run_stage_3():
    """阶段3: 可视化 - 图表生成"""
    print_header("阶段3: 可视化")

    logger.info("3.1 生成增强版图表...")
    try:
        from web_interface.services.enhanced_chart_service import EnhancedChartService
        import json

        # 加载配置获取股票列表
        config_path = os.path.join(project_root, 'config', 'config.json')
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        stocks = config.get('stocks', [])
        service = EnhancedChartService()

        # 限制只生成前3个，避免测试时间过长
        process_stocks = stocks[:3]
        
        for stock in process_stocks: 
            code = stock['code']
            name = stock['name']

            logger.info(f"生成 {name}({code}) 的图表...")

            # 双Y轴图表
            service.plot_price_sentiment_dual_axis(code, name, days=30)

            # GPR预测图表
            service.plot_price_with_gpr_prediction(code, name, days=30)

            # 综合分析图表
            service.plot_comprehensive_analysis(code, name, days=30)

        service.close()
        logger.info("[OK] 图表生成完成")

    except Exception as e:
        logger.error(f"[ERROR] 图表生成失败: {e}")
        import traceback
        traceback.print_exc()


def run_all_stages():
    """运行所有优化阶段"""
    start_time = datetime.now()

    print("\n" + "=" * 70)
    print("  股票分析系统优化 - 完整流程")
    print("  开始时间: " + start_time.strftime('%Y-%m-%d %H:%M:%S'))
    print("=" * 70)

    try:
        # 阶段1: 数据基础
        run_stage_1()

        # 阶段2: 预测与预警
        run_stage_2()

        # 阶段3: 可视化
        run_stage_3()

    except KeyboardInterrupt:
        logger.warning("\n用户中断执行")
    except Exception as e:
        logger.error(f"\n[ERROR] 执行过程中发生错误: {e}")
        import traceback
        traceback.print_exc()

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print("\n" + "=" * 70)
    print(f"  完成时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  总耗时: {duration:.2f} 秒")
    print("=" * 70 + "\n")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='股票分析系统优化运行脚本')
    parser.add_argument('--stage', type=int, choices=[1, 2, 3],
                        help='指定运行阶段 (1=数据基础, 2=预测预警, 3=可视化)')
    parser.add_argument('--all', action='store_true',
                        help='运行所有阶段')

    args = parser.parse_args()

    # 如果没有指定参数，默认运行所有
    if args.all or (not args.stage):
        run_all_stages()
    elif args.stage == 1:
        run_stage_1()
    elif args.stage == 2:
        run_stage_2()
    elif args.stage == 3:
        run_stage_3()


if __name__ == '__main__':
    main()