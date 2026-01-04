#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据采集启动脚本
用于采集股票历史数据和新闻数据
"""
import sys
import os
import asyncio
import json
from datetime import datetime, timedelta

# 添加项目根目录到Python路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)


def clean_old_tables():
    """清理旧的数据表（如果表结构不正确）"""
    import mysql.connector

    print("检查并清理旧的数据表...")

    try:
        # 读取配置
        config_path = os.path.join(BASE_DIR, 'config', 'config.json')
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        mysql_config = config.get('mysql_config', {})
        stocks = config.get('stocks', [])

        # 连接MySQL
        conn = mysql.connector.connect(
            host=mysql_config.get('host', 'localhost'),
            port=mysql_config.get('port', 3306),
            user=mysql_config.get('user', 'root'),
            password=mysql_config.get('password', ''),
            database=mysql_config.get('database', 'stock_analysis')
        )
        cursor = conn.cursor()

        # 删除所有股票的历史数据表
        for stock in stocks:
            table_name = f"{stock['name']}_history"
            try:
                cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
                print(f"  已删除旧表: {table_name}")
            except Exception as e:
                print(f"  删除表 {table_name} 失败: {e}")

        conn.commit()
        cursor.close()
        conn.close()
        print("✓ 清理完成\n")

    except Exception as e:
        print(f"✗ 清理失败: {e}\n")


def collect_stock_history():
    """采集股票历史数据"""
    print("=" * 60)
    print("开始采集股票历史数据...")
    print("=" * 60)

    # 导入搜狐证券模块
    from data.搜狐证券 import fetch_stock_history_data, parse_history_data, save_to_database, get_stock_name_from_code

    # 读取配置文件获取股票列表
    config_path = os.path.join(BASE_DIR, 'config', 'config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    stocks = config.get('stocks', [])

    if not stocks:
        print("配置文件中没有股票列表")
        return

    # 设置日期范围（获取最近1年的数据）
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')

    success_count = 0
    fail_count = 0

    for stock in stocks:
        stock_code = stock['code']
        stock_name = stock['name']

        print(f"\n正在采集 {stock_name}({stock_code}) 的历史数据...")
        print(f"日期范围: {start_date} - {end_date}")

        try:
            # 构建搜狐证券的股票代码格式
            sohu_code = f"cn_{stock_code}"

            # 获取数据
            response_text = fetch_stock_history_data(sohu_code, start_date, end_date)
            if not response_text:
                print(f"  ✗ 获取 {stock_name} 数据失败")
                fail_count += 1
                continue

            # 解析数据
            history_data = parse_history_data(response_text)
            if not history_data:
                print(f"  ✗ 解析 {stock_name} 数据失败")
                fail_count += 1
                continue

            # 保存到数据库
            result = save_to_database(stock_name, history_data)
            if result:
                print(f"  ✓ {stock_name} 数据保存成功，共 {len(history_data)} 条记录")
                success_count += 1
            else:
                print(f"  ✗ {stock_name} 数据保存失败")
                fail_count += 1

        except Exception as e:
            print(f"  ✗ 处理 {stock_name} 时出错: {str(e)}")
            fail_count += 1

    print("\n" + "=" * 60)
    print(f"股票历史数据采集完成！")
    print(f"成功: {success_count} 只，失败: {fail_count} 只")
    print("=" * 60)


async def collect_news_data():
    """采集新闻数据"""
    print("\n" + "=" * 60)
    print("开始采集新闻数据...")
    print("=" * 60)

    try:
        # 导入新闻采集模块
        from data.hot_News_data import HotNewsStorage
        from News_crawler.新浪财经 import SinaSpider
        from News_crawler.同花顺 import ThsSpider
        from News_crawler.kr_36氪 import Kr36Spider
        from News_crawler.财联社 import ClsSpider

        # 读取配置文件
        config_path = os.path.join(BASE_DIR, 'config', 'config.json')
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        redis_config = config.get('redis_config', {})

        # 创建新闻存储实例
        storage = HotNewsStorage(
            host=redis_config.get('host', 'localhost'),
            port=redis_config.get('port', 6379),
            db=redis_config.get('db', 0),
            max_days=30
        )

        # 注册所有新闻爬虫
        print("注册新闻爬虫...")
        storage.register_spider(SinaSpider())
        storage.register_spider(ThsSpider())
        storage.register_spider(Kr36Spider())
        storage.register_spider(ClsSpider())

        # 获取新闻
        print("开始抓取新闻...")
        await storage.fetch_all_news()

        # 获取新闻数量
        news_count = await storage.get_news_count()
        print(f"\n✓ 新闻采集完成！Redis中共有 {news_count} 条新闻")

    except Exception as e:
        print(f"✗ 采集新闻数据时出错: {str(e)}")
        import traceback
        traceback.print_exc()

    print("=" * 60)


async def analyze_news():
    """分析新闻数据（使用AI）"""
    print("\n" + "=" * 60)
    print("开始分析新闻...")
    print("=" * 60)

    try:
        from News_analysis.news_stock_analysis import NewsStockAnalyzer

        print("初始化新闻分析器...")
        analyzer = NewsStockAnalyzer()

        print("开始AI分析（这可能需要几分钟）...")
        result = await analyzer.run_analysis(max_news=30, parallel_count=3)

        print("\n" + "=" * 60)
        print("✓ 新闻分析完成！")
        print(f"分析结果: {json.dumps(result, ensure_ascii=False, indent=2)}")
        print("=" * 60)

    except Exception as e:
        print(f"✗ 分析新闻时出错: {str(e)}")
        import traceback
        traceback.print_exc()


def main():
    """主函数"""
    print("\n" + "=" * 60)
    print("股票数据采集系统")
    print("=" * 60)

    while True:
        print("\n请选择要执行的操作：")
        print("1. 采集股票历史数据")
        print("2. 采集新闻数据")
        print("3. 分析新闻数据（使用AI）")
        print("4. 采集所有数据（历史 + 新闻 + 分析）")
        print("5. 清理旧数据表")
        print("0. 退出")

        choice = input("\n请输入选项 (0-5): ").strip()

        if choice == '1':
            clean_old_tables()  # 采集前先清理旧表
            collect_stock_history()
        elif choice == '2':
            asyncio.run(collect_news_data())
        elif choice == '3':
            asyncio.run(analyze_news())
        elif choice == '4':
            clean_old_tables()  # 采集前先清理旧表
            collect_stock_history()
            asyncio.run(collect_news_data())
            asyncio.run(analyze_news())  # 添加新闻分析
        elif choice == '5':
            clean_old_tables()
        elif choice == '0':
            print("退出程序")
            break
        else:
            print("无效的选项，请重新输入")


if __name__ == '__main__':
    main()
