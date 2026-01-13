#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试所有功能是否正常
"""
import mysql.connector
import redis
import json
import os

def test_database_connection():
    """测试数据库连接"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, 'config', 'config.json')

        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        conn = mysql.connector.connect(**config['mysql_config'])
        print("✓ MySQL连接成功")
        conn.close()
        return True
    except Exception as e:
        print(f"✗ MySQL连接失败: {e}")
        return False

def test_redis_connection():
    """测试Redis连接"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, 'config', 'config.json')

        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        redis_config = config['redis_config']
        r = redis.Redis(
            host=redis_config.get('host', 'localhost'),
            port=redis_config.get('port', 6379),
            db=redis_config.get('db', 0),
            password=redis_config.get('password'),
            decode_responses=True
        )
        r.ping()
        print("✓ Redis连接成功")
        return True
    except Exception as e:
        print(f"✗ Redis连接失败: {e}")
        return False

def test_alert_data():
    """测试预警数据"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, 'config', 'config.json')

        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        conn = mysql.connector.connect(**config['mysql_config'])
        cursor = conn.cursor()

        # 检查表是否存在
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_name = 'multi_factor_alerts'
        """)

        if cursor.fetchone()[0] == 0:
            print("✗ 预警数据: 表不存在（需要运行 multi_factor_alert.py）")
            cursor.close()
            conn.close()
            return False

        cursor.execute("SELECT COUNT(*) FROM multi_factor_alerts")
        count = cursor.fetchone()[0]

        if count > 0:
            print(f"✓ 预警数据: {count} 条记录")
            cursor.execute("""
                SELECT stock_code, stock_name, COUNT(*) as count
                FROM multi_factor_alerts
                GROUP BY stock_code, stock_name
                ORDER BY count DESC
                LIMIT 5
            """)
            print("  最多预警的股票:")
            for row in cursor.fetchall():
                print(f"    - {row[1]}({row[0]}): {row[2]} 条预警")
        else:
            print("✗ 预警数据: 0 条记录（需要运行 multi_factor_alert.py）")

        cursor.close()
        conn.close()
        return count > 0
    except Exception as e:
        print(f"✗ 检查预警数据失败: {e}")
        return False

def test_gpr_data():
    """测试GPR预测数据"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, 'config', 'config.json')

        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        conn = mysql.connector.connect(**config['mysql_config'])
        cursor = conn.cursor()

        # 检查表是否存在
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_name = 'stock_price_predictions'
        """)

        if cursor.fetchone()[0] == 0:
            print("✗ GPR预测数据: 表不存在（需要运行 gpr_predictor.py）")
            cursor.close()
            conn.close()
            return False

        cursor.execute("SELECT COUNT(*) FROM stock_price_predictions")
        count = cursor.fetchone()[0]

        if count > 0:
            print(f"✓ GPR预测数据: {count} 条记录")
            cursor.execute("""
                SELECT stock_code, stock_name, COUNT(*) as count
                FROM stock_price_predictions
                GROUP BY stock_code, stock_name
                ORDER BY count DESC
                LIMIT 5
            """)
            print("  有预测的股票:")
            for row in cursor.fetchall():
                print(f"    - {row[1]}({row[0]}): {row[2]} 条预测")

            # 显示最新预测时间
            cursor.execute("""
                SELECT MAX(prediction_time) as latest
                FROM stock_price_predictions
            """)
            latest = cursor.fetchone()[0]
            if latest:
                print(f"  最新预测时间: {latest}")
        else:
            print("✗ GPR预测数据: 0 条记录（需要运行 gpr_predictor.py）")

        cursor.close()
        conn.close()
        return count > 0
    except Exception as e:
        print(f"✗ 检查GPR数据失败: {e}")
        return False

def test_sentiment_data():
    """测试情感分析数据"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, 'config', 'config.json')

        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        redis_config = config['redis_config']
        r = redis.Redis(
            host=redis_config.get('host', 'localhost'),
            port=redis_config.get('port', 6379),
            db=redis_config.get('db', 0),
            password=redis_config.get('password'),
            decode_responses=True
        )

        count = r.hlen('stock:news_all_analyses')

        if count > 0:
            print(f"✓ 情感分析数据: {count} 条记录")
        else:
            print("✗ 情感分析数据: 0 条记录（需要运行 sentiment_analyzer.py）")

        return count > 0
    except Exception as e:
        print(f"✗ 检查情感数据失败: {e}")
        return False

def test_news_data():
    """测试新闻数据"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, 'config', 'config.json')

        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        redis_config = config['redis_config']
        r = redis.Redis(
            host=redis_config.get('host', 'localhost'),
            port=redis_config.get('port', 6379),
            db=redis_config.get('db', 0),
            password=redis_config.get('password'),
            decode_responses=True
        )

        count = r.llen('stock:hot_news')

        if count > 0:
            print(f"✓ 新闻数据: {count} 条记录")

            # 获取最新一条新闻
            latest_news = r.lindex('stock:hot_news', 0)
            if latest_news:
                news = json.loads(latest_news)
                print(f"  最新新闻: {news.get('content', '')[:50]}...")
        else:
            print("✗ 新闻数据: 0 条记录（需要运行新闻爬虫）")

        return count > 0
    except Exception as e:
        print(f"✗ 检查新闻数据失败: {e}")
        return False

def test_stock_data():
    """测试股票实时数据"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, 'config', 'config.json')

        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        conn = mysql.connector.connect(**config['mysql_config'])
        cursor = conn.cursor()

        # 获取第一只股票
        stocks = config.get('stocks', [])
        if not stocks:
            print("✗ 配置文件中没有股票")
            cursor.close()
            conn.close()
            return False

        first_stock = stocks[0]
        stock_code = first_stock['code']

        # 格式化股票代码
        if not stock_code.startswith(('sh', 'sz')):
            if stock_code.startswith('6'):
                formatted_code = f'sh{stock_code}'
            elif stock_code.startswith(('0', '3')):
                formatted_code = f'sz{stock_code}'
            else:
                formatted_code = stock_code
        else:
            formatted_code = stock_code

        table_name = f"stock_{formatted_code}_realtime"

        # 检查表是否存在
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_name = %s
        """, (table_name,))

        if cursor.fetchone()[0] == 0:
            print(f"✗ 股票实时数据: 表 {table_name} 不存在（需要运行 stock_real_data.py）")
            cursor.close()
            conn.close()
            return False

        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        count = cursor.fetchone()[0]

        if count > 0:
            print(f"✓ 股票实时数据: {first_stock['name']} 有 {count} 条记录")

            # 获取最新数据时间
            cursor.execute(f"SELECT MAX(`时间`) FROM `{table_name}`")
            latest = cursor.fetchone()[0]
            if latest:
                print(f"  最新数据时间: {latest}")
        else:
            print(f"✗ 股票实时数据: {first_stock['name']} 0 条记录（需要运行 stock_real_data.py）")

        cursor.close()
        conn.close()
        return count > 0
    except Exception as e:
        print(f"✗ 检查股票数据失败: {e}")
        return False

if __name__ == '__main__':
    print("="*60)
    print("测试系统功能")
    print("="*60)
    print()

    results = {
        'MySQL连接': test_database_connection(),
        'Redis连接': test_redis_connection(),
        '股票实时数据': test_stock_data(),
        '新闻数据': test_news_data(),
        '情感分析数据': test_sentiment_data(),
        'GPR预测数据': test_gpr_data(),
        '预警数据': test_alert_data(),
    }

    print()
    print("="*60)
    print("测试结果汇总")
    print("="*60)

    for name, result in results.items():
        status = "✓ 通过" if result else "✗ 失败"
        print(f"{name}: {status}")

    all_passed = all(results.values())

    print()
    if all_passed:
        print("✓✓✓ 所有测试通过！系统功能正常 ✓✓✓")
    else:
        print("✗✗✗ 部分测试失败，请根据上述信息修复 ✗✗✗")
        print()
        print("修复建议（按顺序执行）:")
        print()

        if not results['股票实时数据']:
            print("  1. 采集股票实时数据:")
            print("     python data/stock_real_data.py")
            print()

        if not results['新闻数据']:
            print("  2. 采集新闻数据:")
            print("     python News_crawler/财联社.py")
            print()

        if not results['情感分析数据']:
            print("  3. 运行情感分析:")
            print("     python News_analysis/sentiment_analyzer.py --limit 100")
            print()

        if not results['GPR预测数据']:
            print("  4. 运行GPR预测:")
            print("     python indicator_analysis/gpr_predictor.py --days 5")
            print()

        if not results['预警数据']:
            print("  5. 运行预警分析:")
            print("     python indicator_analysis/multi_factor_alert.py")
            print()

        print("或者一键运行所有脚本:")
        print("     python run_data_collection.py")
