#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查预警和情感数据
"""
import json
import mysql.connector
import redis
import os
from datetime import datetime, timedelta

def check_alerts_and_sentiment():
    # 加载配置
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config', 'config.json')

    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    print("=" * 80)
    print("检查预警和情感数据")
    print("=" * 80)

    # 1. 检查MySQL预警数据
    print("\n【1】检查预警数据（MySQL）")
    print("-" * 80)
    try:
        conn = mysql.connector.connect(
            host=config['mysql_config']['host'],
            user=config['mysql_config']['user'],
            password=config['mysql_config']['password'],
            database=config['mysql_config']['database']
        )
        cursor = conn.cursor(dictionary=True)

        # 检查表是否存在
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_name = 'multi_factor_alerts'
        """)
        table_exists = cursor.fetchone()['count'] > 0

        if not table_exists:
            print("✗ multi_factor_alerts 表不存在")
        else:
            # 查询总记录数
            cursor.execute("SELECT COUNT(*) as count FROM multi_factor_alerts")
            total_count = cursor.fetchone()['count']
            print(f"✓ multi_factor_alerts 表存在，共 {total_count} 条记录")

            # 查询7天内的记录
            cursor.execute("""
                SELECT COUNT(*) as count FROM multi_factor_alerts
                WHERE alert_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            """)
            recent_count = cursor.fetchone()['count']
            print(f"  - 7天内的记录: {recent_count} 条")

            # 按股票统计
            cursor.execute("""
                SELECT stock_code, stock_name, COUNT(*) as count,
                       MAX(alert_time) as latest_time
                FROM multi_factor_alerts
                WHERE alert_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                GROUP BY stock_code, stock_name
                ORDER BY count DESC
            """)
            by_stock = cursor.fetchall()

            if by_stock:
                print("  - 按股票统计（7天内）:")
                for row in by_stock:
                    print(f"    {row['stock_name']}({row['stock_code']}): "
                          f"{row['count']} 条，最新时间: {row['latest_time']}")
            else:
                print("  - 7天内没有预警记录")

            # 显示最新的3条预警
            cursor.execute("""
                SELECT stock_code, stock_name, alert_type, alert_level,
                       alert_message, alert_time
                FROM multi_factor_alerts
                ORDER BY alert_time DESC
                LIMIT 3
            """)
            latest = cursor.fetchall()

            if latest:
                print("\n  - 最新的3条预警:")
                for row in latest:
                    print(f"    [{row['alert_time']}] {row['stock_name']} - "
                          f"{row['alert_level']}: {row['alert_message']}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"✗ 检查预警数据失败: {e}")

    # 2. 检查Redis情感数据
    print("\n【2】检查情感分析数据（Redis）")
    print("-" * 80)
    try:
        redis_config = config.get('redis_config', {})
        r = redis.Redis(
            host=redis_config.get('host', '127.0.0.1'),
            port=redis_config.get('port', 6379),
            db=redis_config.get('db', 0),
            password=redis_config.get('password'),
            decode_responses=True
        )

        # 检查Redis连接
        r.ping()
        print("✓ Redis连接成功")

        # 检查情感分析数据
        sentiment_count = r.hlen('stock:news_all_analyses')
        print(f"✓ 情感分析数据: {sentiment_count} 条记录")

        if sentiment_count > 0:
            # 随机取几条看看
            sample_keys = r.hkeys('stock:news_all_analyses')[:3]
            print(f"\n  - 情感分析数据示例（前3条）:")
            for key in sample_keys:
                value = r.hget('stock:news_all_analyses', key)
                if value:
                    try:
                        data = json.loads(value)
                        sentiment = data.get('sentiment', 0)
                        print(f"    哈希: {key[:30]}...")
                        print(f"    情感: {sentiment:.2f}")
                    except:
                        pass

        # 检查新闻数据
        news_count = r.llen('stock:hot_news')
        print(f"\n✓ 新闻数据: {news_count} 条记录")

        if news_count > 0:
            # 获取最新一条新闻
            latest_news = r.lindex('stock:hot_news', 0)
            if latest_news:
                try:
                    news = json.loads(latest_news)
                    print(f"  - 最新新闻: {news.get('content', '')[:50]}...")
                    print(f"    时间: {news.get('datetime', '')}")
                except:
                    pass

        r.close()

    except Exception as e:
        print(f"✗ 检查Redis数据失败: {e}")

    print("\n" + "=" * 80)
    print("诊断完成")
    print("=" * 80)
    print("\n如果预警或情感数据为0，请运行：")
    print("  python indicator_analysis/multi_factor_alert.py")
    print("  python News_analysis/sentiment_analyzer.py --limit 100")
    print("\n如果数据存在但网页不显示，请：")
    print("  1. 按 Ctrl+Shift+R 硬刷新浏览器")
    print("  2. 按 F12 打开开发者工具，查看Console的错误信息")
    print("=" * 80)

if __name__ == '__main__':
    check_alerts_and_sentiment()
