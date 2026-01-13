#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
专门检查情感徽章显示问题
"""
import json
import redis
import os
import sys

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from News_analysis.news_stock_analysis import NewsStockAnalyzer

def check_sentiment_display():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config', 'config.json')

    print("=" * 80)
    print("检查情感徽章显示问题")
    print("=" * 80)

    # 加载配置
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    redis_config = config.get('redis_config', {})

    # 连接Redis
    try:
        r = redis.Redis(
            host=redis_config.get('host', '127.0.0.1'),
            port=redis_config.get('port', 6379),
            db=redis_config.get('db', 0),
            password=redis_config.get('password'),
            decode_responses=True
        )
        r.ping()
        print("✓ Redis连接成功\n")
    except Exception as e:
        print(f"✗ Redis连接失败: {e}")
        return

    # 1. 检查新闻数据
    print("【1】检查新闻数据")
    print("-" * 80)
    news_count = r.llen('stock:hot_news')
    print(f"新闻总数: {news_count} 条")

    if news_count == 0:
        print("✗ 没有新闻数据！")
        print("  请运行: python News_crawler/财联社.py")
        return

    # 获取前3条新闻
    news_list = r.lrange('stock:hot_news', 0, 2)
    print(f"\n前3条新闻:")
    for i, news_json in enumerate(news_list):
        news = json.loads(news_json)
        print(f"  {i+1}. {news.get('content', '')[:50]}...")
        print(f"     时间: {news.get('datetime', '')}")

    # 2. 检查情感分析数据
    print("\n【2】检查情感分析数据")
    print("-" * 80)
    sentiment_count = r.hlen('stock:news_all_analyses')
    print(f"情感分析总数: {sentiment_count} 条")

    if sentiment_count == 0:
        print("✗ 没有情感分析数据！")
        print("  请运行: python News_analysis/sentiment_analyzer.py --limit 100")
        return

    # 3. 测试新闻哈希匹配
    print("\n【3】测试新闻与情感数据的匹配")
    print("-" * 80)

    # 创建分析器
    analyzer = NewsStockAnalyzer(config_path)

    matched = 0
    unmatched = 0

    print("\n检查前10条新闻的情感匹配情况:")
    news_list = r.lrange('stock:hot_news', 0, 9)
    for i, news_json in enumerate(news_list):
        news_data = json.loads(news_json)

        # 生成哈希
        news_hash = analyzer.generate_news_hash(news_data)

        # 检查是否有情感分析
        analysis_result = r.hget('stock:news_all_analyses', news_hash)

        print(f"\n新闻 #{i+1}:")
        print(f"  内容: {news_data.get('content', '')[:40]}...")
        print(f"  哈希: {news_hash[:30]}...")

        if analysis_result:
            matched += 1
            try:
                analysis = json.loads(analysis_result)
                sentiment = analysis.get('sentiment', 0)

                # 判断情感类型
                if sentiment >= 0.3:
                    sentiment_type = "↑ 正面"
                    badge_color = "绿色"
                elif sentiment <= -0.3:
                    sentiment_type = "↓ 负面"
                    badge_color = "红色"
                else:
                    sentiment_type = "━ 中性"
                    badge_color = "灰色"

                print(f"  ✓ 有情感分析: {sentiment_type} {sentiment:.2f} ({badge_color}徽章)")
            except Exception as e:
                print(f"  ✗ 情感数据解析错误: {e}")
        else:
            unmatched += 1
            print(f"  ✗ 没有情感分析")

    print("\n" + "=" * 80)
    print(f"匹配统计: {matched} 条有情感, {unmatched} 条无情感")
    print("=" * 80)

    if matched == 0:
        print("\n❌ 问题诊断：新闻和情感数据不匹配！")
        print("\n可能原因:")
        print("  1. 情感分析使用的新闻数据和当前新闻数据不同")
        print("  2. 新闻内容被修改导致哈希值改变")
        print("  3. 新闻爬取时间和情感分析时间不一致")
        print("\n解决方案:")
        print("  重新运行情感分析:")
        print("  python News_analysis/sentiment_analyzer.py --limit 100")
    elif matched < len(news_list) / 2:
        print("\n⚠️ 警告：大部分新闻没有情感分析")
        print("\n建议:")
        print("  运行情感分析补充数据:")
        print("  python News_analysis/sentiment_analyzer.py --limit 100")
    else:
        print("\n✓ 情感数据匹配正常")
        print("\n如果网页还是不显示徽章，请:")
        print("  1. 按 Ctrl+Shift+R 硬刷新浏览器")
        print("  2. 按 F12 打开开发者工具 → Console标签")
        print("  3. 查看是否有JavaScript错误")
        print("  4. 在Console中输入: localStorage.clear() 然后刷新")

    r.close()

if __name__ == '__main__':
    check_sentiment_display()
