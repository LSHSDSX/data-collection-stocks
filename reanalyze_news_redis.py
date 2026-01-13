#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
重新分析新闻并保存到Redis（带sentiment字段）
"""
import sys
import os
import asyncio

# 添加项目路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

async def analyze_news_with_sentiment():
    """使用修复后的news_stock_analysis分析新闻"""
    print("=" * 80)
    print("重新分析新闻数据（生成带sentiment的Redis数据）")
    print("=" * 80)

    try:
        from News_analysis.news_stock_analysis import NewsStockAnalyzer

        print("\n初始化新闻分析器...")
        analyzer = NewsStockAnalyzer()

        print("开始AI分析（这可能需要几分钟）...")
        print("提示：现在的分析会包含sentiment字段\n")

        # 分析最近50条新闻
        result = await analyzer.run_analysis(max_news=50, parallel_count=1)

        print("\n" + "=" * 80)
        print("✓ 新闻分析完成！")
        print("=" * 80)

        # 显示结果
        if result:
            import json
            print(f"分析结果摘要:")
            if 'processed_count' in result:
                print(f"  处理新闻数: {result['processed_count']}")
            if 'analysis_time' in result:
                print(f"  分析用时: {result['analysis_time']}")

        print("\n现在请运行测试脚本验证:")
        print("  python test_news_api.py")

    except Exception as e:
        print(f"✗ 分析新闻时出错: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(analyze_news_with_sentiment())
