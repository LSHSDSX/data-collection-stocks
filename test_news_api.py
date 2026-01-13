#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试新闻API是否返回情感数据
"""
import requests
import json

def test_news_api():
    print("=" * 80)
    print("测试新闻API")
    print("=" * 80)

    # 测试API
    url = "http://127.0.0.1:8010/api/news/"

    try:
        response = requests.get(url, params={'page': 1, 'page_size': 5})

        if response.status_code != 200:
            print(f"✗ API请求失败: {response.status_code}")
            return

        data = response.json()

        print(f"✓ API请求成功")
        print(f"  状态: {data.get('status')}")
        print(f"  新闻数量: {len(data.get('data', []))}")

        # 检查前5条新闻
        news_list = data.get('data', [])

        if not news_list:
            print("\n✗ 没有新闻数据")
            return

        print(f"\n检查前{len(news_list)}条新闻:")
        print("-" * 80)

        has_sentiment = 0
        no_sentiment = 0

        for i, news in enumerate(news_list):
            print(f"\n新闻 #{i+1}:")
            print(f"  内容: {news.get('content', '')[:50]}...")
            print(f"  来源: {news.get('source')}")
            print(f"  时间: {news.get('datetime')}")

            # 检查analysis_result
            if 'analysis_result' in news and news['analysis_result']:
                sentiment = news['analysis_result'].get('sentiment')

                if sentiment is None:
                    no_sentiment += 1
                    print(f"  ✗ 有analysis_result但sentiment为None（数据不完整）")
                    print(f"    → 不会显示徽章")
                else:
                    has_sentiment += 1
                    print(f"  ✓ 有情感分析: {sentiment}")

                    # 判断情感类型
                    if sentiment >= 0.3:
                        print(f"    → 徽章应该是: 绿色 ↑ 正面 {int(sentiment*100)}%")
                    elif sentiment <= -0.3:
                        print(f"    → 徽章应该是: 红色 ↓ 负面 {int(sentiment*100)}%")
                    else:
                        print(f"    → 徽章应该是: 灰色 ━ 中性 {int(sentiment*100)}%")
            else:
                no_sentiment += 1
                print(f"  ✗ 没有情感分析")
                print(f"    → 不会显示徽章")

        print("\n" + "=" * 80)
        print(f"统计: {has_sentiment} 条有情感, {no_sentiment} 条无情感")
        print("=" * 80)

        if has_sentiment == 0:
            print("\n❌ 所有新闻都没有情感分析！")
            print("\n解决方案:")
            print("  1. 运行情感分析:")
            print("     python News_analysis/sentiment_analyzer.py --limit 100")
            print("\n  2. 确认新闻和情感数据匹配:")
            print("     python check_sentiment_display.py")
        elif no_sentiment > 0:
            print(f"\n⚠️ 有 {no_sentiment} 条新闻没有情感分析")
            print("\n建议:")
            print("  运行情感分析补充数据:")
            print("  python News_analysis/sentiment_analyzer.py --limit 100")
        else:
            print("\n✓ 所有新闻都有情感分析！")
            print("\n如果网页还是不显示徽章:")
            print("  1. 按 Ctrl+Shift+R 硬刷新浏览器")
            print("  2. 按 F12 查看Console，应该显示'新闻有analysis_result'")
            print("  3. 检查是否有JavaScript错误")

    except requests.exceptions.ConnectionError:
        print("✗ 无法连接到服务器")
        print("  请确保Django服务器正在运行:")
        print("  python manage.py runserver 127.0.0.1:8010")
    except Exception as e:
        print(f"✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_news_api()
