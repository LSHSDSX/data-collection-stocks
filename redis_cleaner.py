#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Redis数据清理工具（可选择性清理）
"""
import json
import redis
import os

def show_redis_status():
    """显示Redis当前状态"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config', 'config.json')

    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    redis_config = config.get('redis_config', {})

    r = redis.Redis(
        host=redis_config.get('host', '127.0.0.1'),
        port=redis_config.get('port', 6379),
        db=redis_config.get('db', 0),
        password=redis_config.get('password'),
        decode_responses=True
    )

    print("=" * 80)
    print("Redis数据状态")
    print("=" * 80)

    # 新闻数据
    news_count = r.llen('stock:hot_news')
    print(f"\n1. 新闻数据 (stock:hot_news): {news_count} 条")

    # 情感分析数据
    analysis_count = r.hlen('stock:news_all_analyses')
    print(f"2. 情感分析数据 (stock:news_all_analyses): {analysis_count} 条")

    # 已分析标记
    analyzed_count = r.scard('stock:analyzed_news_hashes') if r.exists('stock:analyzed_news_hashes') else 0
    print(f"3. 已分析标记 (stock:analyzed_news_hashes): {analyzed_count} 个")

    # 其他键
    all_keys = r.keys('stock:*')
    other_keys = [k for k in all_keys if k not in ['stock:hot_news', 'stock:news_all_analyses', 'stock:analyzed_news_hashes']]
    if other_keys:
        print(f"\n其他stock相关键 ({len(other_keys)}个):")
        for key in other_keys[:10]:
            print(f"  - {key}")
        if len(other_keys) > 10:
            print(f"  ... 还有 {len(other_keys) - 10} 个")

    r.close()

def clear_sentiment_only():
    """只清空情感分析数据，保留新闻"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config', 'config.json')

    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    redis_config = config.get('redis_config', {})

    r = redis.Redis(
        host=redis_config.get('host', '127.0.0.1'),
        port=redis_config.get('port', 6379),
        db=redis_config.get('db', 0),
        password=redis_config.get('password'),
        decode_responses=True
    )

    print("\n" + "=" * 80)
    print("清空情感分析数据（保留新闻）")
    print("=" * 80)

    deleted = 0

    # 删除情感分析数据
    if r.exists('stock:news_all_analyses'):
        count = r.hlen('stock:news_all_analyses')
        r.delete('stock:news_all_analyses')
        print(f"✓ 已删除 stock:news_all_analyses ({count} 条记录)")
        deleted += 1

    # 删除已分析标记
    if r.exists('stock:analyzed_news_hashes'):
        count = r.scard('stock:analyzed_news_hashes')
        r.delete('stock:analyzed_news_hashes')
        print(f"✓ 已删除 stock:analyzed_news_hashes ({count} 个标记)")
        deleted += 1

    if deleted == 0:
        print("✓ 没有需要删除的数据")
    else:
        print(f"\n✓ 删除完成！共删除 {deleted} 个键")
        print("✓ 新闻数据已保留")

    r.close()

def clear_all_stock_data():
    """清空所有stock相关数据"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config', 'config.json')

    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    redis_config = config.get('redis_config', {})

    r = redis.Redis(
        host=redis_config.get('host', '127.0.0.1'),
        port=redis_config.get('port', 6379),
        db=redis_config.get('db', 0),
        password=redis_config.get('password'),
        decode_responses=True
    )

    print("\n" + "=" * 80)
    print("清空所有stock相关数据")
    print("=" * 80)

    all_keys = r.keys('stock:*')
    print(f"\n找到 {len(all_keys)} 个键")

    if not all_keys:
        print("✓ 没有需要删除的数据")
        r.close()
        return

    print("\n⚠️ 警告：将删除所有stock相关数据（包括新闻）！")
    confirm = input("确认删除？(输入 YES 继续): ").strip()

    if confirm != 'YES':
        print("✗ 已取消删除操作")
        r.close()
        return

    deleted = 0
    for key in all_keys:
        try:
            r.delete(key)
            deleted += 1
            print(f"  ✓ 已删除: {key}")
        except Exception as e:
            print(f"  ✗ 删除失败 {key}: {e}")

    print(f"\n✓ 删除完成！共删除 {deleted} 个键")

    r.close()

def main():
    try:
        while True:
            show_redis_status()

            print("\n" + "=" * 80)
            print("请选择操作：")
            print("=" * 80)
            print("1. 只清空情感分析数据（保留新闻）【推荐】")
            print("2. 清空所有stock相关数据（包括新闻）")
            print("3. 刷新状态显示")
            print("0. 退出")

            choice = input("\n请输入选项 (0-3): ").strip()

            if choice == '1':
                confirm = input("\n确认只删除情感分析数据？(y/n): ").strip().lower()
                if confirm == 'y':
                    clear_sentiment_only()
                    print("\n现在可以重新运行分析：")
                    print("  python reanalyze_news_redis.py")
                    input("\n按回车键继续...")
            elif choice == '2':
                clear_all_stock_data()
                print("\n现在需要重新采集新闻和分析：")
                print("  python run_data_collection.py")
                input("\n按回车键继续...")
            elif choice == '3':
                continue
            elif choice == '0':
                print("退出程序")
                break
            else:
                print("无效的选项，请重新输入")
                input("\n按回车键继续...")

    except KeyboardInterrupt:
        print("\n\n程序已中断")
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
