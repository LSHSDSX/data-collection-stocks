#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
清空Redis中的旧情感分析数据
"""
import json
import redis
import os

def clear_old_sentiment_data():
    print("=" * 80)
    print("清空Redis中的旧情感分析数据")
    print("=" * 80)

    # 加载配置
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config', 'config.json')

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

    print("准备删除以下数据：")
    print("-" * 80)

    # 检查要删除的数据
    keys_to_delete = []

    # 1. 情感分析数据
    if r.exists('stock:news_all_analyses'):
        count = r.hlen('stock:news_all_analyses')
        print(f"1. stock:news_all_analyses (哈希表): {count} 条记录")
        keys_to_delete.append('stock:news_all_analyses')
    else:
        print("1. stock:news_all_analyses: 不存在")

    # 2. 已分析新闻标记
    if r.exists('stock:analyzed_news_hashes'):
        count = r.scard('stock:analyzed_news_hashes')
        print(f"2. stock:analyzed_news_hashes (集合): {count} 个哈希")
        keys_to_delete.append('stock:analyzed_news_hashes')
    else:
        print("2. stock:analyzed_news_hashes: 不存在")

    # 3. 其他可能的分析相关键
    pattern_keys = [
        'stock:news_analysis:*',
        'stock:analysis:*'
    ]

    for pattern in pattern_keys:
        matching_keys = r.keys(pattern)
        if matching_keys:
            print(f"3. 匹配 {pattern}: {len(matching_keys)} 个键")
            keys_to_delete.extend(matching_keys)

    print("-" * 80)

    if not keys_to_delete:
        print("\n✓ 没有需要删除的数据")
        return

    print(f"\n总共要删除 {len(keys_to_delete)} 个键")
    print("\n⚠️ 警告：此操作不可恢复！")
    confirm = input("确认删除？(输入 YES 继续): ").strip()

    if confirm != 'YES':
        print("✗ 已取消删除操作")
        return

    # 执行删除
    print("\n开始删除...")
    deleted_count = 0

    for key in keys_to_delete:
        try:
            r.delete(key)
            deleted_count += 1
            print(f"  ✓ 已删除: {key}")
        except Exception as e:
            print(f"  ✗ 删除失败 {key}: {e}")

    print("\n" + "=" * 80)
    print(f"✓ 删除完成！共删除 {deleted_count} 个键")
    print("=" * 80)

    print("\n现在可以重新运行分析：")
    print("  python reanalyze_news_redis.py")

    r.close()

if __name__ == '__main__':
    clear_old_sentiment_data()
