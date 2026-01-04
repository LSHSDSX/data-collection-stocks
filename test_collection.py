#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
简化的数据采集测试脚本
"""
import sys
import os
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

def test_single_stock():
    """测试单只股票的数据采集"""
    from data.搜狐证券 import fetch_stock_history_data, parse_history_data, save_to_database

    # 测试东阿阿胶
    stock_code = "cn_000423"
    stock_name = "东阿阿胶"

    print(f"测试采集 {stock_name} 的历史数据...")

    # 获取最近30天的数据
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')

    # 1. 获取数据
    print("1. 获取数据...")
    response = fetch_stock_history_data(stock_code, start_date, end_date)
    if not response:
        print("❌ 获取数据失败")
        return False
    print("✓ 数据获取成功")

    # 2. 解析数据
    print("2. 解析数据...")
    history_data = parse_history_data(response)
    if not history_data:
        print("❌ 数据解析失败")
        return False
    print(f"✓ 解析成功，共 {len(history_data)} 条记录")
    print(f"  示例数据: {history_data[0]}")

    # 3. 保存到数据库
    print("3. 保存到数据库...")
    result = save_to_database(stock_name, history_data)
    if result:
        print(f"✓ 数据保存成功")
        return True
    else:
        print("❌ 数据保存失败")
        return False

if __name__ == '__main__':
    print("=" * 60)
    print("简化测试：采集单只股票数据")
    print("=" * 60)
    test_single_stock()
