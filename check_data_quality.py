#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查历史数据的质量（NULL值）
"""
import json
import mysql.connector
import pandas as pd
import os

def check_data_quality():
    # 加载配置
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config', 'config.json')

    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    # 连接MySQL
    conn = mysql.connector.connect(
        host=config['mysql_config']['host'],
        user=config['mysql_config']['user'],
        password=config['mysql_config']['password'],
        database=config['mysql_config']['database']
    )

    stocks = config.get('stocks', [])
    stock = stocks[0]  # 检查第一只股票
    stock_name = stock['name']
    table_name = f"{stock_name}_history"

    print("=" * 80)
    print(f"检查 {stock_name} 的数据质量")
    print("=" * 80)

    # 查询数据
    query = f"""
    SELECT
        `日期` as date,
        `收盘价` as close_price,
        `开盘价` as open_price,
        `最高价` as high_price,
        `最低价` as low_price,
        `成交量(手)` as volume,
        `涨跌幅(%)` as change_pct
    FROM `{table_name}`
    WHERE `日期` >= DATE_SUB(CURDATE(), INTERVAL 70 DAY)
    ORDER BY `日期` ASC
    """

    df = pd.read_sql(query, conn)

    print(f"\n总记录数: {len(df)}")
    print(f"\n数据示例（前5行）:")
    print(df.head())

    print(f"\n各列的NULL值统计:")
    print(df.isnull().sum())

    print(f"\n数据类型:")
    print(df.dtypes)

    # 检查是否有NaN
    print(f"\n删除NaN前的记录数: {len(df)}")
    df_clean = df.dropna()
    print(f"删除NaN后的记录数: {len(df_clean)}")

    if len(df) != len(df_clean):
        print(f"\n⚠️ 警告：删除了 {len(df) - len(df_clean)} 行数据")
        print("\n含有NaN的行:")
        null_rows = df[df.isnull().any(axis=1)]
        print(null_rows)
    else:
        print("\n✓ 数据完整，没有NaN值")

    # 检查数据范围
    print(f"\n数据日期范围:")
    print(f"  最早: {df['date'].min()}")
    print(f"  最晚: {df['date'].max()}")

    # 检查收盘价范围
    print(f"\n收盘价统计:")
    print(f"  最小: {df['close_price'].min()}")
    print(f"  最大: {df['close_price'].max()}")
    print(f"  平均: {df['close_price'].mean():.2f}")

    conn.close()

if __name__ == '__main__':
    check_data_quality()
