#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查历史数据表的记录数量
"""
import json
import mysql.connector
import os

def check_history_data():
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
    cursor = conn.cursor()

    stocks = config.get('stocks', [])

    print("=" * 80)
    print("检查历史数据表")
    print("=" * 80)

    for stock in stocks:
        stock_name = stock['name']
        table_name = f"{stock_name}_history"

        # 检查表是否存在
        check_query = """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
        AND table_name = %s
        """
        cursor.execute(check_query, (table_name,))
        table_exists = cursor.fetchone()[0] > 0

        if not table_exists:
            print(f"✗ {stock_name}({stock['code']}): 表 {table_name} 不存在")
            continue

        # 查询记录数
        count_query = f"SELECT COUNT(*) FROM `{table_name}`"
        cursor.execute(count_query)
        count = cursor.fetchone()[0]

        # 查询日期范围
        if count > 0:
            date_query = f"SELECT MIN(`日期`), MAX(`日期`) FROM `{table_name}`"
            cursor.execute(date_query)
            min_date, max_date = cursor.fetchone()

            status = "✓" if count >= 30 else "✗"
            print(f"{status} {stock_name}({stock['code']}): {count} 条记录 "
                  f"[{min_date} 至 {max_date}]")
        else:
            print(f"✗ {stock_name}({stock['code']}): 0 条记录")

    cursor.close()
    conn.close()

    print("\n" + "=" * 80)
    print("说明：GPR预测需要至少30条历史记录")
    print("如果记录不足，请运行：python run_data_collection.py")
    print("=" * 80)

if __name__ == '__main__':
    check_history_data()
