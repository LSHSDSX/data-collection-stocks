#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
快速修复所有功能
一键生成预警、GPR预测、情感分析数据
"""
import subprocess
import sys
import os

def run_script(script_path, description, args=None):
    """运行Python脚本"""
    print(f"\n{'='*60}")
    print(f"正在执行: {description}")
    print(f"{'='*60}\n")

    cmd = [sys.executable, script_path]
    if args:
        cmd.extend(args)

    try:
        result = subprocess.run(cmd, check=True, capture_output=False, text=True)
        print(f"\n✓ {description} 完成")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n✗ {description} 失败: {e}")
        return False
    except Exception as e:
        print(f"\n✗ {description} 出错: {e}")
        return False

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))

    print("="*60)
    print("快速修复工具 - 生成所有缺失数据")
    print("="*60)
    print()
    print("这个脚本将依次运行:")
    print("  1. 股票实时数据采集")
    print("  2. 新闻数据采集")
    print("  3. 情感分析")
    print("  4. GPR预测")
    print("  5. 多因子预警")
    print()

    input("按回车键继续，或 Ctrl+C 取消...")
    print()

    results = {}

    # 1. 采集股票实时数据
    script_path = os.path.join(script_dir, 'data', 'stock_real_data.py')
    results['股票实时数据'] = run_script(script_path, '采集股票实时数据')

    # 2. 采集新闻数据
    script_path = os.path.join(script_dir, 'News_crawler', '财联社.py')
    results['新闻数据'] = run_script(script_path, '采集财联社新闻')

    # 3. 运行情感分析
    script_path = os.path.join(script_dir, 'News_analysis', 'sentiment_analyzer.py')
    results['情感分析'] = run_script(script_path, '运行情感分析', ['--limit', '100'])

    # 4. 运行GPR预测
    script_path = os.path.join(script_dir, 'indicator_analysis', 'gpr_predictor.py')
    results['GPR预测'] = run_script(script_path, '运行GPR预测', ['--days', '5'])

    # 5. 运行多因子预警
    script_path = os.path.join(script_dir, 'indicator_analysis', 'multi_factor_alert.py')
    results['多因子预警'] = run_script(script_path, '运行多因子预警')

    # 显示结果
    print()
    print("="*60)
    print("执行结果汇总")
    print("="*60)

    for name, success in results.items():
        status = "✓ 成功" if success else "✗ 失败"
        print(f"{name}: {status}")

    all_success = all(results.values())

    print()
    if all_success:
        print("✓✓✓ 所有脚本执行成功！✓✓✓")
        print()
        print("现在可以:")
        print("  1. 刷新网页查看最新数据")
        print("  2. 点击'📢 预警历史'查看预警")
        print("  3. 进入股票详情页查看GPR预测")
        print("  4. 在新闻页查看情感分析")
    else:
        print("✗✗✗ 部分脚本执行失败 ✗✗✗")
        print()
        print("请检查:")
        print("  1. 数据库连接是否正常")
        print("  2. Redis是否运行")
        print("  3. 网络连接是否正常（爬虫需要）")
        print("  4. 查看上方的错误信息")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n操作已取消")
    except Exception as e:
        print(f"\n\n发生错误: {e}")
        import traceback
        traceback.print_exc()
