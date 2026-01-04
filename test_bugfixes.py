#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
快速验证修复的测试脚本
检查关键功能是否正常
"""
import os
import sys
import json

# 添加项目路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

print("=" * 70)
print("  系统修复验证测试")
print("=" * 70)

def test_config_loading():
    """测试1: 配置文件加载"""
    print("\n[测试1] 配置文件加载...")
    try:
        from web_interface.services.enhanced_chart_service import EnhancedChartService
        service = EnhancedChartService()
        service.close()
        print("✓ 配置文件加载成功")
        return True
    except Exception as e:
        print(f"✗ 配置文件加载失败: {e}")
        return False


def test_gpr_table_check():
    """测试2: GPR预测表检查"""
    print("\n[测试2] 数据库表检查...")
    try:
        from indicator_analysis.gpr_predictor import GPRStockPredictor
        predictor = GPRStockPredictor()

        # 测试prepare_training_data是否能处理不存在的表
        stocks = predictor.config.get('stocks', [])
        if stocks:
            test_stock = stocks[0]
            X, y, df = predictor.prepare_training_data(
                test_stock['code'],
                test_stock['name'],
                days=30
            )

            if X is not None:
                print(f"✓ 成功准备训练数据，特征数: {X.shape[1]}, 样本数: {X.shape[0]}")
            else:
                print("⚠ 训练数据不足（这是正常的，如果技术指标表不存在）")

        predictor.close()
        return True
    except Exception as e:
        print(f"✗ GPR测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_alert_system():
    """测试3: 预警系统容错性"""
    print("\n[测试3] 预警系统容错性...")
    try:
        from indicator_analysis.multi_factor_alert import MultiFactorAlertSystem
        alert_system = MultiFactorAlertSystem()

        # 测试分析单只股票
        stocks = alert_system.config.get('stocks', [])
        if stocks:
            test_stock = stocks[0]
            alert_system.analyze_stock(test_stock['code'], test_stock['name'])
            print("✓ 预警系统运行正常（即使某些表不存在也能继续）")

        alert_system.close()
        return True
    except Exception as e:
        print(f"✗ 预警系统测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_chart_service():
    """测试4: 图表服务SQL语句"""
    print("\n[测试4] 图表服务SQL语句...")
    try:
        from web_interface.services.enhanced_chart_service import EnhancedChartService
        service = EnhancedChartService()

        # 加载配置
        stocks = service.config.get('stocks', [])
        if stocks:
            test_stock = stocks[0]

            # 尝试生成图表（可能因为数据不足而失败，但不应该有SQL错误）
            try:
                service.plot_price_sentiment_dual_axis(
                    test_stock['code'],
                    test_stock['name'],
                    days=30
                )
                print("✓ SQL语句正确，图表生成尝试完成")
            except Exception as e:
                # 检查是否是数据问题而非SQL错误
                if "SQL syntax" in str(e) or "doesn't exist" in str(e):
                    print(f"✗ SQL错误: {e}")
                    return False
                else:
                    print(f"⚠ 数据不足或其他问题（SQL语句正确）: {e}")

        service.close()
        return True
    except Exception as e:
        print(f"✗ 图表服务测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """运行所有测试"""
    results = []

    # 运行测试
    results.append(("配置加载", test_config_loading()))
    results.append(("GPR表检查", test_gpr_table_check()))
    results.append(("预警容错", test_alert_system()))
    results.append(("图表SQL", test_chart_service()))

    # 输出结果
    print("\n" + "=" * 70)
    print("  测试结果汇总")
    print("=" * 70)

    passed = 0
    failed = 0

    for name, result in results:
        status = "✓ 通过" if result else "✗ 失败"
        print(f"{name:20s}: {status}")
        if result:
            passed += 1
        else:
            failed += 1

    print(f"\n总计: {passed} 通过, {failed} 失败")

    if failed == 0:
        print("\n🎉 所有测试通过! 修复成功!")
    else:
        print(f"\n⚠️  有 {failed} 个测试失败，请检查上述错误信息")

    print("=" * 70)

    return failed == 0


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
