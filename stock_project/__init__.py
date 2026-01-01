import pymysql

# 1. 模拟 MySQLdb 模块
pymysql.install_as_MySQLdb()

# 2. 绕过 Django 对 MySQL 8.0.11 的版本检查
try:
    from django.db.backends.mysql.base import DatabaseWrapper
    
    def patched_check_database_version_supported(self):
        # 直接返回，不再抛出 NotSupportedError
        return 

    # 替换原有的检查逻辑
    DatabaseWrapper.check_database_version_supported = patched_check_database_version_supported
    print("成功应用 MySQL 5.7 兼容性补丁")
except Exception as e:
    print(f"应用补丁失败: {e}")