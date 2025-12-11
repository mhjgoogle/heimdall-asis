#!/usr/bin/env python3
"""查看DuckDB数据库中的数据"""

import sys
from pathlib import Path

# 添加src到路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.adapters.duckdb_storage import DuckDBStorage
from src.adapters.config import Config

def main():
    """查看DuckDB中的数据"""
    config = Config()

    print(f"连接到DuckDB数据库: {config.DUCKDB_PATH}")

    # 创建DuckDB存储实例（只读模式）
    storage = DuckDBStorage(config.DUCKDB_PATH, read_only=True)

    with storage:
        print("\n=== 数据库表信息 ===")

        # 获取所有表
        try:
            all_tables_df = storage.query_to_dataframe("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
            all_tables = all_tables_df['table_name'].tolist()
            print(f"数据库中的表: {all_tables}")
        except Exception as e:
            print(f"获取表列表时出错: {e}")
            all_tables = []

        # 检查所有表
        tables = ['bronze_meta_mapping', 'silver_meta_mapping', 'gold_meta_mapping']

        for table in tables:
            if storage.table_exists(table):
                row_count = storage.get_table_row_count(table)
                schema = storage.get_table_schema(table)
                print(f"\n表: {table}")
                print(f"行数: {row_count}")
                print("模式:")
                if schema:
                    for col, dtype in schema.items():
                        print(f"  - {col}: {dtype}")
                else:
                    print("  无模式信息")

                # 如果有数据，显示前5行
                if row_count > 0:
                    print(f"\n前5行数据 ({table}):")
                    try:
                        df = storage.query_to_dataframe(f"SELECT * FROM {table} LIMIT 5")
                        print(df.to_string(index=False))
                    except Exception as e:
                        print(f"查询数据时出错: {e}")
            else:
                print(f"\n表: {table} - 不存在")

        print("\n=== 自定义查询示例 ===")
        print("你可以使用以下方式进行自定义查询:")
        print("1. 使用DuckDB CLI: duckdb data/asis.duckdb")
        print("2. 在DuckDB CLI中运行: SELECT * FROM bronze_meta_mapping LIMIT 10;")
        print("3. 修改此脚本添加更多查询")

if __name__ == "__main__":
    main()