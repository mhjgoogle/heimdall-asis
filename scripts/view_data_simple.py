#!/usr/bin/env python3
"""简单查看DuckDB数据的脚本"""

import duckdb
from pathlib import Path

def main():
    db_path = Path("data/asis.duckdb")

    if not db_path.exists():
        print(f"数据库文件不存在: {db_path}")
        return

    print(f"连接到数据库: {db_path}")

    try:
        conn = duckdb.connect(str(db_path), read_only=True)

        # 获取所有表
        tables_result = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()

        if not tables_result:
            print("数据库中没有表")
            return

        print(f"\n找到表:")
        for table_name, in tables_result:
            print(f"- {table_name}")

            # 获取行数
            try:
                count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                count = count_result[0]
                print(f"  行数: {count}")

                # 如果有数据，显示前3行
                if count > 0:
                    print("  前3行数据:")
                    result = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchdf()
                    print(result.to_string(index=False))
                    print()
            except Exception as e:
                print(f"  查询表时出错: {e}")
                print()

        conn.close()

    except Exception as e:
        print(f"连接数据库时出错: {e}")

if __name__ == "__main__":
    main()