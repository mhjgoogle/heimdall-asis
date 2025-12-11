#!/usr/bin/env python3
"""将Parquet文件数据加载到DuckDB中"""

import sys
from pathlib import Path
import pandas as pd
import glob

# 添加src到路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.adapters.duckdb_storage import DuckDBStorage
from src.adapters.config import Config

def load_latest_parquet_to_duckdb():
    """将最新的parquet文件加载到DuckDB"""

    # 查找最新的parquet文件
    parquet_files = glob.glob('data/output/meta_mapping_results_*.parquet')
    if not parquet_files:
        print("没有找到parquet文件")
        return

    latest_file = max(parquet_files)
    print(f"加载文件: {latest_file}")

    # 读取parquet文件
    df = pd.read_parquet(latest_file)
    print(f"数据形状: {df.shape}")
    print(f"列: {list(df.columns)}")

    # 重命名列以匹配数据库模式
    column_mapping = {
        'nb_series': 'series_count',
        'dimension_names': 'dimensions',
        'indexed_at': 'extracted_at'
    }
    df = df.rename(columns=column_mapping)

    # 添加缺失的列
    df['source'] = 'api'
    df['processed_at'] = pd.Timestamp.now()
    df['quality_score'] = 1.0
    df['created_at'] = pd.Timestamp.now()
    df['updated_at'] = pd.Timestamp.now()
    df['is_active'] = True

    print(f"处理后的数据形状: {df.shape}")
    print(f"处理后的列: {list(df.columns)}")

    # 连接到DuckDB
    config = Config()
    storage = DuckDBStorage(config.DUCKDB_PATH)

    with storage:
        print("连接到DuckDB成功")

        # 保存到Gold层
        try:
            success = storage.save_gold_meta_mapping(df)
            if success:
                print("✓ 成功保存到Gold层")
            else:
                print("✗ 保存到Gold层失败")
        except Exception as e:
            print(f"保存失败: {e}")

            # 尝试手动插入
            print("尝试手动插入...")
            try:
                # 直接使用pandas to_sql方法
                df.to_sql('gold_meta_mapping_manual', storage._conn, if_exists='replace', index=False)
                print("✓ 手动插入成功")
            except Exception as e2:
                print(f"手动插入也失败: {e2}")

    print("完成")

if __name__ == "__main__":
    load_latest_parquet_to_duckdb()