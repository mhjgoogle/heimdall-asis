#!/usr/bin/env python3
"""运行完整的Pipeline演示，获取真实数据并保存到DuckDB"""

import sys
from pathlib import Path

# 添加src到路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.pipelines.dbnomics_map_pipeline import DbnomicsMapPipeline

def run_pipeline_demo():
    """运行pipeline演示，限制为前3个providers"""

    print("=== 运行Pipeline演示 ===")
    print("这将获取真实数据并保存到DuckDB")
    print("限制处理前3个新的providers以加快演示速度")
    print()

    # 创建pipeline实例，限制为3个providers
    pipeline = DbnomicsMapPipeline(
        provider_limit=3,  # 只处理前3个新的providers
        verbose=True
    )

    try:
        # 运行完整流程
        results = pipeline.run()

        print("\n=== Pipeline执行完成 ===")
        print(f"结果: {results}")

        # 获取摘要
        summary = pipeline.get_summary()
        print("\n执行摘要:")
        for key, value in summary.items():
            print(f"  {key}: {value}")

    except Exception as e:
        print(f"Pipeline执行失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_pipeline_demo()