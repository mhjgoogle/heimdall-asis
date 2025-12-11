#!/usr/bin/env python3
"""运行Core模块功能演示"""

import sys
from pathlib import Path
import pandas as pd

# 添加src到路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.core.dbnomics_map import (
    get_required_input_columns,
    filter_input_columns,
    extract_input_provider_codes,
    identify_new_providers,
    build_dimension_lookup,
    create_metadata_dataframe,
    verify_output
)

def demo_core_functions():
    """演示Core模块的主要功能"""

    print("=== Core模块功能演示 ===\n")

    # 1. 创建测试输入数据
    print("1. 创建测试输入数据")
    input_data = pd.DataFrame({
        'provider_code': ['ECB', 'FED', 'BOE', 'ECB'],
        'dataset_code': ['EXR', 'GPD', 'CPI', 'INT'],
        'dataset_name': ['Exchange Rates', 'GDP Data', 'Consumer Price Index', 'Interest Rates'],
        'dimension_values': ['["EUR", "USD"]', '["USA"]', '["UK"]', '["EUR"]'],
        'frequency': ['D', 'Q', 'M', 'D']
    })
    print("输入数据:")
    print(input_data)
    print()

    # 2. 演示列过滤
    print("2. 演示列过滤功能")
    required_cols = get_required_input_columns()
    print(f"必需列: {required_cols}")

    filtered_df = filter_input_columns(input_data)
    print("过滤后的数据:")
    print(filtered_df)
    print()

    # 3. 提取provider codes
    print("3. 提取Provider Codes")
    provider_codes = extract_input_provider_codes(input_data)
    print(f"提取的provider codes: {provider_codes}")
    print()

    # 4. 模拟API providers数据
    print("4. 模拟API Providers数据")
    api_providers = {
        'ECB': {'code': 'ECB', 'name': 'European Central Bank'},
        'FED': {'code': 'FED', 'name': 'Federal Reserve'},
        'BOE': {'code': 'BOE', 'name': 'Bank of England'},
        'BUBA': {'code': 'BUBA', 'name': 'Bundesbank'},
        'BOJ': {'code': 'BOJ', 'name': 'Bank of Japan'}
    }
    print(f"API providers: {list(api_providers.keys())}")
    print()

    # 5. 识别新providers
    print("5. 识别新Providers")
    new_providers = identify_new_providers(api_providers, provider_codes)
    print(f"新providers: {list(new_providers.keys())}")
    print()

    # 6. 构建维度查找表
    print("6. 构建维度查找表")
    dimension_lookup = build_dimension_lookup(input_data)
    print("维度查找表:")
    for provider, datasets in dimension_lookup.items():
        print(f"  {provider}: {len(datasets)} datasets")
        for dataset_code, info in datasets.items():
            print(f"    {dataset_code}: {info['frequency']}")
    print()

    # 7. 创建模拟提取的数据
    print("7. 创建模拟提取的元数据")
    # 模拟从新providers提取的数据
    extracted_rows = [
        {
            'provider_code': 'BUBA',
            'dataset_code': 'BBK01',
            'dataset_name': 'Bundesbank Statistics',
            'series_count': 150,
            'dimensions': '["DATE", "INDICATOR"]',
            'frequency': 'M'
        },
        {
            'provider_code': 'BOJ',
            'dataset_code': 'BOJ01',
            'dataset_name': 'Bank of Japan Statistics',
            'series_count': 200,
            'dimensions': '["DATE", "SERIES"]',
            'frequency': 'Q'
        }
    ]

    metadata_df = create_metadata_dataframe(extracted_rows)
    print("创建的元数据DataFrame:")
    print(metadata_df)
    print()

    # 8. 验证输出
    print("8. 验证输出数据")
    is_valid = verify_output(metadata_df, provider_codes)
    print(f"验证结果: {'通过' if is_valid else '失败'}")
    print()

    print("=== 演示完成 ===")

if __name__ == "__main__":
    demo_core_functions()