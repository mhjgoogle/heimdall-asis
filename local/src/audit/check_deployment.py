#!/usr/bin/env python3
# filepath: local/src/audit/check_deployment.py

"""Pre-deployment verification for audit module."""

import sys
import os
import subprocess
from pathlib import Path

def check_python_version():
    """检查 Python 版本"""
    print("✓ 检查 Python 版本...")
    version = sys.version_info
    if version.major == 3 and version.minor >= 9:
        print(f"  ✓ Python {version.major}.{version.minor}.{version.micro} OK")
        return True
    else:
        print(f"  ✗ 需要 Python 3.9+，当前 {version.major}.{version.minor}")
        return False

def check_database():
    """检查数据库是否存在和可访问"""
    print("\n✓ 检查数据库...")
    db_path = Path(__file__).parent.parent.parent / 'data' / 'heimdall.db'
    
    if db_path.exists():
        print(f"  ✓ 数据库存在: {db_path}")
        
        # 检查表
        try:
            import sqlite3
            with sqlite3.connect(db_path) as conn:
                tables = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
                table_names = [t[0] for t in tables]
                
                required = ['news_intel_pool', 'data_catalog', 'timeseries_micro']
                for table in required:
                    if table in table_names:
                        print(f"  ✓ 表 {table} 存在")
                    else:
                        print(f"  ✗ 缺少表 {table}")
                        return False
                
                # 统计数据
                news_count = conn.execute(
                    "SELECT COUNT(*) FROM news_intel_pool"
                ).fetchone()[0]
                print(f"  ✓ 新闻池中有 {news_count} 条记录")
                
        except Exception as e:
            print(f"  ✗ 数据库访问错误: {e}")
            return False
        
        return True
    else:
        print(f"  ✗ 数据库不存在: {db_path}")
        return False

def check_package_optional(package_name, import_name=None):
    """检查可选包是否安装"""
    if import_name is None:
        import_name = package_name
    
    try:
        __import__(import_name)
        print(f"  ✓ {package_name} 已安装")
        return True
    except ImportError:
        print(f"  ◐ {package_name} 未安装 (可选)")
        return False

def check_llm_dependencies():
    """检查 LLM 相关依赖"""
    print("\n✓ 检查 LLM 依赖...")
    
    services = {}
    
    # 检查 Claude (Anthropic)
    if check_package_optional("Anthropic", "anthropic"):
        key = os.getenv("ANTHROPIC_API_KEY")
        if key:
            print(f"    ✓ ANTHROPIC_API_KEY 已配置")
            services['claude'] = True
        else:
            print(f"    ◐ ANTHROPIC_API_KEY 未配置")
            services['claude'] = False
    else:
        services['claude'] = False
    
    # 检查 OpenAI
    if check_package_optional("OpenAI", "openai"):
        key = os.getenv("OPENAI_API_KEY")
        if key:
            print(f"    ✓ OPENAI_API_KEY 已配置")
            services['openai'] = True
        else:
            print(f"    ◐ OPENAI_API_KEY 未配置")
            services['openai'] = False
    else:
        services['openai'] = False
    
    # 检查 Ollama (本地)
    try:
        result = subprocess.run(
            ["curl", "-s", "http://localhost:11434/api/tags"],
            capture_output=True,
            timeout=2
        )
        if result.returncode == 0:
            print(f"  ✓ Ollama 运行在 localhost:11434")
            services['ollama'] = True
        else:
            print(f"  ◐ Ollama 未运行或无法访问")
            services['ollama'] = False
    except Exception:
        print(f"  ◐ Ollama 不可用")
        services['ollama'] = False
    
    return services

def check_python_packages():
    """检查必需的 Python 包"""
    print("\n✓ 检查 Python 包...")
    
    required = {
        'sqlite3': 'sqlite3',
        'json': 'json',
        'pathlib': 'pathlib',
        'datetime': 'datetime',
    }
    
    optional = {
        'pandas': 'pandas',
        'requests': 'requests',
    }
    
    all_ok = True
    
    for package, import_name in required.items():
        try:
            __import__(import_name)
            print(f"  ✓ {package} (必需) 已安装")
        except ImportError:
            print(f"  ✗ {package} (必需) 未安装")
            all_ok = False
    
    for package, import_name in optional.items():
        if check_package_optional(package, import_name):
            print(f"  ✓ {package} (可选) 已安装")
        else:
            print(f"  ◐ {package} (可选) 未安装")
    
    return all_ok

def check_folder_structure():
    """检查项目文件夹结构"""
    print("\n✓ 检查项目结构...")
    
    base_dir = Path(__file__).parent.parent.parent
    required_dirs = [
        'src/audit',
        'data',
        'sandbox/notebooks',
    ]
    
    all_ok = True
    for dir_path in required_dirs:
        full_path = base_dir / dir_path
        if full_path.exists():
            print(f"  ✓ {dir_path} 存在")
        else:
            print(f"  ✗ {dir_path} 缺失")
            all_ok = False
    
    return all_ok

def main():
    print("="*80)
    print("LLM 审计模块 - 部署前检查")
    print("="*80)
    
    checks = {
        'Python 版本': check_python_version(),
        '项目结构': check_folder_structure(),
        'Python 包': check_python_packages(),
        '数据库': check_database(),
    }
    
    llm_services = check_llm_dependencies()
    
    print("\n" + "="*80)
    print("检查结果总结")
    print("="*80)
    
    all_critical_ok = all(checks.values())
    
    if all_critical_ok:
        print("✓ 所有关键检查通过!")
    else:
        print("✗ 存在失败的检查，请解决后再继续")
    
    print("\nLLM 服务可用性:")
    for service, available in llm_services.items():
        status = "✓ 可用" if available else "◐ 不可用"
        print(f"  {service:10} {status}")
    
    available_llms = [k for k, v in llm_services.items() if v]
    
    if not available_llms:
        print("\n⚠️  警告：没有可用的 LLM 服务")
        print("   建议：")
        print("   1. 安装 Anthropic SDK: pip install anthropic")
        print("   2. 或安装 OpenAI SDK: pip install openai")
        print("   3. 或安装 Ollama: https://ollama.ai")
    else:
        print(f"\n✓ 可用的 LLM: {', '.join(available_llms)}")
        print(f"推荐: 使用 Claude 3.5 Sonnet 获得最优质量")
    
    print("\n" + "="*80)
    print("后续步骤")
    print("="*80)
    
    if all_critical_ok:
        print("1. ✓ 基础检查通过")
        print("2. 运行: python3 src/gold_pipeline.py --cost-analysis")
        print("3. 运行: python3 src/gold_pipeline.py --audit --audit-limit 10 --dry-run")
        print("4. 运行: python3 src/gold_pipeline.py --audit --audit-limit 1000")
    else:
        print("请先解决上述失败项，再继续")
    
    print("="*80)
    
    return 0 if all_critical_ok else 1

if __name__ == "__main__":
    sys.exit(main())
