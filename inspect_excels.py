import pandas as pd
import os

files = [
    "附件6 随车机械师积分及千工时保安全竞赛奖励明细汇总表.xlsx",
    "导入数据-每月问题.xlsx",
    "导入数据-随车机械师月度工时（2026年1月）.xlsx",
    "附件2：随车机械师积分及“千工时”保安全竞赛统计汇总表（2024.10.26-2025.12.25）.xlsx"
]

for f in files:
    print(f"\n--- {f} ---")
    try:
        # Read first few rows to get headers and sample data
        # Check sheet names first
        xl = pd.ExcelFile(f)
        print(f"Sheet names: {xl.sheet_names}")
        
        for sheet in xl.sheet_names:
            df = pd.read_excel(f, sheet_name=sheet, nrows=3)
            print(f"\nSheet: {sheet}")
            # Clean columns: replace newlines, strip whitespace
            clean_columns = [str(c).replace('\n', ' ').strip() for c in df.columns.tolist()]
            print("Columns:", clean_columns)
    except Exception as e:
        print(f"Error reading {f}: {e}")
