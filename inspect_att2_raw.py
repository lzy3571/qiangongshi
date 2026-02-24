import pandas as pd

f = "附件2：随车机械师积分及“千工时”保安全竞赛统计汇总表（2024.10.26-2025.12.25）.xlsx"
try:
    xl = pd.ExcelFile(f)
    for sheet in xl.sheet_names:
        print(f"\nSheet: {sheet}")
        df = pd.read_excel(f, sheet_name=sheet, header=None, nrows=6)
        print(df.to_string())
except Exception as e:
    print(e)
