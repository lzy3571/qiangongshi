import pandas as pd

f = "附件2：随车机械师积分及“千工时”保安全竞赛统计汇总表（2024.10.26-2025.12.25）.xlsx"
try:
    xl = pd.ExcelFile(f)
    for sheet in xl.sheet_names:
        print(f"\nSheet: {sheet}")
        # Try reading skipping the first row or two
        df = pd.read_excel(f, sheet_name=sheet, header=1, nrows=3) # Try header row 2 (index 1)
        print("Columns (Header=1):", [str(c).replace('\n', ' ').strip() for c in df.columns.tolist()])
        
        df = pd.read_excel(f, sheet_name=sheet, header=2, nrows=3) # Try header row 3 (index 2)
        print("Columns (Header=2):", [str(c).replace('\n', ' ').strip() for c in df.columns.tolist()])
except Exception as e:
    print(e)
