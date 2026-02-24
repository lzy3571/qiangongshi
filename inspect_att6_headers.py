
import pandas as pd
import os

file_path = os.path.join("使用文件", "附件6 随车机械师积分及千工时保安全竞赛奖励明细汇总表.xlsx")
if os.path.exists(file_path):
    try:
        df = pd.read_excel(file_path)
        print("Columns:", df.columns.tolist())
        # Print first few rows to see data types
        print(df.head(2).to_dict())
    except Exception as e:
        print(f"Error: {e}")
else:
    print("File not found")
