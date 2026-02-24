import pandas as pd
import os

file_path = "附件8 随车机械师积分及“千工时”保安全竞赛工时奖励清零汇总表.xlsx"

if os.path.exists(file_path):
    try:
        df = pd.read_excel(file_path, header=None) # Read without header first to see structure
        print(df.head(10))
        
        # Try to find header row
        df = pd.read_excel(file_path, header=0) # Assume first row is header
        print("\nColumns:")
        print(df.columns.tolist())
        print("\nFirst row data:")
        print(df.iloc[0])
    except Exception as e:
        print(f"Error reading file: {e}")
else:
    print(f"File not found: {file_path}")
