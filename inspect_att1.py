
import pandas as pd
import os

file_path = os.path.join("使用文件", "附件1 随车机械师积分制管理扣分清单.xlsx")
if os.path.exists(file_path):
    try:
        df = pd.read_excel(file_path)
        print("Columns:", df.columns.tolist())
        print("First row:", df.iloc[0].to_dict() if not df.empty else "Empty")
    except Exception as e:
        print(f"Error: {e}")
else:
    print("File not found")
