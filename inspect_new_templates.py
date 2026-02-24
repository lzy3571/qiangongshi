import pandas as pd
import os

files = ['excel_templates/附件2-1模板.xlsx', 'excel_templates/附件2-2模板.xlsx']

for f in files:
    print(f"\n--- Checking {f} ---")
    if os.path.exists(f):
        try:
            df = pd.read_excel(f, header=None, nrows=5)
            print(df.to_string())
        except Exception as e:
            print(f"Error: {e}")
    else:
        print("File not found.")
