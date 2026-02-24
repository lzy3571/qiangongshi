import pandas as pd
f = "excel_templates/附件5模板.xlsx"
try:
    df = pd.read_excel(f, nrows=5)
    print(df.columns.tolist())
    print(df.head(2))
except Exception as e:
    print(e)
