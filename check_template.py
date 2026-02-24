import pandas as pd
df = pd.read_excel('excel_templates/附件5模板.xlsx', header=None, nrows=5)
print(df.to_string())
