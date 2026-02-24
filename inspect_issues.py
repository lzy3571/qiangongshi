import pandas as pd
try:
    df = pd.read_excel('导入数据-每月问题.xlsx', nrows=5)
    print(df[['扣分条款', '扣分明细']].to_string())
except Exception as e:
    print(e)
