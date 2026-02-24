import pandas as pd
df = pd.read_excel("附件6 随车机械师积分及千工时保安全竞赛奖励明细汇总表.xlsx", sheet_name=0, nrows=5)
print(df[['本次奖励周期内千工时累积扣分']].to_string())
