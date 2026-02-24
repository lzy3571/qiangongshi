import pandas as pd
f = "附件2：随车机械师积分及“千工时”保安全竞赛统计汇总表（2024.10.26-2025.12.25）.xlsx"
try:
    df = pd.read_excel(f, sheet_name='随车机械师工时奖励周期内积分统计 ', header=3)
    # Find rows with non-null issues
    df_issues = df[df['扣分总计'].notna() & (df['扣分总计'] != 0)]
    print(df_issues[['姓名', '竞赛周期内扣分明细', '问题来源', '扣分条款', '扣分明细', '扣分总计']].head(10).to_string())
except Exception as e:
    print(e)
