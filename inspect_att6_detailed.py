import pandas as pd
f = "附件6 随车机械师积分及千工时保安全竞赛奖励明细汇总表.xlsx"
df = pd.read_excel(f)
print(df.head(10).to_string())
print("\nColumns:", df.columns.tolist())
# Check for duplicates in '工号' to see if there are multiple entries (history)
if '工号' in df.columns:
    print("\nDuplicate IDs:", df[df.duplicated('工号')]['工号'].tolist())
