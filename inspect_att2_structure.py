import pandas as pd
f = "附件2：随车机械师积分及“千工时”保安全竞赛统计汇总表（2024.10.26-2025.12.25）.xlsx"
try:
    df = pd.read_excel(f, sheet_name='随车机械师工时奖励周期内积分统计 ', header=3)
    # Print a chunk of rows to see the structure (e.g. rows 5 to 15)
    # User said曹卫明 is at B7 (index 6 in 0-based DataFrame? or index 5 if header is row 3?)
    # Header=3 means Row 4 is header. Data starts Row 5 (index 0).
    # Excel B7 -> Index 2 (since B5 is Index 0).
    # Let's inspect rows 0 to 15.
    print(df.iloc[0:15][['姓名', '工号', '竞赛周期内扣分明细']].to_string())
except Exception as e:
    print(e)
