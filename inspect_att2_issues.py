import pandas as pd
f = "附件2：随车机械师积分及“千工时”保安全竞赛统计汇总表（2024.10.26-2025.12.25）.xlsx"
try:
    df = pd.read_excel(f, sheet_name='随车机械师工时奖励周期内积分统计 ', header=3)
    # G, H, I, J, K columns (0-based: 6, 7, 8, 9, 10)
    # Columns: 序号, 姓名, 工号, 乘务组别, 统计周期, 累计工时, 竞赛周期内扣分明细(G), 问题来源(H), 扣分条款(I), 扣分明细(J), 扣分总计(K)
    print(df[['竞赛周期内扣分明细', '问题来源', '扣分条款', '扣分明细', '扣分总计']].head(10).to_string())
    print("\nSample values for date check:")
    print(df['竞赛周期内扣分明细'].head(5))
except Exception as e:
    print(e)
