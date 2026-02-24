import pandas as pd
import datetime

file_path = "附件8 随车机械师积分及“千工时”保安全竞赛工时奖励清零汇总表.xlsx"

try:
    # Read raw values to see dates as numbers
    df = pd.read_excel(file_path, header=1)
    
    print("--- Columns ---")
    print(df.columns.tolist())
    
    print("\n--- Chen Yang Rows ---")
    # Find rows related to Chen Yang
    # We need to look at a window around Chen Yang to see how many rows are associated
    
    # Iterate to find Chen Yang
    chen_yang_idx = -1
    for idx, row in df.iterrows():
        if str(row.get('姓名', '')).strip() == '陈洋':
            chen_yang_idx = idx
            break
            
    if chen_yang_idx != -1:
        # Show Chen Yang row and next 10 rows
        print(df.iloc[chen_yang_idx:chen_yang_idx+10][['姓名', '本次满足奖励\n时间（月份）', '本次奖励周期内千工时扣分明细']])
    else:
        print("Chen Yang not found")
        
    print("\n--- Date Sample ---")
    print(df['本次满足奖励\n时间（月份）'].head(10))

except Exception as e:
    print(e)
