import pandas as pd
import os

os.makedirs('excel_templates', exist_ok=True)

# Hours Template
df_hours = pd.DataFrame(columns=['工号', '姓名', '班组', '月度工时小计'])
df_hours.to_excel('excel_templates/月度工时模板.xlsx', index=False)

# Issues Template
df_issues = pd.DataFrame(columns=['姓名', '扣分明细', '问题', '检查日期', '问题来源', '扣分条款'])
df_issues.to_excel('excel_templates/月度问题模板.xlsx', index=False)

print("Templates created.")
