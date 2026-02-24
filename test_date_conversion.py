import pandas as pd
import datetime

def excel_date_to_str(val):
    try:
        if pd.isna(val):
            return "NA"
        
        val_str = str(val).strip()
        if not val_str:
            return "EMPTY"
        
        try:
            val_float = float(val)
            print(f"Float val: {val_float}")
            
            if val_float > 40000 and val_float < 50000:
                dt = datetime.datetime(1899, 12, 30) + datetime.timedelta(days=val_float)
                return dt.strftime('%Y-%m')
            elif val_float > 202000 and val_float < 203000:
                val_s = str(int(val_float))
                if len(val_s) >= 6:
                     return f"{val_s[:4]}-{val_s[4:6]}"
            
        except ValueError:
            print("ValueError in float conversion")
            pass
            
        if '年' in val_str and '月' in val_str:
            val_str = val_str.replace('年', '-').replace('月', '')
            parts = val_str.split('-')
            if len(parts) >= 2:
                return f"{parts[0]}-{int(parts[1]):02d}"
        
        return val_str
    except Exception as e:
        return f"Error: {str(e)}"

# Test with user provided values
test_values = [45809, 45658, "45809", "2024年7月"]

print("Testing conversion:")
for v in test_values:
    res = excel_date_to_str(v)
    print(f"Input: {v} -> Output: {res}")
