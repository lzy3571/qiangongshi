@app.route('/')
@login_required
def index():
    session = Session()
    total = session.query(Mechanic).filter(Mechanic.status == '在岗').count()
    over_1000 = session.query(Mechanic).filter(Mechanic.total_hours >= 1000, Mechanic.status == '在岗').count()
    
    last_record = session.query(MonthlyRecord).order_by(MonthlyRecord.month.desc()).first()
    if last_record:
        count = session.query(MonthlyRecord).filter_by(month=last_record.month).count()
        if count > 0:
            last_month_imported = last_record.month
        else:
            last_month_imported = "无数据"
    else:
        last_month_imported = "无数据"

    # Filter Params
    now = datetime.now()
    filter_year = request.args.get('year', now.year, type=int)
    filter_start_month = request.args.get('start_month', f"{now.year}-01")
    filter_end_month = request.args.get('end_month', f"{now.year}-12")

    mechanics = session.query(Mechanic).options(joinedload(Mechanic.issues), joinedload(Mechanic.rewards_history)).filter(Mechanic.status == '在岗').all()
    
    score_labels = ["12", "11", "9-10", "7-8", "0-6"]
    score_counts = [0, 0, 0, 0, 0]
    deduction_labels = ["0", "0-1", "1-3", "3-6", ">=6"]
    deduction_counts = [0, 0, 0, 0, 0]
    reward_labels = ["1200", "1000", "600", "200", "0"]
    reward_counts = [0, 0, 0, 0, 0]
    hours_labels = ["<500", "500-800", "800-1000", "1000-1200", ">=1200"]
    hours_counts = [0, 0, 0, 0, 0]

    # 1. Annual Score (Filter by Year)
    # 2. Deduction Distribution (Filter by Month Range)
    # 3. Hours Distribution (Current Total)
    
    for m in mechanics:
        # --- Annual Score ---
        # Filter issues by Year
        annual_issues = []
        for i in m.issues:
            if getattr(i, 'include_in_annual', True) and i.date and str(i.date).startswith(str(filter_year)):
                annual_issues.append(i)
        
        deduction_annual = abs(sum(i.detail or 0 for i in annual_issues))
        current_score = 12 - deduction_annual

        if current_score >= 12: score_counts[0] += 1
        elif current_score >= 11: score_counts[1] += 1
        elif current_score >= 9: score_counts[2] += 1
        elif current_score >= 7: score_counts[3] += 1
        else: score_counts[4] += 1

        # --- Deduction Distribution (Range) ---
        range_issues = []
        for i in m.issues:
            if not i.date: continue
            # Date format assumed YYYY-MM-DD
            d_str = str(i.date)[:7] # YYYY-MM
            if filter_start_month <= d_str <= filter_end_month:
                range_issues.append(i)
        
        deduction_range = abs(sum(i.detail or 0 for i in range_issues))
        
        if deduction_range == 0: deduction_counts[0] += 1
        elif deduction_range <= 1: deduction_counts[1] += 1
        elif deduction_range < 3: deduction_counts[2] += 1
        elif deduction_range < 6: deduction_counts[3] += 1
        else: deduction_counts[4] += 1

        # --- Hours Distribution (Current) ---
        hours_val = m.total_hours or 0.0
        if hours_val < 500: hours_counts[0] += 1
        elif hours_val < 800: hours_counts[1] += 1
        elif hours_val < 1000: hours_counts[2] += 1
        elif hours_val < 1200: hours_counts[3] += 1
        else: hours_counts[4] += 1

    # --- Reward Distribution (Range) ---
    # Based on Attachment6Data (Actual Rewards)
    # Normalize dates for query
    s_dot = filter_start_month.replace('-', '.')
    e_dot = filter_end_month.replace('-', '.')
    
    # We fetch all and filter in python to handle mixed formats safely
    att6_all = session.query(Attachment6Data).all()
    for r in att6_all:
        if not r.reward_date: continue
        d = str(r.reward_date).strip()
        # Normalize to YYYY-MM
        d_norm = d.replace('.', '-').replace('年', '-').replace('月', '')
        if len(d_norm) == 6: # YYYY-M -> YYYY-0M
            parts = d_norm.split('-')
            if len(parts) == 2 and len(parts[1]) == 1:
                d_norm = f"{parts[0]}-0{parts[1]}"
        
        # Check range
        # Only check YYYY-MM
        d_chk = d_norm[:7]
        if filter_start_month <= d_chk <= filter_end_month:
            amt = r.reward_amount or 0
            if amt >= 1200: reward_counts[0] += 1
            elif amt >= 1000: reward_counts[1] += 1
            elif amt >= 600: reward_counts[2] += 1
            elif amt >= 200: reward_counts[3] += 1
            else: reward_counts[4] += 1
    
    # Historical Trend (Range Filter)
    att6_records = session.query(Attachment6Data.reward_date, Attachment6Data.total_amount).all()
    history_map = {}
    
    for r in att6_records:
        r_date = r.reward_date
        if not r_date: continue
        try:
            norm_date = r_date.strip().replace('.', '-').replace('年', '-').replace('月', '')
            parts = norm_date.split('-')
            if len(parts) >= 2:
                y = parts[0]
                m = parts[1]
                if len(m) == 1: m = '0' + m
                norm_date = f"{y}-{m}"
            
            # Apply Range Filter to Trend
            if filter_start_month <= norm_date <= filter_end_month:
                if norm_date not in history_map:
                    history_map[norm_date] = {'amount': 0.0, 'count': 0}
                history_map[norm_date]['amount'] += (r.total_amount or 0)
                history_map[norm_date]['count'] += 1
        except: pass
        
    sorted_months = sorted(history_map.keys())
    history_labels = sorted_months
    history_amounts = [round(history_map[m]['amount'], 2) for m in sorted_months]
    history_counts = [history_map[m]['count'] for m in sorted_months]

    session.close()
    return render_template(
        'index.html',
        total_mechanics=total,
        over_1000=over_1000,
        last_month_imported=last_month_imported,
        score_labels=json.dumps(score_labels, ensure_ascii=False),
        score_counts=json.dumps(score_counts),
        deduction_labels=json.dumps(deduction_labels, ensure_ascii=False),
        deduction_counts=json.dumps(deduction_counts),
        reward_labels=json.dumps(reward_labels, ensure_ascii=False),
        reward_counts=json.dumps(reward_counts),
        hours_labels=json.dumps(hours_labels, ensure_ascii=False),
        hours_counts=json.dumps(hours_counts),
        history_labels=json.dumps(history_labels, ensure_ascii=False),
        history_amounts=json.dumps(history_amounts),
        history_counts=json.dumps(history_counts),
        filter_year=filter_year,
        filter_start_month=filter_start_month,
        filter_end_month=filter_end_month
    )
