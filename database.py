from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, DateTime, Boolean
from datetime import datetime
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

Base = declarative_base()

class Mechanic(Base):
    __tablename__ = 'mechanics'
    
    id = Column(Integer, primary_key=True)
    employee_id = Column(String, unique=True, index=True) # 工号
    name = Column(String, index=True) # 姓名
    team = Column(String) # 乘务组别/班组
    workshop_id = Column(Integer, ForeignKey('workshops.id')) # 所属车间ID
    
    # Tracking State
    total_hours = Column(Float, default=0.0) # 实时累计工时 (Base + All Imports)
    base_hours = Column(Float, default=0.0) # 静态基准工时 (截至2025年12月，来自附件2)
    current_cycle_deduction = Column(Float, default=0.0) # 本次奖励周期内千工时累积扣分
    
    # New Fields
    identity = Column(String, default='随车机械师') # 随车机械师/后备随车机械师
    status = Column(String, default='在岗') # 在岗/调离
    
    # Reward History (Last known state from Attachment 6)
    last_reward_month = Column(String) # 本次满足奖励 时间（月份）
    last_reward_cycle = Column(String) # 本次奖励周期
    last_reward_amount = Column(Float, default=0.0) # 本次奖励周期奖励金额
    consecutive_rewards = Column(String) # 连续奖励情况
    extra_reward = Column(Float, default=0.0) # 本次连续额外奖励
    
    # Reset/Cleared hours
    cleared_hours = Column(Float, default=0.0) # 累计清零工时
    
    workshop = relationship("Workshop")

    def __repr__(self):
        return f"<Mechanic(name={self.name}, id={self.employee_id}, hours={self.total_hours})>"

class MonthlyRecord(Base):
    __tablename__ = 'monthly_records'
    
    id = Column(Integer, primary_key=True)
    mechanic_id = Column(Integer, ForeignKey('mechanics.id'))
    month = Column(String) # YYYY-MM
    
    hours = Column(Float, default=0.0)
    deduction = Column(Float, default=0.0)
    issues_details = Column(String) # JSON or text summary of issues
    
    mechanic = relationship("Mechanic", back_populates="records")

class RewardHistory(Base):
    __tablename__ = 'reward_history'
    
    id = Column(Integer, primary_key=True)
    mechanic_id = Column(Integer, ForeignKey('mechanics.id'))
    
    reward_date = Column(String) # 本次满足奖励时间（月份）
    reward_cycle = Column(String) # 本次奖励周期
    deduction = Column(Float, default=0.0) # 本次奖励周期内千工时累积扣分
    amount = Column(Float, default=0.0) # 本次奖励周期奖励金额
    cleared_hours = Column(Float, default=0.0) # 累计清零工时
    extra_reward = Column(Float, default=0.0) # 本次连续额外奖励
    total_amount = Column(Float, default=0.0) # 本月奖励金额
    consecutive_info = Column(String) # 连续奖励情况
    
    mechanic = relationship("Mechanic", back_populates="rewards_history")

class Issue(Base):
    __tablename__ = 'issues'
    
    id = Column(Integer, primary_key=True)
    mechanic_id = Column(Integer, ForeignKey('mechanics.id'))
    date = Column(String) # 检查日期/导入日期
    problem = Column(String) # 问题
    source = Column(String) # 问题来源
    clause = Column(String) # 扣分条款
    detail = Column(Float) # 扣分明细
    total_deduction = Column(Float) # 扣分总计 (Seems redundant if detail is it, but user asked for it)
    status = Column(String, default='未结算') # 问题状态：未结算/已结算
    include_in_annual = Column(Boolean, default=True) # 是否纳入年度积分统计 (True=纳入, False=不纳入)
    
    mechanic = relationship("Mechanic", back_populates="issues")

class ClearedHoursRecord(Base):
    __tablename__ = 'cleared_hours_records'
    
    id = Column(Integer, primary_key=True)
    mechanic_id = Column(Integer, ForeignKey('mechanics.id'))
    
    depot = Column(String) # 动车所
    name = Column(String) # 姓名
    employee_id = Column(String) # 工号
    team = Column(String) # 乘务组别
    activity_cumulative_hours = Column(Float, default=0.0) # 活动起累计工时
    reward_month = Column(String) # 本次满足奖励时间（月份）
    reward_cycle = Column(String) # 本次奖励周期
    cycle_deduction = Column(Float, default=0.0) # 本次奖励周期内千工时累积扣分
    deduction_details = Column(String) # 本次奖励周期内千工时扣分明细 (Stores text, but displayed as count)
    clearing_count = Column(Integer, default=0) # 累计奖励清零次数
    
    # Hours
    cleared_hours_base = Column(Float, default=0.0) # 累计清零工时（截至2025年12月）
    cleared_hours_new = Column(Float, default=0.0) # 2026年后新增清零工时
    total_cleared_hours = Column(Float, default=0.0) # 累计清零工时 (Base + New)
    
    remarks = Column(String) # 备注
    
    mechanic = relationship("Mechanic", back_populates="cleared_records")

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, nullable=False)
    password = Column(String, nullable=False)
    role = Column(String, default='user') # 'admin' or 'user'
    
    # New Fields
    employee_id = Column(String)
    name = Column(String)
    contact = Column(String)
    workshop_id = Column(Integer, ForeignKey('workshops.id'))
    
    workshop = relationship("Workshop")

class Workshop(Base):
    __tablename__ = 'workshops'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)

class Team(Base):
    __tablename__ = 'teams'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    workshop_id = Column(Integer, ForeignKey('workshops.id'))
    
    workshop = relationship("Workshop")

class RouteHoursData(Base):
    __tablename__ = 'route_hours_data'
    id = Column(Integer, primary_key=True)
    train_no = Column(String, nullable=False)
    pre_work_hours = Column(Float, default=0.0)
    enroute_hours = Column(Float, default=0.0)
    post_work_hours = Column(Float, default=0.0)
    remarks = Column(String)

class Attachment6Data(Base):
    __tablename__ = 'attachment6_data'
    id = Column(Integer, primary_key=True)
    employee_id = Column(String)
    name = Column(String)
    team = Column(String)
    reward_date = Column(String) # 本次满足奖励时间（月份）
    reward_cycle = Column(String) # 本次奖励周期
    reward_amount = Column(Float) # 本次奖励周期奖励金额
    
    activity_cumulative_hours = Column(Float) # 活动起累计工时
    cycle_deduction = Column(Float) # 本次奖励周期内千工时累积扣分
    cleared_hours = Column(Float) # 累计清零工时（周期扣分超过6分）
    
    # New fields matching user request
    balance_hours = Column(Float) # 当前前结余工时
    past_reward_count = Column(Integer) # 过去已奖励次数
    is_consecutive = Column(String) # 本次奖励是否触发连续奖励
    consecutive_info = Column(String) # 连续奖励情况（上一周期奖励）
    extra_reward = Column(Float) # 本次连续额外奖励
    total_amount = Column(Float) # 本月奖励金额（元）

    # Fields that might not be in Att6 but were before, keeping just in case or removing if sure.
    # depot, remarks, clearing_count (mapped to past_reward_count?)
    # User didn't ask for depot/remarks in the list, but I'll keep remarks just in case it's useful?
    # Actually user list was specific. Let's stick to the list + basic ID.
    # removing 'depot', 'remarks' (unless user wants them, but list was explicit)
    # The file inspection didn't show 'depot' or 'remarks' in columns. So safe to remove.

class Attachment1Data(Base):
    __tablename__ = 'attachment1_data'
    id = Column(Integer, primary_key=True)
    # Columns from Att 1: 序号, 问题来源, 扣分条款, 扣分明细
    source = Column(String)
    clause = Column(String)
    detail = Column(String) # Description of deduction rule
    score = Column(Float) # Points deducted (usually positive number representing deduction?) Or negative? 
    # Usually "扣分清单" lists the rule and how much to deduct.

class OperationLog(Base):
    __tablename__ = 'operation_logs'
    id = Column(Integer, primary_key=True)
    user = Column(String)
    action = Column(String)
    detail = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)



Mechanic.records = relationship("MonthlyRecord", order_by=MonthlyRecord.id, back_populates="mechanic")
Mechanic.rewards_history = relationship("RewardHistory", order_by=RewardHistory.reward_date.desc(), back_populates="mechanic")
Mechanic.issues = relationship("Issue", order_by=Issue.id.desc(), back_populates="mechanic")
Mechanic.cleared_records = relationship("ClearedHoursRecord", order_by=ClearedHoursRecord.reward_month.desc(), back_populates="mechanic")

import os

# Setup DB
# Data Directory Configuration
# Use environment variable DATA_DIR if set (for Docker), otherwise current directory
DATA_DIR = os.environ.get('DATA_DIR', '.')
DB_PATH = os.path.join(DATA_DIR, 'mechanics.db')

engine = create_engine(f'sqlite:///{DB_PATH}')
Session = sessionmaker(bind=engine)

def init_db():
    Base.metadata.create_all(engine)
