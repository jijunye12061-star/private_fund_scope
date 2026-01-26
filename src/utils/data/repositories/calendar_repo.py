# src/utils/data/repositories/calendar_repo.py
"""交易日历查询"""
import pandas as pd
from utils.data import oracle_fetcher


def get_trading_dt(begin_date: str, end_date: str) -> pd.DatetimeIndex:
    """获取交易日序列"""
    sql = """
          SELECT TRADE_DT as 交易日期
          FROM TYTFUND.QT_TRADE_CALENDAR
          WHERE IS_D = '1'
            AND TRADE_DT >= TO_DATE(:begin_date, 'YYYY-MM-DD')
            AND TRADE_DT <= TO_DATE(:end_date, 'YYYY-MM-DD') 
          """
    df = oracle_fetcher.query(sql, begin_date=begin_date, end_date=end_date)
    return pd.DatetimeIndex(df['交易日期'])