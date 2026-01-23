# src/utils/data/repositories/fund_repo.py
"""基金数据查询"""
import pandas as pd
from utils.data import oracle_fetcher


def get_fund_nav(fund_codes: list[str], begin_date: str, end_date: str) -> pd.DataFrame:
    """获取基金净值数据"""
    sql = """
          SELECT SECURITYCODE AS 基金代码,
                 ENDDATE      AS 交易日期,
                 AANVPER      AS 复权净值
          FROM TYTFUND.FUND_DR_FUNDNV
          WHERE SECURITYCODE IN (:code_list)
            AND ENDDATE >= TO_DATE(:begin_date, 'YYYY-MM-DD')
            AND ENDDATE <= TO_DATE(:end_date, 'YYYY-MM-DD') 
          """
    return oracle_fetcher.batch_query(
        sql, fund_codes, batch_size=500,
        begin_date=begin_date, end_date=end_date
    )


def get_fund_iv_cb(fund_codes: list[str], report_dt: str) -> pd.DataFrame:
    """获取基金转债持仓数据"""
    sql = """
          SELECT FUNDCODE  AS 基金代码,
                 ENDDATE   AS 报告日期,
                 BONDCODE  AS 债券代码,
                 INNERCODE AS 债券内码,
                 PCTNV     AS 持仓占比,
                 STYLE
          FROM TYTFUND.FUND_IV_BONDINVESTD
          WHERE FUNDCODE IN (:code_list)
            AND ENDDATE = TO_DATE(:report_dt, 'YYYY-MM-DD')
            AND BONDTYPE = '2' 
          """
    df = oracle_fetcher.batch_query(sql, fund_codes, report_dt=report_dt)

    # 去重：按STYLE排序保留第一条
    return (df.sort_values('STYLE', ascending=True)
            .drop_duplicates(subset=['基金代码', '报告日期', '债券内码'], keep='first')
            .drop('STYLE', axis=1))


def get_fund_iv_stock(fund_codes: list[str], report_dt: str) -> pd.DataFrame:
    """获取基金股票持仓数据"""
    sql = """
          SELECT FUNDCODE              AS 基金代码,
                 ENDDATE               AS 报告日期,
                 STOCKCODE             AS 股票代码,
                 EMSECURITYVARIETYCODE AS 股票内码,
                 PCTNV                 AS 持仓占比
          FROM TYTFUND.FUND_IV_STOCKINVESTO
          WHERE FUNDCODE IN (:code_list)
            AND ENDDATE = TO_DATE(:report_dt, 'YYYY-MM-DD')
            AND STYLE IN ('01', '02', '03', '04') 
          """
    return oracle_fetcher.batch_query(sql, fund_codes, report_dt=report_dt)