# src/utils/data/repositories/fund_repo.py
"""基金数据查询"""
import pandas as pd
from utils.data import oracle_fetcher


def get_fund_nav(fund_codes: list[str], begin_date: str, end_date: str) -> pd.DataFrame:
    """获取基金复权净值数据

    Args:
        fund_codes: 基金代码列表 ['001031', '000001']
        begin_date: 起始日期 'YYYY-MM-DD'
        end_date: 结束日期 'YYYY-MM-DD'

    Returns:
        DataFrame(基金代码, 交易日期, 复权净值, 昨复权净值)
    """
    sql = """
          SELECT SECURITYCODE AS 基金代码,
                 ENDDATE      AS 交易日期,
                 AANVPER      AS 复权净值,
                 AANVPERL     AS 昨复权净值
          FROM TYTFUND.FUND_DR_FUNDNV
          WHERE SECURITYCODE IN (:code_list)
            AND ENDDATE >= TO_DATE(:begin_date, 'YYYY-MM-DD')
            AND ENDDATE <= TO_DATE(:end_date, 'YYYY-MM-DD')
          """
    return oracle_fetcher.batch_query(
        sql, fund_codes, batch_size=500,
        begin_date=begin_date, end_date=end_date)


def get_fund_iv_cb(fund_codes: list[str], report_dt: str) -> pd.DataFrame:
    """获取基金转债持仓数据

    Args:
        fund_codes: 基金代码列表 ['000003', '001031']
        report_dt: 报告期日期 'YYYY-MM-DD'（季度末）

    Returns:
        DataFrame(基金代码, 报告日期, 披露日期, 债券代码, 债券内码, 持仓占比)
        已按STYLE去重，每只转债每只基金仅保留一条记录
    """
    sql = """
          SELECT FUNDCODE   AS 基金代码,
                 ENDDATE    AS 报告日期,
                 NOTICEDATE AS 披露日期,
                 BONDCODE   AS 债券代码,
                 INNERCODE  AS 债券内码,
                 PCTNV      AS 持仓占比,
                 STYLE
          FROM TYTFUND.FUND_IV_BONDINVESTD
          WHERE FUNDCODE IN (:code_list)
            AND ENDDATE = TO_DATE(:report_dt, 'YYYY-MM-DD')
            AND BONDTYPE = '2' \
          """
    df = oracle_fetcher.batch_query(sql, fund_codes, report_dt=report_dt)

    # 去重：按STYLE排序保留第一条
    return (df.sort_values('STYLE', ascending=True)
            .drop_duplicates(subset=['基金代码', '报告日期', '债券内码'], keep='first')
            .drop('STYLE', axis=1))


def get_fund_iv_stock(fund_codes: list[str], report_dt: str) -> pd.DataFrame:
    """获取基金股票持仓数据

    Args:
        fund_codes: 基金代码列表 ['000001', '000003']
        report_dt: 报告期日期 'YYYY-MM-DD'（季度末）

    Returns:
        DataFrame(基金代码, 报告日期, 披露日期, 股票代码, 持仓占比)
        已按STYLE去重，每只股票每只基金仅保留一条记录
    """
    sql = """
          SELECT FUNDCODE   AS 基金代码,
                 ENDDATE    AS 报告日期,
                 NOTICEDATE AS 披露日期,
                 STOCKCODE  AS 股票代码,
                 PCTNV      AS 持仓占比
          FROM TYTFUND.FUND_IV_STOCKINVESTO
          WHERE FUNDCODE IN (:code_list)
            AND ENDDATE = TO_DATE(:report_dt, 'YYYY-MM-DD')
          ORDER BY STYLE
          """
    df = oracle_fetcher.batch_query(sql, fund_codes, report_dt=report_dt)

    # 去重：按STYLE排序保留第一条
    return df.drop_duplicates(subset=['基金代码', '报告日期', '股票代码'], keep='first')


def get_fund_asset_detail(fund_codes: list[str], report_dt: str) -> pd.DataFrame:
    """获取基金资产配置（债券细分+杠杆）

    Returns:
        DataFrame(基金代码, 报告日期, 披露日期, 股票占比, 转债占比, 利率债占比,
                 信用债占比, 非政金债占比, ABS占比, 货币占比, 杠杆率)
    """
    sql = """
          SELECT FUNDCODE                                                                 AS 基金代码, \
                 ENDDATE                                                                  AS 报告日期,
                 NOTICEDATE                                                               AS 披露日期,
                 COALESCE(SIPCTNV, 0)                                                     AS 股票占比, \
                 COALESCE(BTPCTNV, 0)                                                     AS 转债占比, \
                 COALESCE(NBPCTNV1, 0) + COALESCE(PBPCTNV, 0) + \
                 COALESCE(CBBPCTNV, 0) + COALESCE(LOCALGOV_BOND_NR, 0)                    AS 利率债占比, \
                 COALESCE(CBVPCTNV, 0) + COALESCE(MTNPCTNV, 0) + \
                 COALESCE(CBSBPCTNV, 0) + COALESCE(DEPOSITCEPCT, 0)                       AS 信用债占比, \
                 COALESCE(FINABVPCTNV, 0) - COALESCE(PBPCTNV, 0)                          AS 非政金债占比, \
                 COALESCE(ABSPCTNV, 0)                                                    AS ABS占比, \
                 (FASSETSUM / FNVSUM) * 100 - COALESCE(SIPCTNV, 0) - COALESCE(BSPCTNV, 0) AS 货币占比, \
                 FASSETSUM / FNVSUM                                                       AS 杠杆率
          FROM TYTFUND.FUND_IV_ASSETALLOCT
          WHERE FUNDCODE IN (:code_list)
            AND ENDDATE = TO_DATE(:report_dt, 'YYYY-MM-DD')
          ORDER BY STYLE
          """

    df = oracle_fetcher.batch_query(sql, fund_codes, report_dt=report_dt)
    return df.drop_duplicates(subset=['基金代码', '报告日期'], keep='first')


if __name__ == '__main__':
    main_fund_codes = ['000003', '000001']
    main_begin_date = '2020-01-01'
    main_end_date = '2024-12-31'

    main_cb = get_fund_iv_cb(main_fund_codes, main_end_date)
    main_stk = get_fund_iv_stock(main_fund_codes, main_end_date)

    main_iv = get_fund_asset_detail(main_fund_codes, main_end_date)
