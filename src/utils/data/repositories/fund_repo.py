# src/utils/data/repositories/fund_repo.py
"""基金数据查询"""
import pandas as pd
from utils.data import oracle_fetcher, doris_fetcher


def get_fund_nav(fund_codes: list[str], begin_date: str, end_date: str) -> pd.DataFrame:
    """获取基金复权净值数据

    Args:
        fund_codes: 基金代码列表 ['001031', '000001']
        begin_date: 起始日期 'YYYY-MM-DD'
        end_date: 结束日期 'YYYY-MM-DD'

    Returns:
        DataFrame(基金代码, 交易日期, 复权净值, 前日复权净值)
    """
    sql = """
          SELECT c_fd_code     AS 基金代码, 
                 c_trade_date  AS 交易日期, 
                 c_nav_adj     AS 复权净值, 
                 c_nav_adj_pre AS 前日复权净值
          FROM tytdata.tb_fd_nav_daily
          WHERE c_fd_code IN :code_list
            AND c_trade_date >= :begin_date
            AND c_trade_date <= :end_date 
          """

    return doris_fetcher.batch_query(
        sql, fund_codes,
        begin_date=begin_date,
        end_date=end_date
    )


def get_fund_iv_cb(fund_codes: list[str], report_dt: str) -> pd.DataFrame:
    """获取基金转债持仓数据

    Args:
        fund_codes: 基金代码列表 ['000003', '001031']
        report_dt: 报告期日期 'YYYY-MM-DD'（季度末）

    Returns:
        DataFrame(基金代码, 报告日期, 债券代码, 债券内码, 持仓占比)
        已按STYLE去重，每只转债每只基金仅保留一条记录
    """
    sql = """
          SELECT c_fd_code       AS 基金代码, 
                 c_report_date   AS 报告日期, 
                 c_bd_code       AS 债券代码, 
                 c_bd_inner_code AS 债券内码, 
                 c_nav_ratio     AS 持仓占比, 
                 c_style
          FROM tytdata.tb_fd_portfolio_bd
          WHERE c_fd_code IN :code_list
            AND c_report_date = :report_dt
            AND c_bd_type = '2' 
          """

    df = doris_fetcher.batch_query(sql, fund_codes, report_dt=report_dt)

    # 去重：STYLE优先级 '01' > '02' > '03' > '04'，保留最优先的记录
    return (df.sort_values('c_style', ascending=True)
            .drop_duplicates(subset=['基金代码', '报告日期', '债券内码'], keep='first')
            .drop(columns='c_style'))


def get_fund_iv_stock(fund_codes: list[str], report_dt: str) -> pd.DataFrame:
    """获取基金股票持仓数据

    Args:
        fund_codes: 基金代码列表 ['000001', '000003']
        report_dt: 报告期日期 'YYYY-MM-DD'（季度末）

    Returns:
        DataFrame(基金代码, 报告日期, 股票代码, 持仓占比)
        已按STYLE去重，每只股票每只基金仅保留一条记录
    """
    sql = """
          SELECT c_fd_code     AS 基金代码, 
                 c_report_date AS 报告日期, 
                 c_stk_code    AS 股票代码, 
                 c_nav_ratio   AS 持仓占比, 
                 c_style 
          FROM tytdata.tb_fd_portfolio_stk
          WHERE c_fd_code IN :code_list
            AND c_report_date = :report_dt
          """

    df = doris_fetcher.batch_query(sql, fund_codes, report_dt=report_dt)

    # 去重：STYLE优先级 '01' > '02' > '03' > '04'，保留最优先的记录
    return (df.sort_values('c_style', ascending=True)
            .drop_duplicates(subset=['基金代码', '报告日期', '股票代码'], keep='first')
            .drop(columns='c_style'))


if __name__ == '__main__':
    main_fund_codes = ['000003', '000001']
    main_begin_date = '2020-01-01'
    main_end_date = '2024-12-31'

    main_fund_nav = get_fund_iv_stock(main_fund_codes, main_end_date)

