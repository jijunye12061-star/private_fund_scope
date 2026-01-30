"""固收+基金行业归因 - 数据加载层"""
import pandas as pd
from utils.data.repositories.fund_repo import (
    get_fund_nav, get_fund_iv_cb, get_fund_asset_detail
)
from utils.data.repositories.index_repo import get_index_nav
from utils.data.repositories.bond_repo import get_bond_daily_nav
from utils.data.repositories.calendar_repo import get_trading_dt
from utils.calendar import generate_report_dates, find_next_report_date
from research.fund_industry_attribution.fund_attr_config import ASSET_INDEX_MAP, INDUSTRY_CONFIG
from typing import List


def load_fund_nav(fund_code: str, begin_date: str, end_date: str) -> pd.Series:
    """基金日收益率序列"""
    df = get_fund_nav([fund_code], begin_date, end_date)
    df = df.set_index('交易日期').sort_index()
    trade_dates = get_trading_dt(begin_date, end_date)
    df = df.reindex(trade_dates, method='ffill')
    return (df['复权净值'] / df['昨复权净值'] - 1).rename('基金收益')


def load_asset_allocation(fund_code: str, begin_date: str, end_date: str) -> pd.DataFrame:
    """资产配置（向前填充到交易日）

    从 begin_date 开始的前一个季度开始查询，确保有数据可 forward fill

    Returns:
        DataFrame(交易日期, 股票占比, 转债占比, 利率债占比, 信用债占比,
                 非政金债占比, ABS占比, 货币占比, 杠杆率)，返回小数
        索引为交易日期，已 forward fill
    """
    # 计算需要查询的报告期范围
    next_dt = find_next_report_date(begin_date, containing=False)
    start_report = generate_report_dates(next_dt, 3)[0]  # begin_date 之前的季度末
    end_report = find_next_report_date(end_date, containing=True)

    # 计算需要回溯的期数
    report_dates_all = generate_report_dates(end_report, 20)  # 足够多
    report_dates = [dt for dt in report_dates_all if dt >= start_report]

    dfs: List[pd.DataFrame] = []
    for dt in report_dates:
        df = get_fund_asset_detail([fund_code], dt)
        if not df.empty:
            dfs.append(df)

    if not dfs:
        raise ValueError(f"基金 {fund_code} 在 {start_report} 至 {end_report} 无资产配置数据")

    allocation = pd.concat(dfs).set_index('披露日期').sort_index()
    pct_columns = ['股票占比', '转债占比', '利率债占比', '信用债占比',
                   '非政金债占比', 'ABS占比', '货币占比']
    allocation[pct_columns] = allocation[pct_columns] / 100

    # 向前填充到交易日
    trade_dates = get_trading_dt(begin_date, end_date)
    return allocation.reindex(trade_dates, method='ffill')


def load_convertible_holdings(fund_code: str, begin_date: str, end_date: str) -> pd.DataFrame:
    """转债持仓（按披露日期展开为日度数据）

    季报持仓从披露日期开始生效，forward fill 到下一披露日

    Returns:
        DataFrame(交易日期, 债券内码, 持仓占比)
        MultiIndex(交易日期, 债券内码)，值为持仓占比
        已按披露日期 forward fill 到日度
    """
    # 查询足够多的报告期
    next_dt = find_next_report_date(begin_date, containing=False)
    start_report = generate_report_dates(next_dt, 3)[0]  # begin_date 之前的季度末
    end_report = find_next_report_date(end_date, containing=True)
    report_dates_all = generate_report_dates(end_report, 20)
    report_dates = [dt for dt in report_dates_all if dt >= start_report]

    dfs: List[pd.DataFrame] = []
    for dt in report_dates:
        df = get_fund_iv_cb([fund_code], dt)
        if not df.empty:
            dfs.append(df[['披露日期', '债券内码', '持仓占比']])

    if not dfs:
        return pd.DataFrame()  # 没有转债持仓

    cb_holdings = pd.concat(dfs)

    # 转成宽表：披露日期 × 债券内码
    pivot = cb_holdings.pivot(index='披露日期', columns='债券内码', values='持仓占比')
    pivot = pivot.fillna(0)  # 新增持仓前为 0

    # Forward fill 到交易日
    trade_dates = get_trading_dt(begin_date, end_date)
    daily_holdings = pivot.reindex(trade_dates, method='ffill').fillna(0)

    # 转回长表格式
    return daily_holdings.stack().rename('持仓占比').reset_index()


def load_bond_index_returns(begin_date: str, end_date: str) -> pd.DataFrame:
    """债券各类指数日收益率

    Returns:
        DataFrame(交易日期, 利率债收益, 信用债收益, 非政金债收益, ABS收益, 转债收益, 货币收益)
    """
    index_codes = list(ASSET_INDEX_MAP.values())
    df = get_index_nav(index_codes, begin_date, end_date)

    df['日收益'] = (df['收盘价'] / df['前收盘价'] - 1)

    # 转宽表
    pivot = df.pivot(index='交易日期', columns='指数代码', values='日收益')

    # 列名映射
    reverse_map = {v: k for k, v in ASSET_INDEX_MAP.items()}
    return pivot.rename(columns={code: f'{reverse_map[code]}收益' for code in pivot.columns})


def load_convertible_returns(cb_inner_codes: list[str], begin_date: str, end_date: str) -> pd.DataFrame:
    """个券转债日收益率

    Returns:
        DataFrame(交易日期, 债券内码, 日收益)
    """
    df = get_bond_daily_nav(cb_inner_codes, begin_date, end_date)
    df['日收益'] = (df['收盘全价'] / df['前收盘全价'] - 1).fillna(0)
    return df[['交易日期', '债券内码', '日收益']]


def load_industry_returns(begin_date: str, end_date: str) -> pd.DataFrame:
    """中信30个行业日收益率

    Returns:
        DataFrame(交易日期, CI005001, CI005002, ...) - 列名为行业代码
    """
    industry_codes = list(INDUSTRY_CONFIG.keys())
    df = get_index_nav(industry_codes, begin_date, end_date)

    df['日收益'] = (df['收盘价'] / df['前收盘价'] - 1)
    return df.pivot(index='交易日期', columns='指数代码', values='日收益')


def load_trading_calendar(begin_date: str, end_date: str) -> pd.DatetimeIndex:
    """交易日序列"""
    return get_trading_dt(begin_date, end_date)


if __name__ == "__main__":
    # test_asset = load_asset_allocation('000003', '2025-01-30', '2025-12-31')

    test_cb = load_convertible_holdings('000003', '2025-07-30', '2025-12-31')

    inner_codes = test_cb['债券内码'].unique().tolist()
    cb_nav = get_bond_daily_nav(inner_codes, "2025-07-31", "2025-08-31")

    index_ret = load_bond_index_returns("2025-07-31", "2025-08-31")
