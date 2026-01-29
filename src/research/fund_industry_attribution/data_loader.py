"""固收+基金行业归因 - 数据加载层"""
import pandas as pd
from utils.data.repositories.fund_repo import (
    get_fund_nav, get_fund_iv_cb, get_fund_asset_detail
)
from utils.data.repositories.index_repo import get_index_nav
from utils.data.repositories.bond_repo import get_bond_daily_nav
from utils.data.repositories.calendar_repo import get_trading_dt
from utils.calendar import generate_report_dates
from .config import ASSET_INDEX_MAP, INDUSTRY_CONFIG


def load_fund_nav(fund_code: str, begin_date: str, end_date: str) -> pd.Series:
    """基金日收益率序列"""
    df = get_fund_nav([fund_code], begin_date, end_date)
    df = df.set_index('交易日期').sort_index()
    return (df['复权净值'] / df['昨复权净值'] - 1).rename('基金收益')


def load_asset_allocation(fund_code: str, end_date: str, lookback: int = 8) -> pd.DataFrame:
    """最近N期资产配置（向前填充到交易日）

    Returns:
        DataFrame(交易日期, 股票占比, 转债占比, 利率债占比, ..., 货币占比, 杠杆率)
    """
    report_dts = generate_report_dates(end_date, lookback)

    dfs = []
    for dt in report_dts:
        df = get_fund_asset_detail([fund_code], dt)
        if not df.empty:
            dfs.append(df)

    if not dfs:
        raise ValueError(f"基金 {fund_code} 无资产配置数据")

    allocation = pd.concat(dfs).set_index('报告日期').sort_index()

    # 向前填充到交易日
    trade_dates = get_trading_dt(allocation.index[0].strftime('%Y-%m-%d'), end_date)
    return allocation.reindex(trade_dates, method='ffill')


def load_convertible_holdings(fund_code: str, end_date: str, lookback: int = 8) -> pd.DataFrame:
    """转债持仓（季报披露部分）

    Returns:
        DataFrame(报告日期, 债券内码, 持仓占比)
    """
    report_dts = generate_report_dates(end_date, lookback)

    dfs = []
    for dt in report_dts:
        df = get_fund_iv_cb([fund_code], dt)
        if not df.empty:
            dfs.append(df[['报告日期', '债券内码', '持仓占比']])

    return pd.concat(dfs) if dfs else pd.DataFrame()


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