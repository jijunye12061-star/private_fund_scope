"""金融日历工具 - 报告期生成、交易日查询"""
import pandas as pd
from functools import lru_cache


@lru_cache(maxsize=1)
def _get_trading_calendar(end_date: str = '2030-12-31') -> pd.DatetimeIndex:
    """获取交易日历（缓存）"""
    from utils.data.repositories.calendar_repo import get_trading_dt
    return get_trading_dt('2000-01-01', end_date)


def generate_report_dates(last_report_dt: str, n: int) -> list[str]:
    """生成最近n个报告期日期

    Args:
        last_report_dt: 最后报告期 'YYYY-MM-DD'
        n: 报告期数量

    Returns:
        升序排列的报告期列表
    """
    quarter_ends = ['03-31', '06-30', '09-30', '12-31']
    last_date = pd.to_datetime(last_report_dt)

    if last_date.strftime('%m-%d') not in quarter_ends:
        raise ValueError("输入日期不是有效的报告期日期")

    year = last_date.year
    quarter_index = quarter_ends.index(last_date.strftime('%m-%d'))

    report_dates = []
    for i in range(n):
        quarter = quarter_index % 4
        report_year = year - (quarter_index // 4)
        report_dates.append(f'{report_year}-{quarter_ends[quarter]}')
        quarter_index -= 1

    return sorted(report_dates)


def find_next_report_date(date: str | pd.Timestamp, containing: bool = True) -> str:
    """找到下一个报告期日期

    Args:
        date: 任意日期
        containing: 是否包含当日

    Returns:
        报告期日期 'YYYY-MM-DD'
    """
    date = pd.to_datetime(date)
    if not containing:
        date = date + pd.DateOffset(days=1)

    quarter_ends = ['03-31', '06-30', '09-30', '12-31']
    quarter = (date.month - 1) // 3
    return f'{date.year}-{quarter_ends[quarter]}'


def get_next_index_transfer_days(months: list[int] = None,
                                 week_num: int = 1,
                                 weekday: int = 0) -> pd.Series:
    """获取符合规则的下一交易日

    Args:
        months: 指定月份列表 (1-12)
        week_num: 第几周 (1=第一周)
        weekday: 周几 (0=周一, 6=周日)

    Returns:
        符合规则的下一交易日序列
    """
    trading_dt = _get_trading_calendar()

    if months:
        trading_dt = trading_dt[trading_dt.month.isin(months)]

    result_days = []
    for (year, month), group in trading_dt.groupby([trading_dt.year, trading_dt.month]):
        group_weekday = group[group.weekday == weekday]
        if len(group_weekday) >= week_num:
            result_days.append(group_weekday[week_num - 1])

    # 获取下一交易日
    all_trading = _get_trading_calendar()
    next_days = [all_trading[all_trading > day].min() for day in result_days]

    return pd.Series(next_days)