"""投资组合指标计算"""
import pandas as pd
import numpy as np


def calculate_annualized_return(nav_series: pd.Series) -> float:
    """计算年化收益率

    Args:
        nav_series: 净值序列，索引为日期

    Returns:
        年化收益率（小数，0.1 = 10%）
    """
    if not isinstance(nav_series.index, pd.DatetimeIndex):
        nav_series.index = pd.to_datetime(nav_series.index)

    total_days = (nav_series.index[-1] - nav_series.index[0]).days
    if total_days == 0:
        return 0.0

    total_return = nav_series.iloc[-1] / nav_series.iloc[0] - 1
    return (1 + total_return) ** (365 / total_days) - 1


def calculate_sharpe_ratio(nav_series: pd.Series, risk_free_rate: float = 0.02) -> float:
    """计算夏普比率

    Args:
        nav_series: 净值序列（日度）
        risk_free_rate: 年化无风险收益率

    Returns:
        夏普比率
    """
    daily_returns = nav_series.pct_change().dropna()

    if len(daily_returns) < 2:
        return np.nan

    annual_return = (1 + daily_returns.mean()) ** 252 - 1
    annual_volatility = daily_returns.std() * np.sqrt(252)

    return (annual_return - risk_free_rate) / annual_volatility if annual_volatility else np.nan


def calculate_metrics(nav_series: pd.Series) -> tuple[float, float, float]:
    """计算最大回撤、年化波动率、年化收益率

    Args:
        nav_series: 净值序列，索引为日期

    Returns:
        (最大回撤, 年化波动率, 年化收益率)
    """
    returns = nav_series.pct_change().dropna()

    # 最大回撤
    cummax = nav_series.cummax()
    drawdown = (nav_series - cummax) / cummax
    max_drawdown = abs(drawdown.min())

    # 年化波动率
    annual_volatility = returns.std() * np.sqrt(252)

    # 年化收益率
    total_days = (nav_series.index[-1] - nav_series.index[0]).days
    total_return = nav_series.iloc[-1] / nav_series.iloc[0] - 1
    annual_return = (1 + total_return) ** (365 / total_days) - 1

    return max_drawdown, annual_volatility, annual_return


def calculate_yearly_performance(nav_series: pd.Series,
                                 benchmark_series: pd.Series = None) -> pd.DataFrame:
    """计算分年度收益率

    Args:
        nav_series: 净值序列
        benchmark_series: 基准序列（可选）

    Returns:
        包含年度收益率、波动率、回撤的DataFrame
    """
    if not isinstance(nav_series.index, pd.DatetimeIndex):
        nav_series.index = pd.to_datetime(nav_series.index)

    years = sorted(set(nav_series.index.year))
    yearly_stats = []

    for year in years:
        year_nav = nav_series[nav_series.index.year == year]

        if len(year_nav) == 0:
            continue

        # 收益率和波动率
        yearly_return = year_nav.iloc[-1] / year_nav.iloc[0] - 1
        daily_returns = year_nav.pct_change()
        volatility = daily_returns.std() * np.sqrt(len(daily_returns))

        # 最大回撤
        running_max = year_nav.cummax()
        drawdown = (year_nav - running_max) / running_max
        max_drawdown = drawdown.min()

        stats = {
            '年份': year,
            '收益率': yearly_return,
            '波动率': volatility,
            '最大回撤': max_drawdown
        }

        # 基准比较
        if benchmark_series is not None:
            year_bench = benchmark_series[benchmark_series.index.year == year]
            if len(year_bench) > 0:
                bench_return = year_bench.iloc[-1] / year_bench.iloc[0] - 1
                stats['基准收益率'] = bench_return
                stats['超额收益率'] = yearly_return - bench_return

        yearly_stats.append(stats)

    return pd.DataFrame(yearly_stats).set_index('年份')


def format_annualized_return(nav_series: pd.Series,
                             percentage: bool = True,
                             decimal_places: int = 2) -> str:
    """格式化年化收益率输出"""
    ret = calculate_annualized_return(nav_series)

    if percentage:
        return f"{(ret * 100):.{decimal_places}f}%"
    return f"{ret:.{decimal_places}f}"