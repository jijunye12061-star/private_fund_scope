"""
计算净值曲线的一些指标，主要包括如下函数：
calculate_annualized_return：计算净值序列的年化收益率
coding:utf-8
@Time:2024/11/13 15:30
@Author: 季俊晔
"""
import pandas as pd
import numpy as np


def calculate_annualized_return(nav_series: pd.Series) -> float:
    """
    计算净值序列的年化收益率
    :param nav_series: pd.Series - 净值序列，index为datetime格式的日期
    :return: float - 年化收益率，以小数形式返回（如0.1表示10%）
    """
    # 确保索引是日期类型
    if not isinstance(nav_series.index, pd.DatetimeIndex):
        nav_series.index = pd.to_datetime(nav_series.index)

    # 获取起始和结束日期
    start_date = nav_series.index[0]
    end_date = nav_series.index[-1]

    # 计算总天数
    total_days = (end_date - start_date).days

    # 如果天数为0，返回0
    if total_days == 0:
        return 0.0

    # 计算总收益率
    total_return = nav_series.iloc[-1] / nav_series.iloc[0] - 1

    # 计算年化收益率
    # 使用实际天数计算，考虑闰年
    annualized_return = (1 + total_return) ** (365 / total_days) - 1

    return annualized_return


# 添加一个带格式化输出的版本
def format_annualized_return(nav_series: pd.Series,
                             percentage: bool = True,
                             decimal_places: int = 2) -> str:
    """
    计算并格式化年化收益率

    Parameters:
    nav_series: pd.Series - 净值序列，index为datetime格式的日期
    percentage: bool - 是否以百分比格式返回
    decimal_places: int - 小数位数

    Returns:
    str - 格式化的年化收益率字符串
    """
    annualized_return = calculate_annualized_return(nav_series)

    if percentage:
        return f"{(annualized_return * 100):.{decimal_places}f}%"
    else:
        return f"{annualized_return:.{decimal_places}f}"


def calculate_yearly_performance(nav_series, benchmark_series=None):
    """
    计算投资组合的分年度收益率和超额收益率
    :param nav_series: Series，净值曲线（日期索引）
    :param benchmark_series: Series，基准指数净值曲线（日期索引），可选
    :return: DataFrame，包含各年度收益率和超额收益率
    """
    # 确保索引是日期类型
    if not isinstance(nav_series.index, pd.DatetimeIndex):
        nav_series.index = pd.to_datetime(nav_series.index)

    # 获取年份列表
    years = sorted(set(nav_series.index.year))

    yearly_stats = []
    for year in years:
        # 获取当年的净值数据
        year_nav = nav_series[nav_series.index.year == year]

        if len(year_nav) > 0:
            # 计算当年收益率
            yearly_return = year_nav.iloc[-1] / year_nav.iloc[0] - 1

            # 计算当年波动率
            daily_returns = year_nav.pct_change()
            volatility = daily_returns.std() * np.sqrt(len(daily_returns))

            # 计算当年最大回撤
            running_max = year_nav.cummax()
            drawdown = (year_nav - running_max) / running_max
            max_drawdown = drawdown.min()

            year_stats = {
                '年份': year,
                '收益率': yearly_return,
                '波动率': volatility,
                '最大回撤': max_drawdown
            }

            # 如果提供了基准指数，计算超额收益
            if benchmark_series is not None:
                # 获取当年的基准指数数据
                year_bench = benchmark_series[benchmark_series.index.year == year]

                if len(year_bench) > 0:
                    # 计算基准收益率
                    bench_return = year_bench.iloc[-1] / year_bench.iloc[0] - 1
                    # 计算超额收益
                    excess_return = yearly_return - bench_return

                    year_stats.update({
                        '基准收益率': bench_return,
                        '超额收益率': excess_return
                    })

            yearly_stats.append(year_stats)

    # 转换为DataFrame
    yearly_stats_df = pd.DataFrame(yearly_stats)
    yearly_stats_df.set_index('年份', inplace=True)

    return yearly_stats_df


def calculate_sharpe_ratio(nav_series, risk_free_rate=0.02):
    """
    计算夏普比率

    Parameters:
    nav_series: pandas Series, 净值序列（日度数据）
    risk_free_rate: float, 年化无风险收益率，默认为0

    Returns:
    float: 夏普比率
    """
    # 计算日收益率
    daily_returns = nav_series.pct_change().dropna()

    if len(daily_returns) < 2:
        return np.nan

    # 年化收益率
    annual_return = (1 + daily_returns.mean()) ** 252 - 1

    # 年化波动率
    annual_volatility = daily_returns.std() * np.sqrt(252)

    # 夏普比率
    if annual_volatility == 0:
        return np.nan

    sharpe_ratio = (annual_return - risk_free_rate) / annual_volatility

    return sharpe_ratio

if __name__ == "__main__":
    from utils.query_data_funcs import fetcher
    fund_nav = fetcher.query_fund_nav(['001691'], '2022-06-11', '2025-06-11')

    result = calculate_sharpe_ratio(fund_nav.set_index('交易日期')['复权净值'], 0.02)
    pass
