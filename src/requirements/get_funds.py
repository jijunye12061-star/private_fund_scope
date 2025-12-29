#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: jijunye
@file: get_funds.py
@time: 2025/11/17 17:43
@description:
"""
from utils.query_data_funcs import doris_fetcher, fetcher
from utils.dating_funcs import generate_report_dates
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from utils.fund_backtrader_new import WeightBasedBacktest, PortfolioEvaluator, BasePortfolioBacktest
import logging

def generate_quarter_start_25th(end_date):
    """
    从季度末日期生成对应的下一个季度初25日

    Args:
        end_date: str or datetime, 季度末日期，如 '2024-09-30'

    Returns:
        str: 下一个季度初的25日，格式 'YYYY-MM-DD'

    Examples:
        generate_quarter_start_25th('2024-09-30') -> '2024-10-25'
        generate_quarter_start_25th('2024-12-31') -> '2025-01-25'
    """
    trading_dts = fetcher.get_trading_dt("2021-01-01", "2025-11-20")
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')

    # 计算下个月的第一天（即下季度的第一个月）
    next_quarter_start = end_date + timedelta(days=1)

    # 设置为该月的25日
    target_date = next_quarter_start.replace(day=25)
    pos = trading_dts.searchsorted(target_date, side='left')
    new_target_date = trading_dts[pos]

    return new_target_date.strftime('%Y-%m-%d')

def get_funds(end_date: str):
    report_dts = generate_report_dates(end_date, 3)
    query_sql = f"""
                  SELECT
    asset_t0.c_fd_code
FROM
    tytdata.tb_fd_asset_allocation asset_t0  -- 当前期 (T0: 2025-09-30)
LEFT JOIN
    tytdata.tb_fd_basic_info info
ON
    info.c_fd_code = asset_t0.c_fd_code
-- 第一次自连接：前一期 (T-1: 2025-06-30)
INNER JOIN
    tytdata.tb_fd_asset_allocation asset_t1
ON
    asset_t0.c_fd_code = asset_t1.c_fd_code AND asset_t1.c_report_date = :report_dt1
-- 第二次自连接：前两期 (T-2: 2025-03-31)
INNER JOIN
    tytdata.tb_fd_asset_allocation asset_t2
ON
    asset_t0.c_fd_code = asset_t2.c_fd_code AND asset_t2.c_report_date = :report_dt2
WHERE
    asset_t0.c_report_date = :report_dt0  -- 筛选 T0 数据
    AND info.c_class2_code = '003002'
    AND info.c_fd_code = info.c_init_code
    AND info.c_regular_open_status = '0'
    AND info.c_full_name NOT LIKE '%持有%'
    AND asset_t0.c_fund_nav_total > 2e8
    -- T0 期的权益比例 < 10 (原条件)
    AND COALESCE(asset_t0.c_stk_total_ratio, 0) + COALESCE(asset_t0.c_bd_convertible_ratio, 0) * 0.5 < 10
    -- T-1 期的权益比例 < 10 (新增条件)
    AND COALESCE(asset_t1.c_stk_total_ratio, 0) + COALESCE(asset_t1.c_bd_convertible_ratio, 0) * 0.5 < 10
    -- T-2 期的权益比例 < 10 (新增条件)
    AND COALESCE(asset_t2.c_stk_total_ratio, 0) + COALESCE(asset_t2.c_bd_convertible_ratio, 0) * 0.5 < 10
    AND COALESCE(asset_t0.c_stk_total_ratio, 0) + COALESCE(asset_t0.c_bd_convertible_ratio, 0) * 0.5 > 3
    AND COALESCE(asset_t1.c_stk_total_ratio, 0) + COALESCE(asset_t1.c_bd_convertible_ratio, 0) * 0.5 > 3
    AND COALESCE(asset_t2.c_stk_total_ratio, 0) + COALESCE(asset_t2.c_bd_convertible_ratio, 0) * 0.5 > 3
                """
    result = doris_fetcher.query(query_sql, report_dt0=report_dts[2], report_dt1=report_dts[1], report_dt2=report_dts[0])
    result['trade_dt'] = generate_quarter_start_25th(end_date)
    result["持仓权重"] = 1 / len(result)
    result.columns = ["基金代码", "交易日期", "持仓权重"]
    return result

def weight_backtest(trade_info: pd.DataFrame):
    start_date = trade_info['交易日期'].min()
    end_date = '2025-11-14'
    benchmark_index = '809007'
    flat_fee = 0.0
    percentage_fee = 0.0

    fund_codes = trade_info['基金代码'].unique().tolist()
    config_dict = {'level': logging.INFO,
                   'log_file': 'backtest.log',
                   'if_console': True,
                   'if_file': True}
    portfolio_manager = WeightBasedBacktest(fund_codes, start_date, end_date,
                                            benchmark_index, nav_type='adj',
                                            flat_fee=flat_fee, percentage_fee=percentage_fee,
                                            **config_dict)
    portfolio_manager.backtest(trade_info, redemption_type='yesterday')

    return portfolio_manager


def evaluation(portfolio_manager: BasePortfolioBacktest):
    evaluator = PortfolioEvaluator(portfolio_manager)
    trade_info = portfolio_manager.trade_info
    trade_info[['交易日期', '确认日期']] = trade_info[['交易日期', '确认日期']].apply(
        lambda x: x.dt.strftime('%Y-%m-%d'))
    navs_df = pd.DataFrame({'组合单位净值': portfolio_manager.portfolio_series,
                            '基准净值': portfolio_manager.benchmark_nav_series,
                            '组合净值': portfolio_manager.nav_df.sum(axis=1),
                            '组合成本': portfolio_manager.costs_df.sum(axis=1)})

    results = {
        '基金调仓信息': trade_info,
        '持仓净值': portfolio_manager.nav_df,
        '持仓份额': portfolio_manager.units_df,
        '组合成本': portfolio_manager.costs_df,
        '组合净值': navs_df,
        '净值指标': pd.concat([pd.DataFrame(evaluator.calculate_performance()[x])
                               for x in ['rolling', 'yearly']], axis=1),
        '月度收益': evaluator.calculate_monthly_returns(),
        '持仓贡献度': evaluator.calculate_contributions()
    }

    return results


def calculate_portfolio_metrics(nav_series, risk_free_rate=0.02, freq=None):
    """
    计算投资组合的风险收益指标

    Args:
        nav_series: pandas.Series，索引为时间，值为净值
        risk_free_rate: float，无风险利率（年化），默认2%
        freq: str，数据频率 ('D'日, 'W'周, 'M'月)，None为自动识别

    Returns:
        dict: 包含各项指标的字典
    """

    # 数据预处理
    nav_series = nav_series.dropna().sort_index()

    if len(nav_series) < 2:
        return None

    # 自动识别数据频率
    if freq is None:
        time_diff = nav_series.index[1] - nav_series.index[0]
        if time_diff.days <= 1:
            freq = 'D'  # 日频
            periods_per_year = 252  # 交易日
        elif time_diff.days <= 7:
            freq = 'W'  # 周频
            periods_per_year = 52
        elif time_diff.days <= 31:
            freq = 'M'  # 月频
            periods_per_year = 12
        else:
            freq = 'D'
            periods_per_year = 252
    else:
        periods_per_year = {'D': 252, 'W': 52, 'M': 12}.get(freq, 252)

    # 计算收益率
    returns = nav_series.pct_change().dropna()

    # 1. 累计收益率
    cumulative_return = (nav_series.iloc[-1] / nav_series.iloc[0]) - 1

    # 2. 年化收益率
    total_periods = len(nav_series) - 1
    years = total_periods / periods_per_year
    annualized_return = (nav_series.iloc[-1] / nav_series.iloc[0]) ** (1 / years) - 1

    # 3. 年化波动率
    annualized_volatility = returns.std() * np.sqrt(periods_per_year)

    # 4. 夏普比率
    sharpe_ratio = (annualized_return - risk_free_rate) / annualized_volatility if annualized_volatility != 0 else 0

    # 5. 下行波动率（用于索提诺比率）
    downside_returns = returns[returns < 0]
    if len(downside_returns) > 0:
        downside_volatility = downside_returns.std() * np.sqrt(periods_per_year)
        sortino_ratio = (annualized_return - risk_free_rate) / downside_volatility if downside_volatility != 0 else 0
    else:
        downside_volatility = 0
        sortino_ratio = np.inf  # 没有下行风险

    # 6. 回撤计算
    cumulative_nav = nav_series / nav_series.iloc[0]  # 标准化到1开始
    running_max = cumulative_nav.expanding().max()
    drawdown = (cumulative_nav - running_max) / running_max
    max_drawdown = drawdown.min()  # 最大回撤（负值）

    # 7. 卡玛比率
    calmar_ratio = annualized_return / abs(max_drawdown) if max_drawdown != 0 else 0

    # 8. 其他有用指标
    win_rate = (returns > 0).mean()  # 胜率

    # 计算回撤持续时间
    is_drawdown = drawdown < 0
    drawdown_periods = []
    current_period = 0

    for dd in is_drawdown:
        if dd:
            current_period += 1
        else:
            if current_period > 0:
                drawdown_periods.append(current_period)
            current_period = 0

    if current_period > 0:  # 如果以回撤结束
        drawdown_periods.append(current_period)

    max_drawdown_duration = max(drawdown_periods) if drawdown_periods else 0

    return {
        '累计收益率': round(cumulative_return * 100, 2),  # 百分比
        '年化收益率': round(annualized_return * 100, 2),  # 百分比
        '年化波动率': round(annualized_volatility * 100, 2),  # 百分比
        '最大回撤': round(max_drawdown * 100, 2),  # 百分比
        '夏普比率': round(sharpe_ratio, 3),
        '索提诺比率': round(sortino_ratio, 3),
        '卡玛比率': round(calmar_ratio, 3),
        '胜率': round(win_rate * 100, 2),  # 百分比
        '最大回撤持续期': max_drawdown_duration,
        '数据频率': freq,
        '观测期数': len(nav_series),
        '观测年数': round(years, 2)
    }


# 增强版：包含更多指标
def calculate_advanced_portfolio_metrics(nav_series, risk_free_rate=0.02, benchmark_series=None):
    """
    计算高级投资组合指标（包含基准比较）

    Args:
        nav_series: 组合净值序列
        risk_free_rate: 无风险利率
        benchmark_series: 基准净值序列（可选）

    Returns:
        dict: 完整的指标字典
    """

    # 基础指标
    basic_metrics = calculate_portfolio_metrics(nav_series, risk_free_rate)

    if basic_metrics is None:
        return None

    # 如果有基准，计算相对指标
    if benchmark_series is not None:
        # 确保日期对齐
        aligned_data = pd.DataFrame({
            'portfolio': nav_series,
            'benchmark': benchmark_series
        }).dropna()

        if len(aligned_data) > 1:
            portfolio_returns = aligned_data['portfolio'].pct_change().dropna()
            benchmark_returns = aligned_data['benchmark'].pct_change().dropna()

            # 计算 Alpha 和 Beta
            if len(portfolio_returns) == len(benchmark_returns) and len(portfolio_returns) > 1:
                covariance = np.cov(portfolio_returns, benchmark_returns)[0, 1]
                benchmark_variance = np.var(benchmark_returns)
                beta = covariance / benchmark_variance if benchmark_variance != 0 else 0

                benchmark_annualized_return = calculate_portfolio_metrics(benchmark_series, risk_free_rate)[
                                                  '年化收益率'] / 100
                alpha = (basic_metrics['年化收益率'] / 100) - (
                            risk_free_rate + beta * (benchmark_annualized_return - risk_free_rate))

                # 信息比率
                excess_returns = portfolio_returns - benchmark_returns
                tracking_error = excess_returns.std() * np.sqrt(252)
                information_ratio = excess_returns.mean() * 252 / tracking_error if tracking_error != 0 else 0

                basic_metrics.update({
                    'Alpha': round(alpha * 100, 2),
                    'Beta': round(beta, 3),
                    ' 信息比率': round(information_ratio, 3),
                    '跟踪误差': round(tracking_error * 100, 2)
                })

    return basic_metrics


# 使用示例
def demo_usage():
    """使用示例"""
    # 创建示例数据
    dates = pd.date_range('2023-01-01', '2024-01-01', freq='D')
    # 模拟净值曲线（随机游走 + 趋势）
    np.random.seed(42)
    returns = np.random.normal(0.0005, 0.015, len(dates))  # 日收益率
    nav_values = [1.0]
    for ret in returns[1:]:
        nav_values.append(nav_values[-1] * (1 + ret))

    nav_series = pd.Series(nav_values, index=dates)

    # 计算指标
    metrics = calculate_portfolio_metrics(nav_series)

    print("投资组合风险收益指标:")
    print("-" * 30)
    for key, value in metrics.items():
        if isinstance(value, (int, float)):
            if '比率' in key:
                print(f"{key}: {value}")
            elif '率' in key or '撤' in key:
                print(f"{key}: {value}%")
            else:
                print(f"{key}: {value}")
        else:
            print(f"{key}: {value}")

    return nav_series, metrics


if __name__ == "__main__":
    total_report_dts = generate_report_dates("2025-09-30", 12)
    total_result = []
    for single_report_dt in total_report_dts:
        single_result = get_funds(single_report_dt)
        total_result.append(single_result)

    main_trade_info = pd.concat(total_result)
    # main_trade_info.to_pickle(r"trade_info.pkl")

    main_portfolio_manager = weight_backtest(main_trade_info)

    main_results = evaluation(main_portfolio_manager)
    from utils.simple_funcs import export_to_excel
    export_to_excel(main_results, r"混合债基回测结果.xlsx")

    nav_data = main_results["组合净值"]
    nav_data.index = pd.to_datetime(nav_data.index)
    nav_series = nav_data["组合单位净值"]
    bench_series = nav_data["基准净值"]

    new_results = calculate_advanced_portfolio_metrics(nav_series, risk_free_rate=0.0, benchmark_series=bench_series)
    # result_df = pd.DataFrame(new_results)

