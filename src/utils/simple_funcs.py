import pandas as pd
import numpy as np


def export_to_excel(df_dict, filename):
    """
    Export multiple DataFrames to different sheets in an Excel file

    Parameters:
    df_dict: Dict with sheet names as keys and DataFrames as values
    filename: str, path to save the Excel file
    """
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        for sheet_name, df in df_dict.items():
            if isinstance(df.index[0], pd.Timestamp):
                df.index = df.index.strftime('%Y-%m-%d')
            df.to_excel(writer, sheet_name=sheet_name)


def generate_report_dates(last_report_dt, n):
    """
    根据输入的最后一个报告期， 生成最近 n 个报告期的日期
    :param last_report_dt: 结尾的报告期日期，格式为 'YYYY-MM-DD'
    :param n: 输出的报告期个数
    :return: 最近 n 个报告期的日期列表
    """
    # 定义每年的四个报告期
    quarter_ends = ['03-31', '06-30', '09-30', '12-31']

    # 将输入的日期字符串转换为 datetime 类型
    last_date = pd.to_datetime(last_report_dt)

    # 获取当前年的报告期
    year = last_date.year
    current_quarter_end = last_date.strftime('%m-%d')

    # 找到当前日期对应的季度索引
    if current_quarter_end in quarter_ends:
        quarter_index = quarter_ends.index(current_quarter_end)
    else:
        raise ValueError("输入日期不是有效的报告期日期")

    # 生成 n 个报告期
    report_dates = []
    for i in range(n):
        # 获取对应的年份和季度
        quarter = quarter_index % 4
        report_year = year + (quarter_index // 4)
        report_dates.append(f'{report_year}-{quarter_ends[quarter]}')

        # 更新季度索引
        quarter_index -= 1

    # 返回按升序排序的报告期日期
    return sorted(report_dates)


def calculate_metrics(nav_series):
    """
    计算净值序列的最大回撤、波动率和年化收益率

    参数:
    nav_series: pd.Series - 带有日期索引的净值序列

    返回:
    tuple - (最大回撤, 年化波动率, 年化收益率)
    """
    # 计算收益率序列
    returns = nav_series.pct_change().dropna()

    # 计算最大回撤
    cummax = nav_series.cummax()
    drawdown = (nav_series - cummax) / cummax
    max_drawdown = abs(drawdown.min())

    # 计算年化波动率
    annual_volatility = returns.std() * np.sqrt(252)

    # 计算年化收益率
    total_days = (nav_series.index[-1] - nav_series.index[0]).days
    total_return = nav_series.iloc[-1] / nav_series.iloc[0] - 1
    annual_return = (1 + total_return) ** (365 / total_days) - 1

    return max_drawdown, annual_volatility, annual_return



