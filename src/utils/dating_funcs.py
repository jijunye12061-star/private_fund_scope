#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: jijunye
@file: dating_funcs.py
@time: 2025/10/29 16:56
@description:
该模块包含了一些处理日期的函数，包括
generate_report_dates: 生成报告期日期
find_next_report_date: 找到下一个报告期日期
get_next_index_transfer_days: 找到符合某几个月第几个周几的下一交易日
"""
import pandas as pd
from utils.const_variables import finance


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


def find_next_report_date(date, containing=True):
    """
    找到下一个报告期日期
    :param date:  任意日期
    :param containing: 是否包含当日，默认包含
    :return: 下一个报告期日期
    """
    if isinstance(date, str):
        date = pd.to_datetime(date)
    if not containing:
        date = date + pd.DateOffset(days=1)

    year = date.year
    # 定义每年的四个报告期
    quarter_ends = ['03-31', '06-30', '09-30', '12-31']

    # 找到当前日期对应的季度索引
    quarter = (date.month - 1) // 3
    return f'{year}-{quarter_ends[quarter]}'


def get_next_index_transfer_days(months=None, week_num=None, weekday=None):
    """
    获取符合某几个月第几个周几的下一交易日。

    参数:
        months (list): 指定的月份列表（1-12，若为空则选择所有月份）
        week_num (int): 指定第几周（1为第一周，2为第二周...）
        weekday (int): 指定周几（0为周一，1为周二，依此类推，6为周日）

    返回:
        pd.Series: 符合规则的下一交易日的列表
    """
    trading_dt = finance.TRADING_DT
    # 找出符合月份条件的交易日
    if months:
        trading_dt = finance.TRADING_DT[finance.TRADING_DT.dt.month.isin(months)]

    # 找到每个月的第 week_num 个 weekday
    result_days = []
    for month, group in trading_dt.groupby([trading_dt.dt.year, trading_dt.dt.month]):
        # 按照周几过滤
        group_weekday = group[group.dt.weekday == weekday]

        # 获取该月第 week_num 个周几
        if len(group_weekday) >= week_num:
            result_days.append(group_weekday.iloc[week_num - 1])

    # 获取这些日期的下一个交易日
    next_trading_days = []
    for day in result_days:
        # 获取给定日期之后的第一个交易日
        next_day = trading_dt[trading_dt > day].min()
        next_trading_days.append(next_day)

    return pd.Series(next_trading_days)


