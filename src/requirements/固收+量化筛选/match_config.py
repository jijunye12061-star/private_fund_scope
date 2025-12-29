#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: jijunye
@file: match_config.py
@time: 2025/12/19 15:38
@description:
"""
# config.py
"""配置文件"""

# 指数配置
INDEX_CONFIGS = {
    '000016': '上证50',
    '000300': '沪深300',
    '000905': '中证500',
    '000852': '中证1000',
    '399303': '国证2000',
    '399006': '创业板指',
    '000688': '科创50',
    '000510': '中证A500',
    'HIS': '恒生指数',
    'HSTECH': '恒生科技',
    '861520': '微盘股'
}

# 报告期配置
REPORT_DATES = ['2024-12-31', '2025-03-31', '2025-06-30', '2025-09-30']

# 评分权重
SCORE_WEIGHTS = {
    'market_cap_coverage': 0.6,
    'overlap_ratio': 0.4
}

# 最佳匹配数量
TOP_N_MATCHES = 3