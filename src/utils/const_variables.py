#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: jijunye
@file: const_variables.py
@time: 2025/10/29 17:00
@description:
"""
from src.utils.query_data_funcs import fetcher
from datetime import datetime


class FinanceConstants:
    """金融常量配置类"""

    def __init__(self, end_date: str = None):
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        self.END_DATE = end_date

    @property
    def TRADING_DT(self):
        """交易日历"""
        trading_dt_query = """
        SELECT
            TDATE as 交易日期
        from TYTFUND.TRAD_ID_DAILY
        where SECURITYVARIETYCODE = '1000158679'
        AND TDATE <= TO_DATE(:end_date, 'YYYY-MM-DD')
        """
        trading_dt = fetcher.query_data_tytfund(trading_dt_query, end_date=self.END_DATE)
        return trading_dt['交易日期']

    @property
    def CREDIT_INTEREST_BOND(self):
        """信用债/利率债类型"""
        return {
            '利率债': ['国债', '政策性金融债', '央行票据', '地方政府债'],
            '信用债': ['金融债', '企业债', '中期票据', '企业短融', '资产支持证券']
        }

    @property
    def CREDIT_INTEREST_BOND3(self):
        """信用债/利率债/金融债类型"""
        return {
            '利率债': ['国债', '政策性金融债', '央行票据', '地方政府债'],
            '汇总金融债': ['金融债', '同业存单'],
            '信用债': ['企业债', '中期票据', '企业短融', '资产支持证券']
        }


# 创建单例实例
finance = FinanceConstants('2025-12-31')
