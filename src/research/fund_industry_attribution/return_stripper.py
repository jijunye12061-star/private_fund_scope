# src/research/fund_industry_attribution/return_stripper.py
"""固收+基金收益剥离引擎"""
import pandas as pd
import numpy as np
from typing import Optional
from .data_loader import (
    load_fund_nav, load_asset_allocation, load_convertible_holdings,
    load_bond_index_returns, load_convertible_returns
)


class ReturnStripper:
    """单个基金的收益剥离

    将基金总收益拆解为：股票 + 债券 + 转债 + 货币
    通过剥离已知部分，反推股票端收益
    """

    def __init__(self,
                 fund_code: str,
                 begin_date: str,
                 end_date: str,
                 shared_indices: Optional[dict] = None):
        """
        Args:
            fund_code: 基金代码
            begin_date: 起始日期 'YYYY-MM-DD'
            end_date: 结束日期 'YYYY-MM-DD'
            shared_indices: 预加载的公共数据 {'bond': DataFrame, 'cb_index': Series}
                - bond: 债券各类指数收益
                - cb_index: 中证转债指数收益（可选，会从 bond 中提取）
        """
        self.fund_code = fund_code
        self.begin_date = begin_date
        self.end_date = end_date

        # 加载基金特有数据
        self.fund_return = load_fund_nav(fund_code, begin_date, end_date)
        self.allocation = load_asset_allocation(fund_code, begin_date, end_date)
        self.cb_holdings_daily = load_convertible_holdings(fund_code, begin_date, end_date)

        # 公共数据：复用或加载
        if shared_indices and 'bond' in shared_indices:
            self.bond_returns = shared_indices['bond'].reindex(self.fund_return.index)
        else:
            self.bond_returns = load_bond_index_returns(begin_date, end_date)

        # 提取转债指数收益
        self.cb_index_return = self.bond_returns.get('转债收益', pd.Series(0, index=self.fund_return.index))

        # 转债个券收益（延迟加载）
        self._cb_returns = None

    @property
    def cb_returns(self) -> pd.DataFrame:
        """转债个券收益（延迟加载）"""
        if self._cb_returns is None and not self.cb_holdings_daily.empty:
            cb_codes = self.cb_holdings_daily['债券内码'].unique().tolist()
            self._cb_returns = load_convertible_returns(cb_codes, self.begin_date, self.end_date)
        return self._cb_returns

    def _calc_bond_return(self) -> pd.Series:
        """计算债券收益（利率债 + 信用债 + 非政金债 + ABS）"""
        bond_types = ['利率债', '信用债', '非政金债', 'ABS']
        total_return = pd.Series(0.0, index=self.fund_return.index)

        for bond_type in bond_types:
            weight = self.allocation[f'{bond_type}占比'] / 100  # 转为小数
            return_col = f'{bond_type}收益'
            if return_col in self.bond_returns.columns:
                total_return += weight * self.bond_returns[return_col]

        return total_return

    def _calc_cb_return(self) -> pd.Series:
        """计算转债收益（披露个券 + 未披露部分用指数）"""
        if self.cb_holdings_daily.empty:
            # 无转债持仓，直接用总转债占比 × 指数收益
            weight = self.allocation['转债占比'] / 100
            return weight * self.cb_index_return

        # 1. 计算披露转债的加权收益
        disclosed_return = self._calc_disclosed_cb_return()

        # 2. 计算披露占比总和（日度）
        holdings_pivot = self.cb_holdings_daily.pivot(
            index='交易日期', columns='债券内码', values='持仓占比'
        )
        disclosed_pct = holdings_pivot.sum(axis=1)  # 每日披露转债占比总和

        # 3. 未披露占比 = 总转债占比 - 披露占比（clip 到 0）
        total_cb_pct = self.allocation['转债占比']
        undisclosed_pct = (total_cb_pct - disclosed_pct).clip(lower=0)

        # 4. 合并收益
        total_return = (disclosed_return * disclosed_pct / 100 +
                        self.cb_index_return * undisclosed_pct / 100)

        return total_return.fillna(0)

    def _calc_disclosed_cb_return(self) -> pd.Series:
        """计算披露转债的加权平均收益"""
        if self.cb_returns is None or self.cb_returns.empty:
            return pd.Series(0.0, index=self.fund_return.index)

        # 转成宽表：日期 × 债券内码
        cb_ret_pivot = self.cb_returns.pivot(
            index='交易日期', columns='债券内码', values='日收益'
        ).reindex(self.fund_return.index, fill_value=0)

        holdings_pivot = self.cb_holdings_daily.pivot(
            index='交易日期', columns='债券内码', values='持仓占比'
        ).reindex(self.fund_return.index, fill_value=0)

        # 向量化加权：Σ(持仓占比 × 日收益)
        weighted_return = (holdings_pivot * cb_ret_pivot).sum(axis=1)

        # 归一化：除以总持仓占比（避免除0）
        total_weight = holdings_pivot.sum(axis=1).replace(0, np.nan)
        return (weighted_return / total_weight).fillna(0)

    def _calc_cash_return(self) -> pd.Series:
        """计算货币收益"""
        weight = self.allocation['货币占比'] / 100
        cash_ret = self.bond_returns.get('货币收益', pd.Series(0, index=self.fund_return.index))
        return weight * cash_ret

    def strip_to_equity(self) -> pd.Series:
        """剥离得到股票端收益

        Returns:
            Series: 股票端日收益率

        Side Effects:
            保存中间结果到实例属性：
            - bond_return_: 债券收益
            - cb_return_: 转债收益
            - cash_return_: 货币收益
        """
        # 计算各部分收益
        self.bond_return_ = self._calc_bond_return()
        self.cb_return_ = self._calc_cb_return()
        self.cash_return_ = self._calc_cash_return()

        # 股票收益 = 基金总收益 × 杠杆 - 其他资产收益
        leverage = self.allocation['杠杆率']
        equity_return = (self.fund_return * leverage -
                         self.bond_return_ -
                         self.cb_return_ -
                         self.cash_return_)

        return equity_return.rename(f'{self.fund_code}_股票收益')


def batch_strip_returns(fund_codes: list[str],
                        begin_date: str,
                        end_date: str) -> dict:
    """批量剥离多只基金的收益

    Args:
        fund_codes: 基金代码列表
        begin_date: 起始日期
        end_date: 结束日期

    Returns:
        dict: {fund_code: {'equity_return': Series, 'diagnostics': dict}}
              失败的基金值为 None
    """
    # 预加载公共数据
    shared_indices = {
        'bond': load_bond_index_returns(begin_date, end_date)
    }

    results = {}
    for code in fund_codes:
        try:
            stripper = ReturnStripper(code, begin_date, end_date, shared_indices)
            equity_ret = stripper.strip_to_equity()

            results[code] = {
                'equity_return': equity_ret,
                'diagnostics': {
                    'bond_return': stripper.bond_return_,
                    'cb_return': stripper.cb_return_,
                    'cash_return': stripper.cash_return_
                }
            }
        except Exception as e:
            print(f"❌ {code} 失败: {e}")
            results[code] = None

    return results