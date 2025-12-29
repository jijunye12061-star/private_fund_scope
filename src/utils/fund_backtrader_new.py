import pandas as pd
from utils.query_data_funcs import fetcher
import numpy as np
from typing import Optional, Dict, List
from datetime import datetime
import logging
from config.config import setup_logger


# 基金组合回测基类
class BasePortfolioBacktest:
    """基金组合回测的基类，包含基本的数据处理和回测功能"""

    def __init__(self, fund_codes: List[str], start_date: str, end_date: str,
                 benchmark_index: Optional[str] = None, nav_type: str = 'adj', **kwargs):
        """初始化基类"""
        self.handled_idx = []
        self.fund_codes = fund_codes
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.benchmark_index = benchmark_index or '809007.EI'
        self._initialize_data(nav_type)
        self._initialize_portfolio()

        kwargs.update({'name': self.__class__.__name__})
        self.logger = setup_logger(**kwargs)

    def _initialize_data(self, nav_type):
        """初始化基金数据"""
        assert nav_type in ['adj', 'acc'], "nav_type must be one of 'adj', 'raw', 'acc'"
        self._query_data(nav_type)
        self._process_nav_data()

    def _initialize_portfolio(self):
        """初始化组合数据结构"""
        self.units_df = pd.DataFrame(0.0, index=self.trade_dates, columns=self.fund_codes)  # 份额矩阵
        self.nav_df = pd.DataFrame(index=self.trade_dates, columns=self.fund_codes)  # 净值矩阵
        self.portfolio_series = pd.Series(index=self.trade_dates)  # 组合单位净值序列
        self.costs_df = pd.DataFrame(0.0, index=self.trade_dates, columns=self.fund_codes)  # 成本矩阵

        self._initialize_current_state()
        self._initialize_trade_records()

    def _initialize_current_state(self):
        """初始化当前状态"""
        self.nowadays = self.start_date
        self.current_units = 0.0  # 当前投资组合的份额
        self.current_nav = 0.0  # 当前投资组合的总净值
        self.current_unit_nav = 1.0  # 当前单位净值
        self.current_costs = 0.0  # 当前买入基金的总成本
        self.units_series = self.units_df.loc[self.start_date].copy()  # 当前份额序列
        self.costs_series = self.costs_df.loc[self.start_date].copy()  # 当前成本序列

    def _initialize_trade_records(self):
        """初始化交易记录"""
        self.order_book = {}
        self.handled_idx = []
        self.confirm_orders = {}
        self.trade_info = None

    def _query_data(self, nav_type):
        """查询基金和基准数据"""
        start_date = self.start_date.strftime('%Y-%m-%d')
        end_date = self.end_date.strftime('%Y-%m-%d')
        self.trade_dates = fetcher.get_trading_dt(start_date, end_date)
        nav_data = fetcher.query_fund_nav(self.fund_codes, start_date, end_date)
        nav_data_clean = nav_data.drop_duplicates(
            subset=['交易日期', '基金代码'],
            keep='last'  # 保留最后一条，也可以用 'first' 保留第一条
        )
        self.nav_data = nav_data_clean.rename(columns={'复权净值': '单位净值'})
        index_nav_data = fetcher.query_index_nav(self.benchmark_index, start_date, end_date)
        self.index_nav_data = index_nav_data.rename(
            columns={'日期': '交易日期', '收盘价': '指数净值'})

    def _process_nav_data(self):
        """处理净值数据"""
        # 处理基金净值数据
        self.fund_nav_dict = {
            fund_code: self.nav_data[self.nav_data['基金代码'] == fund_code]
            .set_index('交易日期')['单位净值']
            for fund_code in self.fund_codes
        }

        # 创建净值矩阵
        fund_nav_df = self.nav_data.pivot(
            index='交易日期',
            columns='基金代码',
            values='单位净值'
        ).astype(float)
        fund_nav_df.ffill(inplace=True)
        self.fund_nav_df = fund_nav_df

        # 处理基准净值数据
        self.benchmark_nav_series = (
                self.index_nav_data.set_index('交易日期')['指数净值'] /
                self.index_nav_data.set_index('交易日期')['指数净值'].iloc[0]
        )

    def ensure_date_in_nav(self, fund_code, target_date):
        """
        确保指定日期在基金净值序列中，如果不存在则添加（用最新历史数据填充）

        Args:
            fund_code: 基金代码
            target_date: 目标日期

        Returns:
            float: 该日期的净值
        """
        current_series = self.fund_nav_dict[fund_code]

        if target_date in current_series.index:
            return current_series.loc[target_date]

        # 使用 asof 找到最新的历史净值
        latest_nav = current_series.asof(target_date)

        if pd.isna(latest_nav):
            return None  # 没有历史数据

        # 添加新日期
        self.fund_nav_dict[fund_code].loc[target_date] = latest_nav

        # 保持索引有序
        self.fund_nav_dict[fund_code] = self.fund_nav_dict[fund_code].sort_index()

        return latest_nav

    def _adjust_money_fund_nav(self):
        """调整货币基金净值"""
        # 首先获取基金一级分类
        query = """
        select
            Info.FCODE,
            Info.FD_TYPE1_NAME
        from TYTFUND.FUND_JBXX Info
        where Info.FCODE IN (:code_list)
        """
        fund_info = fetcher.query_data_generic(query, code_list=self.fund_codes)
        money_fund_codes = fund_info[fund_info['FD_TYPE1_NAME'] == '货币市场基金'].index.tolist()

        # 找到交易信息中赎回的货币基金交易记录，并找到隔了一个周末的情况
        redemption_trades = self.trade_info[(self.trade_info['交易类型'] == '赎回')
                                            & (self.trade_info['基金代码'].isin(money_fund_codes))].copy()
        abnormal_trades = redemption_trades[
            redemption_trades['确认日期'].dt.date - redemption_trades['交易日期'].dt.date > pd.Timedelta(days=1)]

        for idx, row in abnormal_trades.iterrows():
            fund_code = row['基金代码']
            raw_date = row['交易日期']
            confirm_date = row['确认日期']
            new_date = (confirm_date - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            new_nav = fetcher.get_target_fund_nav(fund_code, new_date)
            self.fund_nav_dict[fund_code].loc[raw_date] = new_nav

    def _process_daily_trades(self, trades):
        """将交易信息添加到订单簿中"""
        if trades.empty:
            return

        new_idx = max(self.order_book.keys()) + 1 if self.order_book else 0
        for _, order in trades.iterrows():
            if order['交易类型'] == '赎回':
                # 如果没有真实赎回份额，则计算赎回份额
                nav = self.ensure_date_in_nav(order['基金代码'], order['交易日期'])
                units = (order['交易金额'] + order['固定费用']) / nav / (1 - order['手续费'] / 100)
                amount = None
            elif order['交易类型'] == '申购':
                amount = order['交易金额']
                units = None
            else:
                raise ValueError("交易类型必须是 '申购' 或 '赎回'")
            self.order_book[new_idx] = {
                '基金代码': order['基金代码'],
                '交易金额': order.get('交易金额') if order.get('交易金额') is not None else amount,
                '固定费用': order['固定费用'],
                '手续费': order['手续费'],
                '交易类型': order['交易类型'],
                '确认日期': order['确认日期'],
                '份额': order.get('份额') if order.get('份额') is not None else units
            }
            new_idx += 1

    def _update_portfolio_status(self, date: pd.Timestamp):
        """更新组合状态"""
        self._update_confirmed_orders(date)
        self.units_series = self.units_series.where(np.abs(self.units_series) >= 1e-4, 0.0)
        nav_series = self.units_series * self.fund_nav_df.loc[date]
        self.current_nav = nav_series.sum()
        if self.current_units > 1e-4:
            self.current_unit_nav = self.current_nav / self.current_units

    def _update_portfolio_records(self, date: pd.Timestamp):
        """更新组合记录"""
        self.units_df.loc[date] = self.units_series
        self.nav_df.loc[date] = self.units_series * self.fund_nav_df.loc[date]
        self.current_nav = self.nav_df.loc[date].sum()
        self.portfolio_series[date] = self.current_unit_nav
        self.costs_df.loc[date] = self.costs_series
        self.current_costs = self.costs_series.sum()

    def handle_order(self):
        """处理订单"""
        self.handled_idx = []
        for idx, order in self.order_book.items():
            self.process_trade(idx, order)
        for idx in self.handled_idx:
            self.order_book.pop(idx)

    def process_trade(self, idx, order):
        """处理交易请求"""
        fund_code = order['基金代码']
        amount = order['交易金额']
        flat_fee = order['固定费用']
        percentage_fee = order['手续费']
        trade_type = order['交易类型']
        confirm_date = order['确认日期']
        date = self.nowadays

        # 判断交易是否可以进行
        if date not in self.fund_nav_dict[fund_code].index:
            self.logger.warning(f"基金{fund_code}在{date.strftime('%Y-%m-%d')}没有净值数据,交易取消")
            return
        nav = self.fund_nav_dict[fund_code].loc[date]
        if nav is None:
            self.logger.warning(f"基金{fund_code}在{date.strftime('%Y-%m-%d')}没有净值数据,交易取消")
            return

        if trade_type == '申购':
            # 金额申购，计算申购份额
            units = (amount - flat_fee) / nav / (1 + percentage_fee / 100)
            delta_portfolio_units = amount / self.current_unit_nav
        elif trade_type == '赎回':
            # 份额赎回，计算赎回金额
            # 计算赎回份额, amount为真实赎回金额（扣费后）
            units = order['份额']
            delta_portfolio_units = units * nav / self.current_unit_nav
            amount = units * nav * (1 - percentage_fee / 100) - flat_fee if pd.isna(amount) else amount
            self.redemption_error(units, fund_code)
        else:
            raise ValueError("交易类型必须是 '申购' 或 '赎回'")

        self._record_order(idx, fund_code, amount, trade_type,
                           confirm_date, units, delta_portfolio_units)

    def redemption_error(self, units, fund_code):
        if units > self.units_series[fund_code] + 1000:
            self.logger.error(
                f"赎回份额不足 - 基金:{fund_code}, "
                f"当前份额:{self.units_series[fund_code]:.2f}, "
                f"赎回份额:{units:.2f}"
            )
            raise ValueError("赎回份额超过持有份额")
        elif units > self.units_series[fund_code] + 10:
            self.logger.warning(
                f"赎回份额不足 - 基金:{fund_code}, "
                f"当前份额:{self.units_series[fund_code]:.2f}, "
                f"赎回份额:{units:.2f}"
            )
            self.units_series[fund_code] = units
        elif abs(units - self.units_series[fund_code]) <= 10:
            # 认为是精度误差，忽略之
            self.units_series[fund_code] = units

    def _update_confirmed_orders(self, date: pd.Timestamp):
        """更新确认订单"""
        date_key = date.date()
        if date_key not in self.confirm_orders:
            return

        for order in self.confirm_orders[date_key]:
            self._apply_order(order)
        # 删除已处理的订单
        self.confirm_orders.pop(date_key, None)

    def _record_order(self, idx: int, fund_code: str, amount: float,
                      trade_type: str, confirm_date: pd.Timestamp,
                      units: float, delta_portfolio_units: float):
        """记录订单"""
        if self.nowadays > confirm_date:
            # 以排除认购期的情况
            confirm_date = self.nowadays
        date_key = confirm_date.date()
        if date_key not in self.confirm_orders:
            self.confirm_orders[date_key] = []

        self.confirm_orders[date_key].append({
            '基金代码': fund_code,
            '交易金额': amount,
            '交易类型': trade_type,
            '确认日期': confirm_date,
            '份额': units,
            '组合份额': delta_portfolio_units
        })

        self.handled_idx.append(idx)
        self.logger.info(
            f"交易成功 - 基金:{fund_code}, 类型:{trade_type}, "
            f"金额:{amount:.2f}, 份额:{units:.2f},"
            f"此时持仓:{self.units_series[fund_code]:.2f}"
        )

    def _apply_order(self, order: Dict):
        """应用订单"""
        fund_code = order['基金代码']
        amount = order['交易金额']
        units = order['份额']
        delta_portfolio_units = order['组合份额']

        if order['交易类型'] == '申购':
            self.units_series[fund_code] += units
            self.costs_series[fund_code] += amount
            self.current_units += delta_portfolio_units
        elif order['交易类型'] == '赎回':
            self.units_series[fund_code] -= units
            self.costs_series[fund_code] -= amount
            self.current_units -= delta_portfolio_units
        else:
            raise ValueError("交易类型必须是 '申购' 或 '赎回'")

    def backtest(self, trade_info: pd.DataFrame, **kwargs):
        """执行回测"""
        raise NotImplementedError("backtest method must be implemented in subclass")


class SubscriptionRedemptionBacktest(BasePortfolioBacktest):
    """基于申购赎回信息簿的回测类"""

    def __init__(self, fund_codes: List[str], start_date: str, end_date: str, benchmark_index: Optional[str] = None,
                 nav_type: str = 'adj', **kwargs):
        """初始化基于申购赎回的回测类"""
        super().__init__(fund_codes, start_date, end_date, benchmark_index, nav_type, **kwargs)
        self.nowadays = None
        self.trade_info = None

    def backtest(self, trade_info, **kwargs):
        """
        执行基于申购赎回的回测
        trade_info: DataFrame，申购赎回信息，包括基金代码，交易日期，交易金额，固定费用，手续费（百分比），交易类型，确认日期
        """
        trade_info[['交易日期', '确认日期']] = trade_info[['交易日期', '确认日期']].apply(pd.to_datetime,
                                                                                          format='%Y-%m-%d',
                                                                                          errors='coerce')
        self.trade_info = trade_info

        self._adjust_money_fund_nav()
        self.logger.info("开始回测")
        for date in self.trade_dates:
            self.nowadays = date
            self._update_portfolio_status(date)

            daily_trades = trade_info[trade_info['交易日期'] == date].copy()
            self._process_daily_trades(daily_trades)
            if self.order_book:
                self.handle_order()
            self._update_confirmed_orders(date)

            # 最后更新组合净值序列
            self._update_portfolio_records(date)


class WeightBasedBacktest(BasePortfolioBacktest):
    """基于权重 的 回测类"""

    def __init__(self, fund_codes: List[str], start_date: str, end_date: str,
                 benchmark_index: Optional[str] = None, nav_type: str = 'adj',
                 flat_fee: float = 0.0, percentage_fee: float = 0.0,
                 **kwargs):
        """初始化权重回测类"""
        super().__init__(fund_codes, start_date, end_date, benchmark_index, nav_type, **kwargs)
        self.nowadays = None
        self.trade_instructions = pd.DataFrame(
            columns=['基金代码', '交易金额', '交易类型', '交易日期', '确认日期']
        )
        self.trade_info_weights = None  # 存储交易权重信息
        self.trade_info = None  # 存储交易信息
        self.last_trade_date = None
        self.flat_fee = flat_fee
        self.percentage_fee = percentage_fee

    def backtest(self, trade_info: pd.DataFrame, redemption_type: str = 'yesterday',
                 initial_navs: float = 1e9, **kwargs):
        """执行基于权重 回测
        trade_info:  DataFrame，持仓权重信息，包括基金代码，交易日期，持仓权重
        """
        # 赎回类型, 'today' 表示当日赎回，'yesterday' 表示次日赎回
        assert redemption_type in ['today',
                                   'yesterday'], f"赎回类型必须是 'today' 或 'yesterday', got {redemption_type}"

        trade_info['交易日期'] = pd.to_datetime(trade_info['交易日期'], errors='coerce')
        self.last_trade_date = trade_info['交易日期'].max()
        self.trade_info_weights = trade_info

        self.logger.info("开始回测")
        for date in self.trade_dates:
            self.nowadays = date
            self._update_portfolio_status(date)
            if date == trade_info['交易日期'].min():
                trades = self._generate_initial_trades(trade_info, date, initial_navs)
            else:
                trades = self.generate_trades_based_weights(trade_info, date, redemption_type)

            if not trades.empty:
                trades['固定费用'] = kwargs.get('flat_fee', 0)
                trades['手续费'] = kwargs.get('percentage_fee', 0.0)
                if self.trade_info is None:
                    self.trade_info = trades.astype({
                        '交易金额': 'float64',
                        '份额': 'float64'
                    })
                else:
                    self.trade_info = pd.concat([
                        self.trade_info,
                        trades.astype({
                            '交易金额': 'float64',
                            '份额': 'float64'
                        })
                    ], axis=0, ignore_index=True)
            trades = self.trade_info[self.trade_info['交易日期'] == date].copy()
            self._process_daily_trades(trades)

            if self.order_book:
                self.handle_order()
            self._update_confirmed_orders(date)

            self._update_portfolio_records(date)

    def generate_trades_based_weights(self, trade_info: pd.DataFrame,
                                      date: pd.Timestamp,
                                      redemption_type: str) -> pd.DataFrame:
        """生成交易指令"""
        assert redemption_type in ['yesterday',
                                   'today'], f"redemption_type must be 'yesterday' or 'today', got {redemption_type}"
        # 预先定义空DataFrame避免重复创建
        TRADE_COLUMNS = ['基金代码', '交易金额', '交易类型', '交易日期', '确认日期', '份额']
        trades = pd.DataFrame(columns=TRADE_COLUMNS)

        # 提前验证日期条件
        if (redemption_type == 'yesterday' and date >= self.last_trade_date) or \
                (redemption_type == 'today' and date > self.last_trade_date):
            return trades

        # 获取目标持仓
        if redemption_type == 'yesterday':
            switch_date = self.trade_dates[self.trade_dates.get_loc(date) + 1]
            if switch_date not in trade_info['交易日期'].unique():
                return trades
            target_date = switch_date
            confirm_date = self.trade_dates[self.trade_dates.get_loc(date) + 2]
        else:
            if date not in trade_info['交易日期'].unique():
                return trades
            target_date = date
            confirm_date = self.trade_dates[self.trade_dates.get_loc(date) + 1]

        # 使用loc替代boolean indexing
        target_position = trade_info.loc[trade_info['交易日期'] == target_date].set_index('基金代码')['持仓权重']

        # 合并生成交易指令
        both_trades = [df for df in [
            self.generate_redemptions(date, target_date, target_position),
            self.generate_subscriptions(target_date, confirm_date, target_position)
        ] if not df.empty]

        return pd.concat(both_trades) if both_trades else trades

    def generate_redemptions(self, date: pd.Timestamp,
                             confirm_date: pd.Timestamp,
                             target_position: pd.Series) -> pd.DataFrame:
        """生成赎回交易"""
        # 缓存计算结果
        nav_date = self.fund_nav_df.loc[date]
        target_position = target_position.reindex(self.fund_nav_df.columns).fillna(0.0)

        # 向量化计算
        redemption_units = (self.units_series -
                            (self.current_nav * target_position) / nav_date)

        # 使用query替代boolean indexing
        redemption_df = redemption_units[redemption_units > 0].to_frame('份额').reset_index()

        return redemption_df.assign(
            交易类型='赎回',
            交易日期=date,
            确认日期=confirm_date,
            交易金额=pd.Series(dtype='float64'),
            份额=lambda x: x['份额'].astype('float64')
        ).rename(columns={'index': '基金代码'})

    def generate_subscriptions(self, date: pd.Timestamp,
                               confirm_date: pd.Timestamp,
                               target_position: pd.Series) -> pd.DataFrame:
        """生成申购交易"""
        nav_date = self.fund_nav_df.loc[date]
        target_position = target_position.reindex(self.fund_nav_df.columns).fillna(0.0)

        subscription_nav = (self.current_nav * target_position -
                            self.units_series * nav_date.fillna(1.0))

        subscription_df = subscription_nav[subscription_nav > 0].to_frame('交易金额').reset_index()

        return subscription_df.assign(
            交易类型='申购',
            交易日期=date,
            确认日期=confirm_date,
            份额=pd.Series(dtype='float64'),
            交易金额=lambda x: x['交易金额'].astype('float64')
        ).rename(columns={'index': '基金代码'})

    def _generate_initial_trades(self, trade_info: pd.DataFrame,
                                 date: pd.Timestamp, initial_navs: float) -> pd.DataFrame:
        """生成初始建仓交易"""
        target_position = trade_info.loc[trade_info['交易日期'] == date].set_index('基金代码')['持仓权重']
        if target_position.empty:
            return pd.DataFrame()

        confirm_date = self.trade_dates[self.trade_dates.get_loc(date) + 1]
        self.current_nav = initial_navs
        trades = self.generate_subscriptions(date, confirm_date, target_position)

        return trades


class PortfolioEvaluator:
    """组合评价类，负责计算各类评价指标"""

    def __init__(self, portfolio_backtest):
        self.backtest = portfolio_backtest
        self.portfolio_series = portfolio_backtest.portfolio_series
        self.nav_df = portfolio_backtest.nav_df
        self.benchmark_nav_series = portfolio_backtest.benchmark_nav_series
        self.trade_info = portfolio_backtest.trade_info
        self.costs_df = portfolio_backtest.costs_df

    def calculate_performance(self,
                              date: Optional[datetime] = None,
                              metric_type: str = 'both',
                              periods: Optional[Dict] = None,
                              start_year: Optional[int] = None) -> Dict:
        """
        Calculate both rolling and yearly metrics for the portfolio
        Args:
            date: Reference date for rolling metrics
            metric_type: 'rolling', 'yearly', or 'both'
            periods: Custom periods for rolling metrics
            start_year: Starting year for yearly metrics
        """
        date = date or self.portfolio_series.index.max()
        results = {}

        if metric_type in ['rolling', 'both']:
            results['rolling'] = self._calculate_rolling_metrics(date, periods)

        if metric_type in ['yearly', 'both']:
            results['yearly'] = self._calculate_yearly_metrics(start_year)

        return results

    def _calculate_period_metrics(self, period_dates) -> Optional[Dict]:
        """Calculate metrics for a specific period"""
        if len(period_dates) <= 1:
            return None

        period_nav = self.portfolio_series[self.portfolio_series.index.isin(period_dates)]
        period_benchmark = self.benchmark_nav_series[self.benchmark_nav_series.index.isin(period_dates)]
        period_fund_nav = self.nav_df.loc[period_dates]
        period_portfolio = period_fund_nav.sum(axis=1)
        period_fund_costs = self.costs_df.loc[period_dates]
        period_costs = period_fund_costs.sum(axis=1)

        # Returns
        total_return = (period_nav.iloc[-1] / period_nav.iloc[0] - 1) * 100
        bm_return = (period_benchmark.iloc[-1] / period_benchmark.iloc[0] - 1) * 100

        # Drawdown analysis
        rolling_max = period_nav.expanding().max()
        drawdown = ((period_nav - rolling_max) / rolling_max) * 100
        max_dd = drawdown.min()
        max_dd_end = drawdown.idxmin()
        max_dd_start = period_nav[:max_dd_end].idxmax()
        max_dd_days = self.nav_df.index.get_loc(max_dd_end) - self.nav_df.index.get_loc(max_dd_start)

        # Upside analysis
        returns = period_nav / period_nav.iloc[0] - 1
        max_up = returns.max() * 100
        max_up_end = returns.idxmax()
        max_up_start = period_nav[:max_up_end].idxmin()
        max_up_days = self.nav_df.index.get_loc(max_up_end) - self.nav_df.index.get_loc(max_up_start)

        # Profit/Loss
        profit_loss = period_portfolio.iloc[-1] - period_portfolio.iloc[0]
        profit_loss -= period_costs.iloc[-1] - period_costs.iloc[0]

        return {
            '收益率': round(total_return, 2),
            '基准收益率': round(bm_return, 2),
            '最大涨幅': round(max_up, 2),
            '最大涨幅天数': max_up_days,
            '最大涨幅起点': max_up_start.strftime('%Y/%m/%d'),
            '最大涨幅终点': max_up_end.strftime('%Y/%m/%d'),
            '最大回撤': round(max_dd, 2),
            '最大回撤天数': max_dd_days,
            '最大回撤起点': max_dd_start.strftime('%Y/%m/%d'),
            '最大回撤终点': max_dd_end.strftime('%Y/%m/%d'),
            '区间损益': round(profit_loss, 2)
        }

    def _calculate_rolling_metrics(self, date: datetime, periods: Optional[List[str]] = None) -> Dict:
        """Calculate metrics for rolling periods (1M, 3M, 6M, 1Y, inception)"""
        nav_series = self.backtest.portfolio_series[:date].dropna()

        default_periods = {
            '近一月': pd.DateOffset(months=1),
            '近三月': pd.DateOffset(months=3),
            '近六月': pd.DateOffset(months=6),
            '近一年': pd.DateOffset(years=1),
            '创建以来': None
        }

        periods = periods or default_periods
        metrics = {}

        for period_name, offset in periods.items():
            if offset:
                period_dates = nav_series[(nav_series.index >= date - offset) &
                                          (nav_series.index <= date)].index
            else:
                period_dates = nav_series[nav_series.index <= date].index

            metrics[period_name] = self._calculate_period_metrics(period_dates)

        return metrics

    def _calculate_yearly_metrics(self, start_year: Optional[int] = None) -> Dict:
        """Calculate metrics for each calendar year"""
        if not start_year:
            start_year = self.portfolio_series.index.min().year

        current_year = self.portfolio_series.index.max().year
        yearly_metrics = {}

        for year in range(start_year, current_year + 1):
            year_dates = self.portfolio_series[self.portfolio_series.index.year == year].index
            metrics = self._calculate_period_metrics(year_dates)
            if metrics:
                yearly_metrics[str(year)] = metrics

        return yearly_metrics

    def calculate_monthly_returns(self, date=None):
        """计算月度收益"""
        date = date or self.backtest.end_date
        nav_series = self.portfolio_series[:date].dropna()
        benchmark_series = self.benchmark_nav_series.loc[nav_series.index]

        # 计算月度收益
        returns = self._calculate_returns(nav_series, benchmark_series, 'ME')
        returns.index = returns.index.strftime('%Y-%m')

        return returns

    @staticmethod
    def _calculate_returns(nav_series, benchmark_series, freq='ME'):
        """计算不同频率的收益率

        Args:
            nav_series: 净值序列
            benchmark_series: 基准序列
            freq: 频率，可选 'W'(周)/'ME'(月)/'QE'(季)

        Returns:
            包含组合收益、基准收益和超额收益的DataFrame
        """
        # 转换为对应频率的数据
        period_nav = nav_series.resample(freq).last().astype(float)
        period_bench = benchmark_series.resample(freq).last().astype(float)

        # 获取起始值
        nav_start = nav_series.iloc[0]
        bench_start = benchmark_series.iloc[0]

        # 计算收益率
        nav_returns = period_nav.pct_change() * 100
        bench_returns = period_bench.pct_change() * 100

        # 修正第一期的收益率
        nav_returns.iloc[0] = (period_nav.iloc[0] / nav_start - 1) * 100
        bench_returns.iloc[0] = (period_bench.iloc[0] / bench_start - 1) * 100

        # 计算超额收益
        excess_returns = nav_returns - bench_returns

        return pd.DataFrame({
            'portfolio_return': nav_returns,
            'benchmark_return': bench_returns,
            'excess_return': excess_returns
        }).round(2)

    def calculate_contributions(self, date=None):
        """计算贡献度分析"""
        date = date or self.backtest.end_date
        portfolio = self.backtest.nav_df.loc[date]
        costs = self.backtest.costs_df.loc[date]

        contributions = pd.DataFrame({
            '基金代码': portfolio.index,
            '成本': costs,
            '市值': portfolio,
            '盈亏': portfolio - costs
        })

        total_nav = portfolio.sum()
        total_pl = contributions['盈亏'].sum()

        contributions['权重'] = contributions['市值'] / total_nav
        contributions['贡献占比'] = contributions['盈亏'] / total_pl

        return contributions.sort_values('贡献占比', ascending=False)


def demo_weight_backtest() -> WeightBasedBacktest:
    """展示基于权重的再平衡回测示例"""
    trade_info = pd.DataFrame({
        '基金代码': ['000001', '000003', '015650', '000759', '000005'],
        '交易日期': ['2024-01-02', '2024-01-02', '2024-06-04', '2024-06-04', '2024-06-04'],
        '持仓权重': [0.5, 0.5, 0.3, 0.3, 0.3]
    })
    start_date = '2024-01-02'
    end_date = '2024-12-31'
    benchmark_index = '000300'
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


def demo_trades_backtest() -> SubscriptionRedemptionBacktest:
    """展示基于交易信息的回测示例"""
    trade_info = pd.DataFrame({
        '基金代码': ['000001.OF', '000003.OF', '000001.OF', '000759.OF', '000005.OF'],
        '交易日期': ['2024-01-02', '2024-01-02', '2024-06-04', '2024-06-04', '2024-06-04'],
        '交易金额': [100000, 100000, 50000, 50000, 50000],
        '固定费用': [0, 0, 0, 0, 0],
        '手续费': [0.0, 0.0, 0.0, 0.0, 0.0],
        '交易类型': ['申购', '申购', '赎回', '申购', '申购'],
        '确认日期': ['2024-01-03', '2024-01-03', '2024-06-05', '2024-06-05', '2024-06-05']
    })
    start_date = '2024-01-02'
    end_date = '2024-12-31'
    benchmark_index = '000300.SH'

    fund_codes = trade_info['基金代码'].unique().tolist()

    config_dict = {'level': logging.WARNING,
                   'log_file': 'backtest.log',
                   'if_console': True,
                   'if_file': True}
    portfolio_manager = SubscriptionRedemptionBacktest(fund_codes, start_date, end_date,
                                                       benchmark_index, **config_dict)

    portfolio_manager.backtest(trade_info)

    return portfolio_manager


def demo_evaluation(portfolio_manager: BasePortfolioBacktest):
    """展示回测结果评价示例"""
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


if __name__ == '__main__':
    # test_trades_portfolio_manager = demo_trades_backtest()
    test_weight_portfolio_manager = demo_weight_backtest()
    # test_results = demo_evaluation(test_weight_portfolio_manager)
    # export_to_excel(test_results, 'backtest_results.xlsx')
