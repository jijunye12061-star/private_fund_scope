"""
Author: 季俊晔
Create Time: 2024-09-09
该代码主要是筛选出基金的分类信息
"""
import pandas as pd
from utils.dating_funcs import generate_report_dates
import os
from utils.query_data_funcs import fetcher


class FundType:
    def __init__(self, fund_info, fund_assert_allocation, end_date):
        """
        初始化基金信息，根据一定规则对基金进行大类分类
        :param fund_info: 基金信息表,列名为 fund_code, estabdate, maturity_dt, ftype_lv1, ftype_lv2
        :param fund_assert_allocation: 基金资产配置表,列名为 报告日期, 基金代码, 股票投资占比, 可转债投资占比, 权益占比
        :param end_date: 截止日期
        """
        self.equity_funds = []
        self.fi_funds = []
        self.pure_bond_funds = []
        self.fund_info = fund_info
        self.fund_assert_allocation = fund_assert_allocation
        self.end_date = end_date

        rolling_dates = generate_report_dates(end_date, 8)
        self.backtrace_dates = pd.to_datetime(rolling_dates)

        self.fund_assert_allocation = self.fund_assert_allocation[
            (self.fund_assert_allocation['报告日期'] <= self.end_date) &
            (self.fund_assert_allocation['报告日期'] >= self.backtrace_dates[0])]

        # 根据季度报告披露的信息，将报告日期不在季度末的作为转变信息，更新成立日期
        self.renew_estabdate()

        # 仅考虑backtrace_dates中的日期
        self.fund_assert_allocation = self.fund_assert_allocation[
            self.fund_assert_allocation['报告日期'].isin(self.backtrace_dates)]
        # 筛选出符合条件的所有基金，成立满两年且未到期
        initiative_fund_types = ['混合型-灵活', '债券型-混合一级', '混合型-偏股', '债券型-长债', '债券型-混合二级',
                                 '债券型-中短债',
                                 '混合型-平衡', '混合型-偏债', '股票型', '债券型-混合债']
        self.fund_pool = self.fund_info[
            (self.fund_info['estabdate'] <= self.backtrace_dates[0]) &
            (self.fund_info['maturity_dt'] > self.end_date) &
            (self.fund_info['ftype_lv2'].isin(initiative_fund_types))]['fund_code'].tolist()

        self.stock_pct = self.fund_assert_allocation.groupby('基金代码')['股票投资占比'].mean()
        self.convertible_bond_pct = self.fund_assert_allocation.groupby('基金代码')['可转债投资占比'].mean()
        self.equity_pct = self.fund_assert_allocation.groupby('基金代码')['权益占比'].mean()
        self.equity_pct_max = self.fund_assert_allocation.groupby('基金代码')['权益占比'].max()

        # 确定权益基金池
        self.determine_equity_funds()
        self.determine_fi_funds()
        self.determine_pure_bond_funds()
        self.mix_funds = list(set(self.fund_pool) - set(self.equity_funds)
                              - set(self.fi_funds) - set(self.pure_bond_funds))

    def renew_estabdate(self):
        """
        根据季度报告信息，更新基金的成立日期
        """
        abnormal_info = self.fund_assert_allocation[~self.fund_assert_allocation['报告日期'].isin(self.backtrace_dates)]
        # 将列名统一以便合并
        # 先将链式操作分开
        abnormal_info_copy = abnormal_info.copy()  # 确保我们操作的是 DataFrame 的副本
        abnormal_info_copy.rename(columns={'报告日期': 'estabdate', '基金代码': 'fund_code'}, inplace=True)

        # 用 'fund_code' 作为键进行合并
        merged_info = pd.merge(self.fund_info, abnormal_info_copy, on='fund_code', how='left', suffixes=('', '_update'))

        # 使用 update 方法更新 'estabdate' 列
        self.fund_info['estabdate'] = self.fund_info['estabdate'].combine_first(merged_info['estabdate_update'])

    def determine_equity_funds(self):
        """
        确定权益基金池
        :return:
        """
        equity_funds1 = self.fund_info[
            (self.fund_info['fund_code'].isin(self.fund_pool)) &
            (self.fund_info['ftype_lv2'] == '股票型')]['fund_code'].tolist()

        # 筛选出混合基金中，考察期内股票均值不低于70%的基金
        prefer_equity = self.stock_pct[self.stock_pct >= 70].index.tolist()
        equity_funds2 = (
            self.fund_info[(self.fund_info['fund_code'].isin(self.fund_pool))
                           & (self.fund_info['ftype_lv2'].isin(['混合型-灵活', '混合型-偏股', '混合型-平衡'])) &
                           (self.fund_info['fund_code'].isin(prefer_equity))]['fund_code'].tolist())

        self.equity_funds = equity_funds1 + equity_funds2

    def determine_fi_funds(self):
        """
        确定固定收益基金池
        :return:
        """
        # 对转债仓位限制排除转债基金
        # 排除转债仓位均值超过60%的基金
        prefer_convertible_bond = self.convertible_bond_pct[self.convertible_bond_pct <= 120].index.tolist()
        fi_funds_pool1 = (
            self.fund_info[(self.fund_info['fund_code'].isin(self.fund_pool)) &
                           (self.fund_info['ftype_lv2'].isin(['债券型-混合一级', '债券型-混合二级', '债券型-混合债'])) &
                           (self.fund_info['fund_code'].isin(prefer_convertible_bond))]['fund_code'].tolist())

        # 混合指数中，如果权益仓位均值不超过30%，或者最大值不超过40%，或者转债仓位高于股票，则认为是固收+基金
        prefer_equity_combined = (
                set(self.equity_pct[self.equity_pct <= 30].index) |
                set(self.equity_pct_max[self.equity_pct_max <= 40].index) |
                set(self.convertible_bond_pct[self.convertible_bond_pct >= self.stock_pct].index)
        )
        fi_funds_pool2 = (
            self.fund_info[(self.fund_info['fund_code'].isin(self.fund_pool)) &
                           (self.fund_info['ftype_lv2'].isin(['混合型-灵活', '混合型-偏债', '混合型-平衡'])) &
                           (self.fund_info['fund_code'].isin(prefer_equity_combined))]['fund_code'].tolist())

        self.fi_funds = fi_funds_pool1 + fi_funds_pool2

    def determine_pure_bond_funds(self):
        """
        确定纯债基金池
        :return:
        """
        # 全部的短债和长债基金
        pure_bond_funds_pool = (
            self.fund_info[(self.fund_info['fund_code'].isin(self.fund_pool)) &
                           (self.fund_info['ftype_lv2'].isin(['债券型-长债', '债券型-中短债']))]['fund_code'].tolist())

        self.pure_bond_funds = pure_bond_funds_pool


def get_if_initial(fund_info):
    """
    获取基金是否为初始基金
    :param fund_info: 基金信息表
    :return: new_fund_info
    """
    fund_codes = fund_info['fund_code'].tolist()
    fund_assert_allocation_query = """
        SELECT
            FUNDCODE AS fund_code,
            CASE
                WHEN CORRECODE_INNER_CODE = SECURITYVARIETYCODE THEN '是'
                ELSE '否'
            END AS 是否初始基金
        FROM TYTFUND.CODE_CD_FUNDCLASS
        WHERE FUNDCODE IN (:code_list)
        """
    result_df = fetcher.query_data_generic(fund_assert_allocation_query, code_list=fund_codes)

    fund_info = pd.merge(fund_info, result_df, on='fund_code', how='left')
    fund_info['是否初始基金'] = fund_info['是否初始基金'].fillna('是')
    return fund_info


def renew_iv_data(fund_info, renew_date):
    """
    更新基金资产配置数据
    :param fund_info:
    :param renew_date:
    :return:
    """
    fund_codes = fund_info['fund_code'].tolist()
    fund_assert_allocation_query = """
        SELECT
            FUNDCODE AS 基金代码,
            ENDDATE AS 报告日期,
            COALESCE(SIPCTNV, 0) AS 股票投资占比,
            COALESCE(BTPCTNV, 0) AS 可转债投资占比,
            COALESCE(SIPCTNV, 0) + COALESCE(BTPCTNV, 0) / 2 AS 权益占比
        FROM TYTFUND.FUND_IV_ASSETALLOCT
        WHERE FUNDCODE IN (:code_list)
        AND STYLE IN ('01', '02', '03', '04')
        AND ENDDATE = TO_DATE(:renew_date, 'YYYY-MM-DD')
    """
    result_df = fetcher.query_data_generic(fund_assert_allocation_query, code_list=fund_codes, renew_date=renew_date)

    return result_df


if __name__ == '__main__':
    # 读取基金信息表
    fund_info_data = pd.read_parquet(r'./data/fund_info.parquet')
    fund_info_data = fund_info_data[fund_info_data['是否初始基金'] == '是']
    fund_info_data = fund_info_data.drop_duplicates(subset='fund_code', keep='first')
    fund_info_data[['estabdate', 'maturity_dt']] = fund_info_data[['estabdate', 'maturity_dt']].apply(pd.to_datetime)
    end_date = '2024-09-30'
    #
    fund_assert_allocation_data = pd.read_parquet(r'./data/fund_iv_asset_allocation.parquet')
    # main_result_df = renew_iv_data(fund_info_data, end_date)
    # fund_assert_allocation_data = pd.concat([fund_assert_allocation_data, main_result_df], axis=0)
    # fund_assert_allocation_data.fillna(0, inplace=True)
    # fund_assert_allocation_data.to_parquet(r'./data/fund_iv_asset_allocation.parquet')
    fund_assert_allocation_data.fillna(0, inplace=True)

    some_test_dates = generate_report_dates(end_date, 22)
    some_test_dates = pd.to_datetime(some_test_dates)
    extra_fund_info = fund_assert_allocation_data[~fund_assert_allocation_data['报告日期'].isin(some_test_dates)]

    fund_type = FundType(fund_info_data, fund_assert_allocation_data, end_date)
    print(f'权益基金池数量: {len(fund_type.equity_funds)}')
    print(f'固收基金池数量: {len(fund_type.fi_funds)}')
    print(f'纯债基金池数量: {len(fund_type.pure_bond_funds)}')
    print(f'混合基金池数量: {len(fund_type.mix_funds)}')

    # 将结果保存为csv文件
    save_path = f'./fund_result/{end_date}-new'
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    equity_funds_result = pd.Series(fund_type.equity_funds, name='基金代码')
    equity_funds_result.to_csv(os.path.join(save_path, 'equity_funds.csv'), index=False)
    fi_funds_result = pd.Series(fund_type.fi_funds, name='基金代码')
    fi_funds_result.to_csv(os.path.join(save_path, 'fi_funds.csv'), index=False)
    fi_funds_result.to_excel(os.path.join(save_path, 'fi_funds.xlsx'), index=False)
    pure_bond_funds_result = pd.Series(fund_type.pure_bond_funds, name='基金代码')
    pure_bond_funds_result.to_csv(os.path.join(save_path, 'pure_bond_funds.csv'), index=False)
    mix_funds_result = pd.Series(fund_type.mix_funds, name='基金代码')
    mix_funds_result.to_csv(os.path.join(save_path, 'mix_funds.csv'), index=False)

