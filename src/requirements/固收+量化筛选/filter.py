# fund_index_matcher.py
"""基金指数风格匹配分析"""

import pandas as pd
from typing import List, Set, Tuple, Dict
from utils.query_data_funcs import fetcher
from match_config import REPORT_DATES, SCORE_WEIGHTS, TOP_N_MATCHES, INDEX_CONFIGS


class FundIndexMatcher:
    """基金指数匹配分析器"""

    def __init__(self, fund_codes: List[str], index_codes: List[str] = None):
        self.fund_codes = fund_codes
        self.index_codes = index_codes or list(INDEX_CONFIGS.keys())
        self.report_dates = REPORT_DATES

    def get_fund_holdings(self, fund_code: str, end_date: str) -> pd.DataFrame:
        """获取基金前十大持仓"""
        query = """
                SELECT STOCKCODE, PCTNV
                FROM TYTFUND.FUND_IV_STOCKINVESTO
                WHERE FUNDCODE = :fund_code
                  AND ENDDATE = TO_DATE(:end_date, 'YYYY-MM-DD')
                  AND STYLE IN ('01', '02', '03', '04') \
                """
        return fetcher.query_data_tytfund(query, fund_code=fund_code, end_date=end_date)

    def get_index_constituents(self, index_code: str, trade_date: str) -> Set[str]:
        """获取指数成分股"""
        query = """
                SELECT SECURITYCODE
                FROM TYTFUND.IDEX_YS_WEIGHT
                WHERE INDEXCODE = :index_code
                  AND TRADEDATE = TO_DATE(:trade_date, 'YYYY-MM-DD') \
                """
        df = fetcher.query_data_tytfund(query, index_code=index_code, trade_date=trade_date)
        return set(df['SECURITYCODE'].tolist()) if not df.empty else set()

    def calculate_metrics(self, holdings_df: pd.DataFrame, constituents_set: Set[str]) -> Tuple[float, float]:
        """计算单期指标：持仓重合度和市值覆盖率"""
        if holdings_df.empty or not constituents_set:
            return None, None

        matched_stocks = holdings_df[holdings_df['STOCKCODE'].isin(constituents_set)]

        # 持仓重合度
        overlap_ratio = len(matched_stocks) / len(holdings_df)

        # 市值覆盖率（归一化）
        total_pctnv = holdings_df['PCTNV'].sum()
        matched_pctnv = matched_stocks['PCTNV'].sum()
        market_cap_coverage = matched_pctnv / total_pctnv if total_pctnv > 0 else 0

        return overlap_ratio, market_cap_coverage

    def calculate_score(self, coverage: float, overlap: float) -> float:
        """计算综合得分"""
        return (SCORE_WEIGHTS['market_cap_coverage'] * coverage +
                SCORE_WEIGHTS['overlap_ratio'] * overlap)

    def analyze(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """执行分析，返回三个DataFrame"""
        detail_records = []
        agg_records = []

        for fund_code in self.fund_codes:
            for index_code in self.index_codes:
                period_metrics = []

                # 计算4期指标
                for report_date in self.report_dates:
                    holdings = self.get_fund_holdings(fund_code, report_date)
                    constituents = self.get_index_constituents(index_code, report_date)

                    if holdings.empty or not constituents:
                        continue

                    overlap, coverage = self.calculate_metrics(holdings, constituents)
                    if overlap is None:
                        continue

                    # 明细数据
                    detail_records.append({
                        '基金代码': fund_code,
                        '指数代码': index_code,
                        '指数名称': INDEX_CONFIGS.get(index_code, index_code),
                        '报告期': report_date,
                        '市值覆盖率': coverage,
                        '持仓重合度': overlap
                    })

                    period_metrics.append({
                        'coverage': coverage,
                        'overlap': overlap
                    })

                # 聚合数据
                if period_metrics:
                    df_temp = pd.DataFrame(period_metrics)
                    coverage_avg = df_temp['coverage'].mean()
                    overlap_avg = df_temp['overlap'].mean()

                    agg_records.append({
                        '基金代码': fund_code,
                        '指数代码': index_code,
                        '指数名称': INDEX_CONFIGS.get(index_code, index_code),
                        '市值覆盖率_avg': coverage_avg,
                        '市值覆盖率_std': df_temp['coverage'].std(),
                        '持仓重合度_avg': overlap_avg,
                        '持仓重合度_std': df_temp['overlap'].std(),
                        '综合得分': self.calculate_score(coverage_avg, overlap_avg)
                    })

        df_detail = pd.DataFrame(detail_records)
        df_agg = pd.DataFrame(agg_records)
        df_best_match = self._generate_best_match(df_agg)

        return df_detail, df_agg, df_best_match

    def _generate_best_match(self, df_agg: pd.DataFrame) -> pd.DataFrame:
        """生成最佳匹配结果"""
        best_match_records = []

        for fund_code in self.fund_codes:
            fund_matches = df_agg[df_agg['基金代码'] == fund_code].nlargest(TOP_N_MATCHES, '综合得分')

            if len(fund_matches) > 0:
                record = {'基金代码': fund_code}
                for i, row in enumerate(fund_matches.itertuples(), 1):
                    record[f'Top{i}指数代码'] = row.指数代码
                    record[f'Top{i}指数名称'] = row.指数名称
                    record[f'Top{i}得分'] = row.综合得分
                best_match_records.append(record)

        return pd.DataFrame(best_match_records)


if __name__ == '__main__':
    # 配置基金列表
    fund_info = pd.read_excel(r'./基金经理筛选.xlsx', sheet_name='3.匹配成功固收+名单')

    fund_list = [fund_code[:6] for fund_code in fund_info['证券代码']]

    # 创建匹配器
    matcher = FundIndexMatcher(fund_codes=fund_list)

    # 执行分析
    print("开始分析...")
    df_detail, df_agg, df_best_match = matcher.analyze()

    # 展示结果
    print("\n" + "=" * 80)
    print("【明细数据】")
    print(df_detail.to_string(index=False))

    print("\n" + "=" * 80)
    print("【聚合数据】")
    print(df_agg.round(4).to_string(index=False))

    print("\n" + "=" * 80)
    print("【最佳匹配】")
    print(df_best_match.round(4).to_string(index=False))

    # 可选：保存到Excel
    with pd.ExcelWriter('基金指数匹配分析.xlsx') as writer:
        df_detail.to_excel(writer, sheet_name='明细', index=False)
        df_agg.to_excel(writer, sheet_name='聚合', index=False)
        df_best_match.to_excel(writer, sheet_name='最佳匹配', index=False)
