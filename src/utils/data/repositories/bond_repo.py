import pandas as pd
from utils.data import oracle_fetcher


def get_bond_daily_nav(bond_inner_codes: list[str], begin_date: str, end_date: str) -> pd.DataFrame:
    """获取债券日行情数据

    Args:
        bond_inner_codes: 债券内码列表 ['1002020923']
        begin_date: 起始日期 'YYYY-MM-DD'
        end_date: 结束日期 'YYYY-MM-DD'

    Returns:
        DataFrame(交易日期, 债券内码, 前收盘全价, 收盘全价)
    """
    sql = """
          SELECT TDATE               AS 交易日期,
                 SECURITYVARIETYCODE AS 债券内码,
                 LFCLOSE             AS 前收盘全价,
                 FCLOSE              AS 收盘全价
          FROM TYTFUND.BOND_TD_DAILY
          WHERE TDATE BETWEEN TO_DATE(:begin_date, 'YYYY-MM-DD')
              AND TO_DATE(:end_date, 'YYYY-MM-DD')
            AND SECURITYVARIETYCODE IN (:code_list)
          """
    return oracle_fetcher.batch_query(
        sql, bond_inner_codes,
        begin_date=begin_date,
        end_date=end_date
    )


if __name__ == '__main__':
    code_list = ['1002020923']

    bond_nav = get_bond_daily_nav(code_list, begin_date='2024-09-23', end_date='2024-10-23')