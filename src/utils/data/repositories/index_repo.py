# src/utils/data/repositories/index_repo.py
"""指数数据查询"""
import pandas as pd
from utils.data import oracle_fetcher


def get_index_nav(index_codes: str | list[str], begin_date: str, end_date: str) -> pd.DataFrame:
    """获取指数净值数据"""
    if isinstance(index_codes, str):
        index_codes = [index_codes]

    sql = """
          SELECT a.INDEXCODE AS 指数代码,
                 a.TDATE     AS 交易日期,
                 a.NEW       AS 收盘价,
                 a.LCLOSE    AS 前收盘价
          FROM TYTFUND.TRAD_ID_DAILY a
                   INNER JOIN TYTFUND.INDEX_BA_INFO b ON a.SECURITYVARIETYCODE = b.SECURITYVARIETYCODE
          WHERE b.INDEXCODE = :index_code
            AND a.TDATE >= TO_DATE(:begin_date, 'YYYY-MM-DD')
            AND a.TDATE <= TO_DATE(:end_date, 'YYYY-MM-DD') \
          """

    results = [
        oracle_fetcher.query(sql, index_code=code, begin_date=begin_date, end_date=end_date)
        for code in index_codes
    ]
    return pd.concat(results, ignore_index=True)