"""Doris数据库查询"""
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from config.settings import settings


class DorisQuery:
    """Doris查询器"""

    def __init__(self):
        self.config = settings.doris
        self.engine = create_engine(
            URL.create(
                "mysql+pymysql",
                username=self.config['username'],
                password=self.config['password'],
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database']
            ),
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800
        )

    def query(self, sql: str, chunksize: int = None, **params) -> pd.DataFrame:
        """执行查询"""
        with self.engine.connect() as conn:
            result = pd.read_sql(text(sql), conn, params=params, chunksize=chunksize)
            return pd.concat(result) if chunksize else result

    def batch_query(self, sql: str, code_list: list[str],
                    batch_size: int = 1000, **params) -> pd.DataFrame:
        """批量查询 - 处理IN子句

        Args:
            sql: SQL语句，IN子句使用 :code_list 占位
            code_list: 代码列表
            batch_size: 每批数量
            **params: 其他参数

        Returns:
            合并后的DataFrame
        """
        results = []
        for i in range(0, len(code_list), batch_size):
            batch = tuple(code_list[i:i + batch_size])
            results.append(self.query(sql, code_list=batch, **params))

        return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

    def execute(self, sql: str) -> None:
        """执行DDL/DML"""
        with self.engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()

    def batch_insert(self, table: str, df: pd.DataFrame, batch_size: int = 8000):
        """批量插入数据"""
        with self.engine.connect() as conn:
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]
                batch.to_sql(table, conn, if_exists='append', index=False)