"""Oracle数据库查询"""
import pandas as pd
import oracledb
from config.settings import settings

oracledb.init_oracle_client()


class OracleQuery:
    """Oracle查询器"""

    def __init__(self):
        self.config = settings.oracle

    def _get_conn(self):
        dsn = f"{self.config['host']}:{self.config['port']}/{self.config['service_name']}"
        return oracledb.connect(
            user=self.config['username'],
            password=self.config['password'],
            dsn=dsn
        )

    def query(self, sql: str, **params) -> pd.DataFrame:
        """执行查询"""
        with self._get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, **params)
                columns = [col[0] for col in cursor.description]
                return pd.DataFrame(cursor.fetchall(), columns=columns)

    def batch_query(self, sql: str, code_list: list[str],
                    batch_size: int = 450, **params) -> pd.DataFrame:
        """
        批量查询 - 处理IN子句

        sql示例: "SELECT * FROM table WHERE code IN (:code_list)"
        """
        results = []
        for i in range(0, len(code_list), batch_size):
            batch = code_list[i:i + batch_size]
            # 动态替换绑定变量
            modified_sql = sql.replace(
                "IN (:code_list)",
                f"IN ({','.join([f':{j}' for j in range(len(batch))])})"
            )
            bind_vars = {str(j): code for j, code in enumerate(batch)}
            bind_vars.update(params)
            results.append(self.query(modified_sql, **bind_vars))

        return pd.concat(results, ignore_index=True) if results else pd.DataFrame()