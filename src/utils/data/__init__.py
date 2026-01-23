# src/utils/data/__init__.py
from .oracle import OracleQuery
from .doris import DorisQuery

oracle_fetcher = OracleQuery()
doris_fetcher = DorisQuery()