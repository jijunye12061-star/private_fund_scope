import pandas as pd
import oracledb

# 初始化Oracle客户端
oracledb.init_oracle_client()

# 数据库连接
dsn = "10.211.80.109:1521/EMBASEGB"
conn = oracledb.connect(
    user='FUNDEMUSER',
    password='Pa8Tud0B',
    dsn=dsn
)

# SQL查询
sql = """
WITH NewStocks2025 AS (
    SELECT n.SECURITYCODE, n.SECURITYNAME, lp.COMPANYCODE, ib.FINANCECODE
    FROM NEWSADMIN.LICO_FI_NEWSTOCKINFO n
    INNER JOIN NEWSADMIN.CDSY_LISTPROCESS lp 
        ON n.SECURITYCODE = lp.SECURITYCODE AND lp.TYPE = '9'
    INNER JOIN NEWSADMIN.CPI_ISSUEBASICINFO ib 
        ON lp.COMPANYCODE = ib.COMPANYCODE
    WHERE n.LISTINGDATE BETWEEN DATE'2025-01-01' AND DATE'2026-01-01'
)
SELECT 
    pr.PLACEOBJECTCODE AS 基金代码,
    pr.PLACEOBJECT AS 基金名称,
    COUNT(*) AS 中签次数,
    SUM(pr.SHAREPLACE) AS 总配售数量,
    SUM(pr.SUMPLACE) AS 总配售金额
FROM NewStocks2025 ns
INNER JOIN NEWSADMIN.CPI_PLACERESULT pr ON ns.FINANCECODE = pr.FINANCECODE
WHERE LENGTH(pr.PLACEOBJECTCODE) = 6
GROUP BY pr.PLACEOBJECTCODE, pr.PLACEOBJECT
ORDER BY 中签次数 DESC
"""

# 执行查询
df = pd.read_sql(sql, conn)
conn.close()

# 查看结果
print(f"查询到 {len(df)} 只基金")
print(df.head(20))

# 保存到Excel
df.to_excel('2025年公募基金打新统计.xlsx', index=False)