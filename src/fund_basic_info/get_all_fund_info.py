"""
该代码是为了从投研通的底层数据库中获取所有的基金信息
Author: 季俊晔
Create Time: 2024-09-25
"""
import pandas as pd
from utils.query_data_funcs import fetcher


def get_fund_class_level2():
    query = """
    WITH FilteredFund AS (
    SELECT
        A.FUNDCODE,
        A.TYPECODE AS 二级分类代码,
        C.TYPECODE AS 一级分类代码,
        FOUNDFO.FOUNDDATE,
        FOUNDFO.ENDDATE,
        A.SECURITYVARIETYCODE,
        CASE WHEN D.TYPECODE IS NOT NULL THEN '是' ELSE '否' END AS 是否定开
    FROM
        TYTFUND.FUND_BS_ATYPE A
    LEFT JOIN
        TYTFUND.FUND_BS_ATYPE C
        ON A.SECURITYVARIETYCODE = C.SECURITYVARIETYCODE
        AND C.TYPEMETHOD = '101'  -- 一级分类
        AND C.ISUSING = '1'  -- 仅选择启用的数据
    LEFT JOIN 
        TYTFUND.FUND_BS_ATYPE D
        ON A.SECURITYVARIETYCODE = D.SECURITYVARIETYCODE
        AND D.TYPECODE = '105016'  -- 二级分类
        AND D.ISUSING = '1'  -- 仅选择启用的数据
    LEFT JOIN
        TYTFUND.FUND_BS_OFINFO FOUNDFO
        ON A.SECURITYVARIETYCODE = FOUNDFO.SECURITYVARIETYCODE  -- 联接基金信息表
    WHERE
        A.TYPEMETHOD = '102'  -- 二级分类筛选
        AND A.ISUSING = '1'  -- 仅选择启用的数据
)

-- 再进行常数表的联接以获取分类名称
SELECT
    F.FUNDCODE AS 基金代码,
    F.FOUNDDATE AS 成立日期,
    F.ENDDATE AS 截止日期,
    D.PARAMCHNAME AS 一级分类名称,
    B.PARAMCHNAME AS 二级分类名称,
    F.SECURITYVARIETYCODE AS 基金内码,
    F.是否定开
FROM
    FilteredFund F
LEFT JOIN
    TYTFUND.CFP_PVALUE B
    ON F.二级分类代码 = B.PARAMCODE
    AND B.NIPMID = '138000000412793992'  -- 二级分类的名称
LEFT JOIN
    TYTFUND.CFP_PVALUE D
    ON F.一级分类代码 = D.PARAMCODE
    AND D.NIPMID = '138000000412793992'  -- 一级分类的名称
    """
    fund_class_level2 = fetcher.query_data_tytfund(query)
    return fund_class_level2


def judge_if_first(fund_variety_codes):
    """
    判断是否为初始基金
    :param fund_variety_codes: 基金内码列表
    :return: 返回是否为初始基金的df
    """
    query = """
    SELECT 
        FUNDCODE AS 基金代码,
        SECURITYVARIETYCODE AS 基金内码,
        CORRECODE_INNER_CODE AS 主基金内码
    FROM TYTFUND.CODE_CD_FUNDCLASS
    WHERE SECURITYVARIETYCODE IN (:code_list)
    """
    result = fetcher.query_data_generic(query, code_list=fund_variety_codes)
    result['是否初始基金'] = result.apply(lambda x: '是' if x['基金内码'] == x['主基金内码'] else '否', axis=1)
    return result


if __name__ == '__main__':
    main_fund_info = get_fund_class_level2()
    main_fund_variety_codes = main_fund_info['基金内码'].unique().tolist()
    main_fund_first = judge_if_first(main_fund_variety_codes)
    main_fund_info = pd.merge(main_fund_info, main_fund_first[['基金内码', '是否初始基金']], on='基金内码', how='left')
    main_fund_info['是否初始基金'] = main_fund_info['是否初始基金'].fillna('是')
    main_fund_info.to_parquet(r'./data/tytfund_fund_info.parquet', index=False)
    pass

