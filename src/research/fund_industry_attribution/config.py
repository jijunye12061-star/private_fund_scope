"""
1. config.py - 行业归因分析配置

资产-指数映射表
申万行业列表
WLS 回归参数（窗口期、权重衰减等）
数据频率/日期范围
"""
from dataclasses import dataclass

# 资产类别 → 指数代码映射
ASSET_INDEX_MAP = {
    '利率债': 'CBA05801',  # 中债-国债及政策性银行债财富(总值)
    '信用债': 'CBA02721',  # 中债-信用债总财富(1-3年)
    '非政金债': 'CBA15321',  # 中债-金融机构二级资本债券财富(1-3年)
    'ABS': 'CBA05301',  # 中债-资产支持证券
    '转债': '000832',  # 中证转债
    '货币': 'H11025'  # 货币基金指数
}

# 中信一级行业映射
INDUSTRY_CONFIG = {
    'CI005001': {'name': '石油石化', 'inner_code': '1000414414'},
    'CI005002': {'name': '煤炭', 'inner_code': '1000414415'},
    'CI005003': {'name': '有色金属', 'inner_code': '1000414416'},
    'CI005004': {'name': '电力及公用事业', 'inner_code': '1000414417'},
    'CI005005': {'name': '钢铁', 'inner_code': '1000414418'},
    'CI005006': {'name': '基础化工', 'inner_code': '1000414419'},
    'CI005007': {'name': '建筑', 'inner_code': '1000414420'},
    'CI005008': {'name': '建材', 'inner_code': '1000414421'},
    'CI005009': {'name': '轻工制造', 'inner_code': '1000414422'},
    'CI005010': {'name': '机械', 'inner_code': '1000414423'},
    'CI005011': {'name': '电力设备及新能源', 'inner_code': '1000414424'},
    'CI005012': {'name': '国防军工', 'inner_code': '1000414425'},
    'CI005013': {'name': '汽车', 'inner_code': '1000414426'},
    'CI005014': {'name': '商贸零售', 'inner_code': '1000414427'},
    'CI005015': {'name': '消费者服务', 'inner_code': '1000414428'},
    'CI005016': {'name': '家电', 'inner_code': '1000414429'},
    'CI005017': {'name': '纺织服装', 'inner_code': '1000414430'},
    'CI005018': {'name': '医药', 'inner_code': '1000414431'},
    'CI005019': {'name': '食品饮料', 'inner_code': '1000414432'},
    'CI005020': {'name': '农林牧渔', 'inner_code': '1000414433'},
    'CI005021': {'name': '银行', 'inner_code': '1000414434'},
    'CI005022': {'name': '非银行金融', 'inner_code': '1000414435'},
    'CI005023': {'name': '房地产', 'inner_code': '1000414436'},
    'CI005024': {'name': '交通运输', 'inner_code': '1000414437'},
    'CI005025': {'name': '电子', 'inner_code': '1000414438'},
    'CI005026': {'name': '通信', 'inner_code': '1000414439'},
    'CI005027': {'name': '计算机', 'inner_code': '1000414440'},
    'CI005028': {'name': '传媒', 'inner_code': '1000414441'},
    'CI005029': {'name': '综合', 'inner_code': '1000414442'},
    'CI005030': {'name': '综合金融', 'inner_code': '1002118426'},
}


@dataclass
class WLSConfig:
    """WLS回归参数配置"""
    window_days: int = 60  # 滚动窗口天数
    decay_rate: float = 0.94  # 指数衰减率（None为等权）
    min_samples: int = 30  # 最小样本数
    non_negative: bool = True  # 是否非负约束
    sum_to_one: bool = True  # 是否和为1约束

    def get_weights(self, n: int) -> list[float]:
        """生成时间衰减权重"""
        if self.decay_rate is None:
            return [1.0] * n
        return [self.decay_rate ** i for i in range(n - 1, -1, -1)]


@dataclass
class AttributionConfig:
    """归因分析总配置"""
    wls: WLSConfig = WLSConfig()

    # 资产配置数据向前填充的最大天数
    max_fill_days: int = 120

    # 是否剥离货币收益（小基金可能不需要）
    strip_cash: bool = True