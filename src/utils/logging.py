"""日志配置工具"""
import logging
from logging.handlers import RotatingFileHandler


def setup_logger(name: str, log_file: str = 'app.log', level: int = logging.INFO) -> logging.Logger:
    """配置带文件输出的logger"""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # 控制台
    logger.addHandler(logging.StreamHandler())
    # 文件（10MB轮转）
    logger.addHandler(RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5))

    for h in logger.handlers:
        h.setFormatter(formatter)

    return logger