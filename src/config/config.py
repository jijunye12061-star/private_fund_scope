import logging
from logging.handlers import RotatingFileHandler


# 设置变量区域
START_DATE = '2022-01-01'
END_DATE = '2024-12-31'

# 变量设置完成




def setup_logger(name: str = None,
                 level: int = logging.INFO,
                 log_file: str = 'app.log',
                 if_console: bool = True,
                 if_file: bool = True) -> logging.Logger:
    logger = logging.getLogger(name or __name__)
    logger.propagate = False  # 添加这行
    # 清除已存在的处理器
    if logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if if_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    if if_file:
        file_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
