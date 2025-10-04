import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime


def init_logger(name='logger', file_log=True, single_date=True, rotate=False, rotate_max_byte_size=100000000, rotate_backup_count=1, log_dir_path=None):
    """
    Function performs logging customization
    Handlers:
    1) Stream - prints outputs into cmd shell
    2) File - save outputs into file with full datetime or with single date
    default rotate size is 100MB or 100000000 bytes

    :param name:
    :param file_log:
    :param single_date:
    :return:
    """
    formatter = logging.Formatter(fmt=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s')

    if file_log:
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'logs') if not log_dir_path else log_dir_path
        if not os.path.isdir(log_dir):
            try:
                os.system(f'mkdir {log_dir}')
            except Exception as e:
                print(f'failed to create log directory on a path::{log_dir}::{e}')
                exit()

        if not single_date and rotate:
            fn = f"{name}.log"
        elif single_date and not rotate:
            fn = f"{name}_{str(datetime.now().date())}.log"
        else:
            fn = f"{name}.log"

        log_path = os.path.join(log_dir, fn)

        if rotate:
            handler = RotatingFileHandler(
                log_path,
                mode='a',
                maxBytes=rotate_max_byte_size,
                backupCount=rotate_backup_count,
                encoding=None,
                delay=0
            )
        else:
            handler = logging.FileHandler(log_path)

        handler.setFormatter(formatter)
    else:
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear() if logger.hasHandlers() else None

    logger.addHandler(handler)
    return logger