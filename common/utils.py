import logging
from connection.mariadb import connection_177_ktspi


def set_logger(func_name, today):
    mylogger = logging.getLogger("my")
    mylogger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    stream_hander = logging.StreamHandler()
    stream_hander.setFormatter(formatter)

    file_handler = logging.FileHandler(
        "/mnt/storage/collection-amazon/logs/{}-{}.log".format(func_name, today),
        encoding="utf-8-sig",
    )
    mylogger.addHandler(file_handler)

    return mylogger
