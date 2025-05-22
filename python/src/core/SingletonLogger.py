import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime


class SingletonLogger:
    _instances = {}

    log_path = None

    def __new__(cls, log_name="application", level=logging.INFO):
        if log_name not in cls._instances:
            cls._instances[log_name] = super(SingletonLogger, cls).__new__(cls)
            cls._instances[log_name]._initialize_logger(log_name, level)
        return cls._instances[log_name]

    def _initialize_logger(self, log_name, level):
        """
        Initializes the logger instance.
        """
        log_dir = os.path.join(os.environ.get("LOG_PATH", "/tmp"), "logs")

        self.log_path = log_dir
        self.log_name = log_name
        log_file = os.path.join(log_dir, f"{log_name}.log")
        os.makedirs(log_dir, exist_ok=True)

        self.logger = logging.getLogger(log_name)
        self.logger.setLevel(level)

        if not self.logger.hasHandlers():
            file_handler = RotatingFileHandler(
                log_file, maxBytes=5 * 1024 * 1024, backupCount=3
            )
            file_handler.setLevel(level)

            formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            file_handler.setFormatter(formatter)

            self.logger.addHandler(file_handler)

    def force_log(self, result):
        """
        Forcefully logs data to a specified file.
        """
        with open(os.path.join(self.log_path, f"{self.log_name}.raw"), "a") as log_file:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_file.write(f"\n{timestamp}: {result}\n")

    def get_logger(self):
        return self.logger


# Usage example:
# logger = SingletonLogger("my_log").get_logger()
# logger.info("This is a log message.")
