import logging


class LoggerUtil:
    @staticmethod
    def init_logger(log: str) -> logging.Logger:
        """
        Initialize logging with the specified log level.

        Parameters:
            log (str): The log level as a string (e.g., 'DEBUG', 'INFO').
        """
        log_level = log.upper()
        numeric_level = getattr(logging, log_level, None)
        if not isinstance(numeric_level, int):
            raise ValueError(f"Invalid log level: {log}")
        logging.basicConfig(
            level=numeric_level, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        logging.debug(f"Logging initialized at {log_level} level.")
        return logging.getLogger("main")
