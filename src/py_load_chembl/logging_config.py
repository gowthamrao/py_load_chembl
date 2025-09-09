import logging
import json
import os
import sys
from typing import Dict, Any

class JsonFormatter(logging.Formatter):
    """
    Formats log records as a JSON string.
    """
    def format(self, record: logging.LogRecord) -> str:
        log_object: Dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "name": record.name,
        }
        if record.exc_info:
            log_object["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_object)

def setup_logging():
    """
    Configures the root logger for the application.
    - Disables existing loggers to prevent duplicate output.
    - Sets the log level based on the LOG_LEVEL environment variable (default: INFO).
    - Adds a handler that directs logs to stdout with a JSON formatter.
    """
    # Disable any existing loggers to avoid conflicts or duplicate messages
    logging.basicConfig(handlers=[], level=logging.NOTSET, force=True)

    log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    # Get the root logger and remove any handlers that may have been configured
    # by other libraries (e.g., pytest).
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    root_logger.setLevel(log_level)

    # Add our custom JSON handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root_logger.addHandler(handler)

    # Suppress verbose logging from third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("psycopg2").setLevel(logging.WARNING)
    logging.getLogger("docker").setLevel(logging.WARNING)

    logging.getLogger(__name__).info(f"Logging configured with level: {log_level_str}")
