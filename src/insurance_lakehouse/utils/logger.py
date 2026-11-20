"""
Logging setup.

KYUN: `print()` ki jagah logging - level (INFO/WARN/ERROR), timestamp, module
name milta hai, aur Databricks job logs / Azure Log Analytics me filter ho
sakta hai.
KYA NAHI KARNA: logs me PII (customer naam, zip, policy number ki list) ya
secrets kabhi mat likhna. Logs bahut logon tak pahunchte hain.
"""

import logging
import sys

_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_FORMAT))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        # Parent (root) logger ko propagate mat karo warna har line 2 baar aayegi
        logger.propagate = False
    return logger
