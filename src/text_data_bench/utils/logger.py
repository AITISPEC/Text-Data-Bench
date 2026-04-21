import logging
from pathlib import Path

def setup_logger(name: str = "text_data_bench", level: str = "INFO", json_format: bool = False) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            if not json_format
            else '{"time":"%(asctime)s","logger":"%(name)s","level":"%(levelname)s","message":"%(message)s"}'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger