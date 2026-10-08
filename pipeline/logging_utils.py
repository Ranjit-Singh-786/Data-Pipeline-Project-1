import json
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Union




def get_logger(name: str = "insurance_pipeline", log_file: Optional[Union[str, Path]] = None, log_dir: Optional[Union[str, Path]] = None) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)

    if log_file is not None:
        target_path = Path(log_file)
    elif log_dir is not None:
        target_path = Path(log_dir) / "pipeline.log"
    else:
        target_path = Path("logs") / "pipeline.log"

    target_path.parent.mkdir(parents=True, exist_ok=True)

    file_handler = TimedRotatingFileHandler(
        str(target_path),
        when="midnight",
        interval=1,   # everyday
        backupCount=30, # keep only 30 days logs
        encoding="utf-8"
    )
    file_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(stream_handler)
    return logger

def log_step(logger: logging.Logger, step: str, **details):
    logger.info(json.dumps({"timestamp": datetime.now(timezone.utc).isoformat(), "step": step, **details}, default=str))




