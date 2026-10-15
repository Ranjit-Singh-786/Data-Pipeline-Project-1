import json
import logging
from logging.handlers import TimedRotatingFileHandler
from pipeline.logging_utils import get_logger, log_step
from pipeline.config import Settings, Config

def test_timed_rotating_file_handler_configuration(tmp_path):
    log_file = tmp_path / "test_pipeline.log"
    logger = get_logger(name="test_logger", log_file=log_file)
    
    file_handlers = [h for h in logger.handlers if isinstance(h, TimedRotatingFileHandler)]
    assert len(file_handlers) == 1
    
    handler = file_handlers[0]
    assert handler.when == "MIDNIGHT"
    assert handler.interval == 86400  # 1 day in seconds for 'midnight'
    assert handler.backupCount == 30
    assert handler.encoding == "utf-8"

def test_log_step_writes_json_to_file(tmp_path):
    log_file = tmp_path / "test_steps.log"
    logger = get_logger(name="test_step_logger", log_file=log_file)
    
    log_step(logger, "test_step", batch=1, rows=100)
    
    for handler in logger.handlers:
        handler.flush()
        
    content = log_file.read_text(encoding="utf-8").strip()
    data = json.loads(content)
    assert data["step"] == "test_step"
    assert data["batch"] == 1
    assert data["rows"] == 100
    assert "timestamp" in data

def test_config_settings_alias():
    assert Config == Settings
    settings = Config()
    assert hasattr(settings, "log_file")
    assert "pipeline.log" in settings.log_file


def test_validate_gcp_config_raises_when_missing():
    import pytest
    settings = Settings(project_id="", bucket="", dry_run=False)
    with pytest.raises(ValueError, match="Missing required GCP configurations"):
        settings.validate_gcp_config()

