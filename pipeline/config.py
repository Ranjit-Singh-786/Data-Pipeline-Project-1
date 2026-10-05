from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    mysql_host: str = os.getenv("MYSQL_HOST", "127.0.0.1")
    mysql_port: int = int(os.getenv("MYSQL_PORT", "3306"))
    mysql_database: str = os.getenv("MYSQL_DATABASE", "insurance")
    mysql_user: str = os.getenv("MYSQL_USER", "insurance_user")
    mysql_password: str = os.getenv("MYSQL_PASSWORD", "insurance_password")
    source_table: str = os.getenv("MYSQL_SOURCE_TABLE", "insurance")
    project_id: str = os.getenv("GCP_PROJECT_ID", "")
    location: str = os.getenv("GCP_LOCATION", "US")
    dataset: str = os.getenv("GCP_DATASET", "insurance_poc")
    table: str = os.getenv("GCP_TABLE", "insurance_clean")
    # GCP Config
    bucket: str = os.getenv("GCS_BUCKET", "")
    gcs_prefix: str = os.getenv("GCS_PREFIX", "insurance-poc")
    batch_size: int = int(os.getenv("BATCH_SIZE", "250"))
    artifact_dir: str = os.getenv("ARTIFACT_DIR", "artifacts")
    log_dir: str = os.getenv("LOG_DIR", "logs")
    log_file: str = os.getenv("LOG_FILE", os.path.join(os.getenv("LOG_DIR", "logs"), "pipeline.log"))
    dry_run: bool = os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes")

    # Change Action API Source Config
    change_action_api_url: str = os.getenv("CHANGE_ACTION_API_URL", "")
    sample_json_path: str = os.getenv("SAMPLE_JSON_PATH", "sample.json")
    api_table: str = os.getenv("GCP_API_TABLE", "change_actions_clean")
    api_gcs_prefix: str = os.getenv("GCS_API_PREFIX", "change-actions")


    def validate_gcp_config(self) -> None:
        """Validates GCP config; raises ValueError if missing parameters when not in dry-run mode."""
        if self.dry_run:
            return
        missing = []
        if not self.project_id:
            missing.append("GCP_PROJECT_ID")
        if not self.bucket:
            missing.append("GCS_BUCKET")
        if not self.dataset:
            missing.append("GCP_DATASET")
        if not self.table:
            missing.append("GCP_TABLE")
        if missing:
            raise ValueError(f"Missing required GCP configurations: {', '.join(missing)}. Set them in .env or run with --dry-run.")

Config = Settings

