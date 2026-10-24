"""Step 1: download the raw loan CSV (skipped when a cached copy already exists)."""
import csv
import logging
import urllib.request
from pathlib import Path

from . import config

log = logging.getLogger(__name__)


def download_loans(force: bool = False) -> str:
    """Download the source file and check its header. Returns the local file path."""
    target: Path = config.RAW_CSV
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists() and not force:
        log.info("Using cached file %s", target)
    else:
        # Download to a temp name first so a failed download never leaves a half-written file.
        tmp = target.with_suffix(".part")
        log.info("Downloading %s", config.SOURCE_URL)
        urllib.request.urlretrieve(config.SOURCE_URL, tmp)
        tmp.replace(target)

    validate_header(target)
    log.info("Raw file ready: %s (%.1f MB)", target, target.stat().st_size / 1e6)
    return str(target)


def validate_header(path: Path) -> None:
    """Schema-drift check: fail early if the source no longer has the columns we use."""
    with open(path, newline="", encoding="utf-8") as f:
        header = next(csv.reader(f))
    missing = [c for c in config.REQUIRED_COLUMNS if c not in header]
    if missing:
        raise ValueError(f"Source file is missing expected columns: {missing}")
