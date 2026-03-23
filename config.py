"""
Shared configuration for equities research scripts.

Handles cross-platform OneDrive path detection.
"""

import platform
import sys
from pathlib import Path

# The OneDrive subfolder name (after "OneDrive - " on Windows, or "OneDrive-" on macOS)
_ONEDRIVE_FOLDER_NAME = "AG"
_RESEARCH_SUBPATH = "01_equities_research"


def get_research_base_dir() -> Path:
    """
    Detect the equities research base directory across macOS and Windows.

    Search order:
      1. macOS: ~/Library/CloudStorage/OneDrive-<ORG>/01_equities_research
      2. Windows: ~/OneDrive - <ORG>/01_equities_research
      3. Fallback: ~/01_equities_research  (created if needed)
    """
    home = Path.home()
    system = platform.system()

    candidates = []

    if system == "Darwin":
        # macOS: OneDrive stores synced folders under ~/Library/CloudStorage/
        cloud_storage = home / "Library" / "CloudStorage"
        if cloud_storage.exists():
            # Look for any OneDrive folder matching the org
            for d in cloud_storage.iterdir():
                if d.is_dir() and d.name.startswith("OneDrive-"):
                    candidates.append(d / _RESEARCH_SUBPATH)
        # Also check the direct OneDrive path (older OneDrive versions)
        candidates.append(home / f"OneDrive - {_ONEDRIVE_FOLDER_NAME}" / _RESEARCH_SUBPATH)

    elif system == "Windows":
        # Windows: OneDrive syncs to ~/OneDrive - <ORG>/
        candidates.append(home / f"OneDrive - {_ONEDRIVE_FOLDER_NAME}" / _RESEARCH_SUBPATH)
        # Also check without space around dash
        candidates.append(home / f"OneDrive-{_ONEDRIVE_FOLDER_NAME}" / _RESEARCH_SUBPATH)
        # Check USERPROFILE-based paths
        import os
        user_profile = os.environ.get("USERPROFILE", "")
        if user_profile:
            up = Path(user_profile)
            candidates.append(up / f"OneDrive - {_ONEDRIVE_FOLDER_NAME}" / _RESEARCH_SUBPATH)

    else:
        # Linux or other
        candidates.append(home / f"OneDrive - {_ONEDRIVE_FOLDER_NAME}" / _RESEARCH_SUBPATH)

    # Return the first candidate that exists
    for path in candidates:
        if path.exists():
            return path

    # If none found, try the first candidate and create it
    if candidates:
        fallback = candidates[0]
    else:
        fallback = home / _RESEARCH_SUBPATH

    print(f"Note: Creating research directory at {fallback}")
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


# Module-level constant for easy import
DEFAULT_RESEARCH_DIR = get_research_base_dir()
