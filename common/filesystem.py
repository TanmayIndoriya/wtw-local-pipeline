from pathlib import Path
import shutil
from typing import List

from common.config import get_config


_config = get_config()
_project_config = _config.get_project()

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

_STORAGE_PATHS = {
    "landing": _PROJECT_ROOT / _project_config["storage"]["landing"],
    "bronze": _PROJECT_ROOT / _project_config["storage"]["bronze"],
    "silver": _PROJECT_ROOT / _project_config["storage"]["silver"],
    "gold": _PROJECT_ROOT / _project_config["storage"]["gold"],
    "checkpoints": _PROJECT_ROOT / _project_config["storage"]["checkpoints"],
    "quarantine": _PROJECT_ROOT / _project_config["storage"]["quarantine"],
}


def ensure_directories() -> None:

    for path in _STORAGE_PATHS.values():
        path.mkdir(parents=True, exist_ok=True)


def get_storage_path(layer: str) -> Path:

    layer = layer.lower()

    if layer not in _STORAGE_PATHS:
        raise ValueError(f"Unknown storage layer: {layer}")

    return _STORAGE_PATHS[layer]


def get_dataset_path(layer: str, dataset: str) -> Path:

    path = get_storage_path(layer) / dataset
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_partition_path(layer: str, dataset: str, *partitions: str) -> Path:

    path = get_dataset_path(layer, dataset)

    for partition in partitions:
        path /= partition

    path.mkdir(parents=True, exist_ok=True)

    return path


def list_files(path: Path, pattern: str = "*") -> List[Path]:

    if not path.exists():
        return []

    return sorted(file for file in path.glob(pattern) if file.is_file())


def directory_exists(path: Path) -> bool:
    return path.exists() and path.is_dir()


def remove_directory(path: Path) -> None:

    if path.exists():
        shutil.rmtree(path)


def clear_layer(layer: str) -> None:

    layer_path = get_storage_path(layer)

    remove_directory(layer_path)

    layer_path.mkdir(parents=True, exist_ok=True)


def dataset_exists(layer: str, dataset: str) -> bool:

    return get_dataset_path(layer, dataset).exists()


def initialize_storage() -> None:

    ensure_directories()

def directory_exists(path: Path) -> bool:

    return path.exists() and path.is_dir()


def ensure_directory(path: Path) -> Path:

    path.mkdir(
        parents=True,
        exist_ok=True,
    )

    return path
