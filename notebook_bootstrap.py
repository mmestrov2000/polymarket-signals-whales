from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType


REPO_ROOT_MARKERS = ("PROJECT_SPEC.md", "ARCHITECTURE.md", "TASKS.md")


def locate_repo_root(start: str | Path) -> Path:
    start_path = Path(start).resolve()
    for candidate in (start_path, *start_path.parents):
        if all((candidate / marker).exists() for marker in REPO_ROOT_MARKERS) and (candidate / "src").is_dir():
            return candidate
    raise RuntimeError(f"Could not locate the repository root from {start_path}.")


def prepare_repo_imports(repo_root: str | Path, package_name: str = "src") -> None:
    repo_root_path = Path(repo_root).resolve()
    repo_root_str = str(repo_root_path)

    sys.path[:] = [entry for entry in sys.path if entry != repo_root_str]
    sys.path.insert(0, repo_root_str)

    existing_module = sys.modules.get(package_name)
    if existing_module is None or _module_belongs_to_repo(existing_module, repo_root_path):
        importlib.invalidate_caches()
        return

    for module_name in tuple(sys.modules):
        if module_name == package_name or module_name.startswith(f"{package_name}."):
            sys.modules.pop(module_name, None)

    importlib.invalidate_caches()


def _module_belongs_to_repo(module: ModuleType, repo_root: Path) -> bool:
    module_file = getattr(module, "__file__", None)
    if module_file and _path_is_inside_repo(module_file, repo_root):
        return True

    module_paths = getattr(module, "__path__", ())
    return any(_path_is_inside_repo(path, repo_root) for path in module_paths)


def _path_is_inside_repo(candidate: str | Path, repo_root: Path) -> bool:
    try:
        return Path(candidate).resolve().is_relative_to(repo_root)
    except (OSError, RuntimeError, TypeError, ValueError):
        return False
