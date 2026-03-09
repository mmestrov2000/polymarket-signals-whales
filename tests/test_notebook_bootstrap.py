from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path

from notebook_bootstrap import locate_repo_root, prepare_repo_imports


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_locate_repo_root_finds_repo_from_nested_notebook_dir() -> None:
    notebook_dir = REPO_ROOT / "notebooks" / "wallet_data_exploration"

    assert locate_repo_root(notebook_dir) == REPO_ROOT


def test_prepare_repo_imports_replaces_preloaded_foreign_src_package() -> None:
    original_sys_path = list(sys.path)
    original_src_modules = {
        name: module
        for name, module in sys.modules.items()
        if name == "src" or name.startswith("src.")
    }

    fake_src = types.ModuleType("src")
    fake_src.__path__ = ["/tmp/foreign-src"]
    fake_research = types.ModuleType("src.research")
    fake_src.research = fake_research

    try:
        sys.path[:] = ["/tmp/site-packages", str(REPO_ROOT)]
        sys.modules["src"] = fake_src
        sys.modules["src.research"] = fake_research

        prepare_repo_imports(REPO_ROOT)

        importlib.invalidate_caches()
        imported_src = importlib.import_module("src")
        imported_research = importlib.import_module("src.research")

        assert Path(imported_src.__file__).resolve() == REPO_ROOT / "src" / "__init__.py"
        assert Path(imported_research.__file__).resolve() == REPO_ROOT / "src" / "research" / "__init__.py"
        assert imported_research.SUPPORTED_COHORT_METRICS
        assert sys.path[0] == str(REPO_ROOT)
        assert sys.path.count(str(REPO_ROOT)) == 1
    finally:
        for name in tuple(sys.modules):
            if name == "src" or name.startswith("src."):
                sys.modules.pop(name, None)
        sys.modules.update(original_src_modules)
        sys.path[:] = original_sys_path
        importlib.invalidate_caches()
