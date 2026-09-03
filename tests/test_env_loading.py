"""Guards that ``.env`` is loaded by CLI entry points, never at import time."""

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SEARCH_ROOTS = (REPO_ROOT / "src", REPO_ROOT / "scripts")


_DEFINITIONS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)


def _module_scope_dotenv_calls(path: Path) -> list[int]:
    tree = ast.parse(path.read_text())
    lines = []
    for node in tree.body:
        if isinstance(node, _DEFINITIONS):
            continue
        for sub in ast.walk(node):
            if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name) and sub.func.id == "load_dotenv":
                lines.append(sub.lineno)
    return lines


def _python_files() -> list[Path]:
    return [p for root in SEARCH_ROOTS if root.is_dir() for p in sorted(root.rglob("*.py"))]


@pytest.mark.parametrize("path", _python_files(), ids=lambda p: str(p.relative_to(REPO_ROOT)))
def test_no_module_scope_load_dotenv(path: Path) -> None:
    offenders = _module_scope_dotenv_calls(path)
    assert not offenders, (
        f"{path.relative_to(REPO_ROOT)} calls load_dotenv() at module scope "
        f"(line {offenders[0]}). Importing the package must not mutate os.environ — "
        f"it leaks a developer's .env into unit tests and sends them at production R2. "
        f"Call load_dotenv() inside the CLI entry point instead."
    )


def test_entry_points_load_dotenv() -> None:
    from spicy_regs.pipelines import regulations
    from spicy_regs.pipelines.rollups import base

    for module in (regulations, base):
        source = Path(module.__file__).read_text()
        assert "load_dotenv()" in source, f"{module.__name__} never calls load_dotenv()"
