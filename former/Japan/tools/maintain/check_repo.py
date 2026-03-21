from __future__ import annotations

import ast
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
TARGET_DIRS = [ROOT / "src", ROOT / "test"]
SKIP_DIR_NAMES = {
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    "node_modules",
    "output",
    ".sisyphus",
    ".git",
}


@dataclass(frozen=True)
class FileViolation:
    path: Path
    message: str


def _iter_dirs(root: Path) -> list[Path]:
    out: list[Path] = []
    if not root.exists():
        return out
    for p in root.rglob("*"):
        if not p.is_dir():
            continue
        if p.name in SKIP_DIR_NAMES:
            continue
        if any(part in SKIP_DIR_NAMES for part in p.parts):
            continue
        out.append(p)
    out.append(root)
    # De-dupe while preserving order
    seen: set[Path] = set()
    deduped: list[Path] = []
    for d in out:
        if d in seen:
            continue
        seen.add(d)
        deduped.append(d)
    return deduped


def _read_text(path: Path) -> str:
    # utf-8-sig strips BOM if present (many repo files are UTF-8 with BOM).
    return path.read_text(encoding="utf-8-sig", errors="replace")


def _count_lines(path: Path) -> int:
    return _read_text(path).count("\n") + 1


def _iter_py_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    files: list[Path] = []
    for p in root.rglob("*.py"):
        if any(part in SKIP_DIR_NAMES for part in p.parts):
            continue
        if "__pycache__" in p.parts:
            continue
        files.append(p)
    return files


def _scan_dir_file_counts() -> list[FileViolation]:
    violations: list[FileViolation] = []
    for root in TARGET_DIRS:
        for d in _iter_dirs(root):
            try:
                files = [p for p in d.iterdir() if p.is_file()]
            except OSError:
                continue
            if len(files) > 5:
                violations.append(
                    FileViolation(
                        d,
                        f"directory has {len(files)} files (limit 5)",
                    )
                )
    return violations


def _scan_file_lengths() -> list[FileViolation]:
    violations: list[FileViolation] = []
    for root in TARGET_DIRS:
        for p in _iter_py_files(root):
            lines = _count_lines(p)
            if lines > 1000:
                violations.append(
                    FileViolation(p, f"file has {lines} lines (limit 1000)")
                )
    return violations


def _scan_function_lengths() -> list[FileViolation]:
    violations: list[FileViolation] = []
    for root in TARGET_DIRS:
        for p in _iter_py_files(root):
            src = _read_text(p)
            try:
                tree = ast.parse(src, filename=str(p))
            except SyntaxError:
                violations.append(FileViolation(p, "syntax error while parsing"))
                continue

            class Visitor(ast.NodeVisitor):
                def __init__(self) -> None:
                    self.class_stack: list[str] = []

                def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802
                    self.class_stack.append(node.name)
                    self.generic_visit(node)
                    self.class_stack.pop()

                def _check(self, node: ast.AST, name: str) -> None:
                    lineno = getattr(node, "lineno", None)
                    end = getattr(node, "end_lineno", None)
                    if lineno is None or end is None:
                        return
                    length = end - lineno + 1
                    if length <= 200:
                        return
                    qual = ".".join(self.class_stack + [name]) if self.class_stack else name
                    violations.append(
                        FileViolation(
                            p,
                            f"{qual} at line {lineno} has {length} lines (limit 200)",
                        )
                    )

                def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
                    self._check(node, node.name)
                    self.generic_visit(node)

                def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
                    self._check(node, node.name)
                    self.generic_visit(node)

            Visitor().visit(tree)
    return violations


def main() -> int:
    problems: list[FileViolation] = []
    problems.extend(_scan_dir_file_counts())
    problems.extend(_scan_file_lengths())
    problems.extend(_scan_function_lengths())

    if not problems:
        print("OK: maintainability checks passed")
        return 0

    print("FAIL: maintainability violations found:\n")
    for v in problems:
        rel = v.path
        try:
            rel = v.path.relative_to(ROOT)
        except Exception:
            rel = v.path
        print(f"- {rel}: {v.message}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

