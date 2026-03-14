"""结构门禁：文件行数、函数行数、目录文件数。"""

from __future__ import annotations

import argparse
import ast
from pathlib import Path


def _count_file_lines(path: Path) -> int:
    return len(path.read_text(encoding='utf-8', errors='ignore').splitlines())


def _iter_python_files(root: Path) -> list[Path]:
    ignored = {'.venv', '__pycache__', '.git', 'node_modules', '.tmp', 'output'}
    return [
        p
        for p in root.rglob('*.py')
        if all(part not in ignored for part in p.parts)
    ]


def _max_function_length(path: Path) -> int:
    tree = ast.parse(path.read_text(encoding='utf-8', errors='ignore'))
    best = 0
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            end = getattr(node, 'end_lineno', node.lineno)
            best = max(best, end - node.lineno + 1)
    return best


def _check_files_per_dir(root: Path, max_files: int) -> list[str]:
    ignored = {'.venv', '__pycache__', '.git', 'node_modules', '.tmp', 'output'}
    errors: list[str] = []
    for directory in root.rglob('*'):
        if not directory.is_dir():
            continue
        if any(part in ignored for part in directory.parts):
            continue
        count = sum(1 for item in directory.iterdir() if item.is_file())
        if count > max_files:
            errors.append(f'目录文件数超限: {directory} -> {count} > {max_files}')
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description='质量门禁检查')
    parser.add_argument('--max-file-lines', type=int, default=1000)
    parser.add_argument('--max-func-lines', type=int, default=200)
    parser.add_argument('--max-files-per-dir', type=int, default=10)
    parser.add_argument('--root', default='.')
    args = parser.parse_args()

    root = Path(args.root).resolve()
    py_files = _iter_python_files(root)
    errors: list[str] = []

    for path in py_files:
        lines = _count_file_lines(path)
        if lines > args.max_file_lines:
            errors.append(f'文件行数超限: {path} -> {lines} > {args.max_file_lines}')

        longest = _max_function_length(path)
        if longest > args.max_func_lines:
            errors.append(f'函数行数超限: {path} -> {longest} > {args.max_func_lines}')

    errors.extend(_check_files_per_dir(root, args.max_files_per_dir))

    if errors:
        print('质量门禁失败：')
        for item in errors:
            print('-', item)
        return 1

    print('质量门禁通过')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
