#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
llm_prep.py — многоцелевой CLI для подготовки контекста проекта к отправке в LLM.

Команды:
  - tree: показать дерево каталогов/файлов с фильтрами.
  - print: печать файлов (полностью или скелетно), с опцией рекурсивных импортов для *.py.
  - deps: печать дерева зависимостей по символам (или всем из файла).

Особенности:
  * Рекурсивные исключения (--exclude-glob) и включения (--include-glob).
  * Скелетная печать для Python (AST: imports, constants, functions, classes).
  * Скелет для не-Python — первые N строк (по умолчанию 80), без раскраски.
  * Рекурсивное разрешение импортов только внутри --root (in-project).
  * Дерево зависимостей: классы/функции/методы/константы, формат tree|list|dot (Graphviz).

Автор: специально под твои требования. Зависимостей нет.
Лицензия: Apache-2.0
"""

from __future__ import annotations

import argparse
import ast
import fnmatch
import io
import json
import os
import re
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple

# ------------------------------ общие настройки ------------------------------

SKIP_DIRS_DEFAULT = {
    "__pycache__", ".git", ".hg", ".svn",
    ".mypy_cache", ".pytest_cache", ".ruff_cache",
    "venv", ".venv", "env", ".env", "build", "dist", ".idea", ".vscode"
}
TEXT_EXTS = {
    ".py", ".pyi",
    ".js", ".jsx", ".ts", ".tsx",
    ".css", ".scss", ".sass",
    ".md", ".rst",
    ".json", ".yml", ".yaml", ".toml", ".ini",
    ".txt", ".env", ".sh", ".cfg", ".conf",
}
CODE_FENCE_LANG = {
    ".py": "python",
    ".pyi": "python",
    ".js": "javascript",
    ".jsx": "jsx",
    ".ts": "typescript",
    ".tsx": "tsx",
    ".css": "css",
    ".scss": "scss",
    ".sass": "sass",
    ".md": "md",
    ".rst": "rst",
    ".json": "json",
    ".yml": "yaml",
    ".yaml": "yaml",
    ".toml": "toml",
    ".ini": "ini",
    ".txt": "",
    ".env": "",
    ".sh": "bash",
    ".cfg": "",
    ".conf": "",
}

CONST_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")  # для модульных констант
ENUM_BASE_SUFFIXES = ("Enum", "IntEnum", "StrEnum", "Flag", "IntFlag", "StrFlag", "AutoEnum")

# ------------------------------ утилиты путей/фильтров ------------------------------

def norm(path: str) -> str:
    return os.path.normpath(path)

def relpath(path: str, root: str) -> str:
    try:
        return os.path.relpath(path, root) or "."
    except Exception:
        return path

def is_textual(path: str) -> bool:
    _, ext = os.path.splitext(path)
    return ext.lower() in TEXT_EXTS

def guess_lang(path: str) -> str:
    _, ext = os.path.splitext(path)
    return CODE_FENCE_LANG.get(ext.lower(), "")

def match_any(path: str, patterns: Iterable[str]) -> bool:
    if not patterns:
        return False
    p = path.replace(os.sep, "/")
    for pat in patterns:
        if fnmatch.fnmatch(p, pat):
            return True
    return False

def should_skip_dir(dirname: str, excludes: Set[str]) -> bool:
    base = os.path.basename(dirname)
    return base in SKIP_DIRS_DEFAULT or base in excludes

def iter_files(
    roots: List[str],
    recursive: bool = True,
    include_globs: List[str] | None = None,
    exclude_globs: List[str] | None = None,
) -> Iterator[str]:
    include_globs = include_globs or []
    exclude_globs = exclude_globs or []
    for root in roots:
        root = norm(root)
        if os.path.isfile(root):
            if (not include_globs or match_any(root.replace(os.sep, "/"), include_globs)) and \
               (not match_any(root.replace(os.sep, "/"), exclude_globs)):
                yield root
            continue

        for dirpath, dirnames, filenames in os.walk(root):
            # фильтр директорий
            dir_rel = dirpath.replace(os.sep, "/")
            # Рекурсивное исключение папок
            dirnames[:] = [
                d for d in dirnames
                if not should_skip_dir(os.path.join(dirpath, d), set()) and
                not match_any(f"{dir_rel}/{d}", exclude_globs)
            ]
            # файлы
            for fn in filenames:
                full = os.path.join(dirpath, fn)
                p = full.replace(os.sep, "/")
                if exclude_globs and match_any(p, exclude_globs):
                    continue
                if include_globs and not match_any(p, include_globs):
                    continue
                yield norm(full)
            if not recursive:
                # если не рекурсивно — не спускаться
                break

# ------------------------------ печать дерева (tree) ------------------------------

def print_tree(
    roots: List[str],
    max_depth: Optional[int],
    exclude_globs: List[str],
    include_globs: List[str],
) -> None:
    """
    Красивое дерево каталогов/файлов с фильтрами.
    """
    def _children(path: str) -> Tuple[List[str], List[str]]:
        try:
            entries = sorted(os.listdir(path))
        except Exception:
            return [], []
        dirs, files = [], []
        for e in entries:
            full = os.path.join(path, e)
            p = full.replace(os.sep, "/")
            if match_any(p, exclude_globs):
                continue
            if os.path.isdir(full):
                # каталог попадет, если внутри теоретически может быть подходящее
                dirs.append(full)
            else:
                if include_globs and not match_any(p, include_globs):
                    continue
                files.append(full)
        return dirs, files

    def _print_dir(root: str, prefix: str, depth: int):
        if max_depth is not None and depth > max_depth:
            return
        dirs, files = _children(root)
        entries = [*dirs, *files]
        for i, p in enumerate(entries):
            last = (i == len(entries) - 1)
            name = os.path.basename(p)
            connector = "└── " if last else "├── "
            print(prefix + connector + name)
            if os.path.isdir(p):
                new_prefix = prefix + ("    " if last else "│   ")
                _print_dir(p, new_prefix, depth + 1)

    for r in roots:
        r = norm(r)
        if os.path.isfile(r):
            print(os.path.basename(r))
        else:
            print(os.path.basename(r) or r)
            _print_dir(r, "", 1)

# ------------------------------ скелет Python (AST) ------------------------------

def get_source(node: ast.AST, source: str) -> str:
    seg = ast.get_source_segment(source, node)
    if seg is not None:
        return seg.strip()
    try:
        return ast.unparse(node)  # type: ignore[attr-defined]
    except Exception:
        return "..."

def unparse(node: Optional[ast.AST], source: str) -> str:
    if not node:
        return ""
    return get_source(node, source)

def is_enum_class(c: ast.ClassDef, source: str) -> bool:
    for b in c.bases:
        txt = unparse(b, source)
        if any(txt.endswith(sfx) for sfx in ENUM_BASE_SUFFIXES):
            return True
    return False

def is_constant_name(name: str) -> bool:
    return bool(CONST_NAME_RE.match(name))

def format_arguments(a: ast.arguments, source: str) -> str:
    parts: List[str] = []

    def fmt_arg(arg: ast.arg, default: Optional[ast.AST]) -> str:
        s = arg.arg
        if arg.annotation:
            s += ": " + unparse(arg.annotation, source)
        if default is not None:
            s += " = " + unparse(default, source)
        return s

    posonly = a.posonlyargs or []
    normal = a.args or []
    pos_defaults = list(a.defaults or [])
    all_pos = posonly + normal
    nd = len(pos_defaults)
    default_map: dict[int, ast.AST] = {}
    if nd:
        for i in range(len(all_pos) - nd, len(all_pos)):
            default_map[i] = pos_defaults[i - (len(all_pos) - nd)]

    for i, arg in enumerate(posonly):
        parts.append(fmt_arg(arg, default_map.get(i)))
    if posonly:
        parts.append("/")

    offset = len(posonly)
    for i, arg in enumerate(normal):
        parts.append(fmt_arg(arg, default_map.get(offset + i)))

    if a.vararg:
        s = "*" + a.vararg.arg
        if a.vararg.annotation:
            s += ": " + unparse(a.vararg.annotation, source)
        parts.append(s)
    else:
        if a.kwonlyargs:
            parts.append("*")

    for i, arg in enumerate(a.kwonlyargs or []):
        default = (a.kw_defaults or [])[i]
        parts.append(fmt_arg(arg, default))

    if a.kwarg:
        s = "**" + a.kwarg.arg
        if a.kwarg.annotation:
            s += ": " + unparse(a.kwarg.annotation, source)
        parts.append(s)

    return ", ".join(parts)

def format_function(f: ast.AST, source: str, indent: str = "") -> str:
    assert isinstance(f, (ast.FunctionDef, ast.AsyncFunctionDef))
    header = []
    for d in f.decorator_list:
        header.append(f"{indent}@{unparse(d, source)}")
    prefix = "async def" if isinstance(f, ast.AsyncFunctionDef) else "def"
    sig = format_arguments(f.args, source)
    ret = ""
    if f.returns:
        ret = f" -> {unparse(f.returns, source)}"
    header.append(f"{indent}{prefix} {f.name}({sig}){ret}: ...")
    return "\n".join(header)

def collect_class_attributes(body: List[ast.stmt], source: str, is_enum: bool) -> Tuple[List[str], List[ast.AST]]:
    lines: List[str] = []
    methods: List[ast.AST] = []

    for node in body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            methods.append(node)
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name):
                target = node.target.id
                ann = unparse(node.annotation, source) if node.annotation else "Any"
                if node.value is not None:
                    val = unparse(node.value, source)
                    lines.append(f"    {target}: {ann} = {val}")
                else:
                    lines.append(f"    {target}: {ann}")
        elif isinstance(node, ast.Assign):
            simple_targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
            if not simple_targets:
                continue
            if is_enum:
                seg = get_source(node, source)
                for ln in seg.splitlines():
                    lines.append("    " + ln.strip())
            else:
                for name in simple_targets:
                    if is_constant_name(name):
                        val = unparse(node.value, source)
                        lines.append(f"    {name} = {val}")
        elif isinstance(node, ast.Pass):
            continue
        else:
            continue

    return lines, methods

def render_class(c: ast.ClassDef, source: str) -> str:
    bases = ", ".join(unparse(b, source) for b in c.bases) if c.bases else ""
    decorators = [f"@{unparse(d, source)}" for d in c.decorator_list]
    header = "\n".join(decorators + [f"class {c.name}({bases}):" if bases else f"class {c.name}:"])

    is_enum = is_enum_class(c, source)
    attr_lines, methods = collect_class_attributes(c.body, source, is_enum=is_enum)

    if not attr_lines and not methods:
        return f"{header}\n    ..."

    parts = [header]
    if attr_lines:
        parts.extend(attr_lines)
        if methods:
            parts.append("")
    for m in methods:
        parts.append(format_function(m, source, indent="    "))
    return "\n".join(parts)

def render_module_constants(mod: ast.Module, source: str) -> List[str]:
    out: List[str] = []
    for node in mod.body:
        if isinstance(node, ast.Assign):
            names = [t.id for t in node.targets if isinstance(t, ast.Name)]
            if not names:
                continue
            if "__all__" in names or all(is_constant_name(n) for n in names):
                seg = get_source(node, source)
                out.append(seg)
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name):
                name = node.target.id
                ann = unparse(node.annotation, source) if node.annotation else "Any"
                if node.value is not None:
                    out.append(f"{name}: {ann} = {unparse(node.value, source)}")
                else:
                    out.append(f"{name}: {ann}")
    return out

def render_imports(mod: ast.Module, source: str) -> List[str]:
    lines: List[str] = []
    for node in mod.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            seg = get_source(node, source)
            lines.append(seg)
    seen = set()
    uniq: List[str] = []
    for ln in lines:
        if ln not in seen:
            uniq.append(ln)
            seen.add(ln)
    return uniq

def render_functions(mod: ast.Module, source: str) -> List[str]:
    out: List[str] = []
    for node in mod.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            out.append(format_function(node, source))
    return out

def render_classes(mod: ast.Module, source: str) -> List[str]:
    out: List[str] = []
    for node in mod.body:
        if isinstance(node, ast.ClassDef):
            out.append(render_class(node, source))
    return out

def to_module_name(path: str, root: str, package_root_hint: Optional[str]) -> str:
    normp = os.path.normpath(path)
    parts = normp.split(os.sep)
    mod_parts = parts[:]
    if package_root_hint and package_root_hint in parts:
        idx = parts.index(package_root_hint)
        mod_parts = parts[idx + 1 :]
    else:
        try:
            rel = os.path.relpath(normp, root)
            mod_parts = rel.split(os.sep)
        except Exception:
            pass
    if mod_parts and mod_parts[-1].endswith(".py"):
        mod_parts[-1] = mod_parts[-1][:-3]
    return ".".join(p for p in mod_parts if p and p != "__init__")

def process_py_file(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            src = f.read()
    except Exception:
        return None
    try:
        tree = ast.parse(src, filename=path, type_comments=True)
    except SyntaxError:
        return None

    imps = render_imports(tree, src)
    consts = render_module_constants(tree, src)
    funcs = render_functions(tree, src)
    classes = render_classes(tree, src)

    if not (imps or consts or funcs or classes):
        return None

    return {
        "imports": imps,
        "constants": consts,
        "functions": funcs,
        "classes": classes,
        "has_future_annotations": ("from __future__ import annotations" in src),
    }

def build_markdown(blocks: List[Tuple[str, Dict[str, Any]]], root: str) -> str:
    chunks: List[str] = []
    for path, data in blocks:
        rel = relpath(path, root)
        header = f"# FILE: {rel}\n```python"
        footer = "```"
        lines: List[str] = [header]
        if not data.get("has_future_annotations"):
            lines.append("from __future__ import annotations")
        if data["imports"]:
            lines.extend(data["imports"])
        if data["constants"]:
            if data["imports"]:
                lines.append("")
            lines.extend(data["constants"])
        if data["functions"]:
            if data["imports"] or data["constants"]:
                lines.append("")
            lines.extend(data["functions"])
        if data["classes"]:
            if data["imports"] or data["constants"] or data["functions"]:
                lines.append("")
            lines.extend(data["classes"])
        lines.append(footer)
        chunks.append("\n".join(lines))
    return "\n\n".join(chunks)

# ------------------------------ печать файлов (полная/скелет) ------------------------------

def read_text(path: str, max_bytes: Optional[int] = None) -> str:
    try:
        with open(path, "rb") as f:
            data = f.read(max_bytes if max_bytes is not None else None)
        return data.decode("utf-8", errors="replace")
    except Exception as e:
        return f"<<ERROR reading {path}: {e}>>"

def print_file_block(path: str, root: str, skeleton: bool, nonpy_skeleton_lines: int) -> str:
    lang = guess_lang(path)
    header = f"# FILE: {relpath(path, root)}\n```{lang}".rstrip()
    footer = "```"
    if skeleton and path.endswith(".py"):
        data = process_py_file(path)
        if not data:
            # пустой модуль или не распарсился — печатаем начало файла
            body = read_text(path).splitlines()[:nonpy_skeleton_lines]
            return f"{header}\n" + "\n".join(body) + f"\n{footer}"
        block = build_markdown([(path, data)], root)
        return block  # уже с # FILE и код-блоком
    else:
        text = read_text(path)
        if skeleton and not path.endswith(".py"):
            lines = text.splitlines()[:nonpy_skeleton_lines]
            body = "\n".join(lines)
        else:
            body = text
        return f"{header}\n{body}\n{footer}"

def print_selection(
    files: List[str],
    root: str,
    skeleton: bool,
    nonpy_skeleton_lines: int,
    show_tree: bool,
) -> None:
    if show_tree:
        # предварительное дерево именно по выбранным файлам
        print("# SELECTED TREE")
        # группируем по папкам
        by_root: Dict[str, List[str]] = defaultdict(list)
        for f in files:
            by_root[os.path.dirname(f)].append(f)
        # рисуем «дерево» в плоском виде
        for f in files:
            print(relpath(f, root))
        print("")
    # печать блоков
    for p in files:
        print(print_file_block(p, root, skeleton, nonpy_skeleton_lines))
        print("")  # разделитель

# ------------------------------ разрешение импортов (внутри root) ------------------------------

@dataclass
class ModuleSpec:
    module: str
    path: str

def discover_project_modules(root: str) -> Dict[str, str]:
    """
    Карту модуль->путь строим по всем *.py внутри root (исключая __pycache__/скрытые).
    """
    mapping: Dict[str, str] = {}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if not d.startswith(".") and d != "__pycache__"]
        for fn in filenames:
            if not fn.endswith(".py"):
                continue
            full = os.path.join(dirpath, fn)
            mod = to_module_name(full, root, None)
            if mod:
                mapping[mod] = norm(full)
    return mapping

def parse_imports_from_file(path: str) -> List[Tuple[str, Optional[str], int]]:
    """
    Возвращает список импортов:
      - ("import", "pkg.mod", level=0) для `import pkg.mod as x` (берем левую часть)
      - ("from", "pkg.mod", level) для `from ..pkg import y`
    """
    out: List[Tuple[str, Optional[str], int]] = []
    try:
        src = read_text(path)
        mod = ast.parse(src, filename=path, type_comments=True)
    except Exception:
        return out
    for node in mod.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                name = alias.name  # pkg.mod
                out.append(("import", name, 0))
        elif isinstance(node, ast.ImportFrom):
            module = node.module  # может быть None при 'from . import x'
            level = node.level or 0
            out.append(("from", module, level))
    return out

def resolve_relative(base_module: str, module: Optional[str], level: int) -> Optional[str]:
    """
    Превращает relative import в абсолютный module str.
    base_module: 'a.b.c', module: 'd.e' or None, level: int
    """
    parts = base_module.split(".")
    if level > len(parts):
        return None
    anchor = parts[: len(parts) - level]
    if module:
        return ".".join([*anchor, module])
    else:
        return ".".join(anchor) if anchor else None

def resolve_inproject_imports(
    start_files: List[str], root: str, include_self: bool = True
) -> List[str]:
    """
    Рекурсивно собирает все файлы внутри root, на которые ссылаются импорты из start_files.
    Только in-project модули.
    """
    module_map = discover_project_modules(root)
    # обратная карта: путь -> модуль
    path_to_module = {v: k for k, v in module_map.items()}

    queue = deque(start_files)
    seen_paths: Set[str] = set()
    result: List[str] = []

    while queue:
        cur = norm(queue.popleft())
        if cur in seen_paths:
            continue
        seen_paths.add(cur)

        modname = path_to_module.get(cur)
        if include_self:
            result.append(cur)

        if not modname:
            # файл не в карте (возможно вне root)
            continue

        # извлекаем импорты и резолвим
        for kind, module, level in parse_imports_from_file(cur):
            target_mod: Optional[str] = None
            if kind == "import":
                target_mod = module
            else:  # from
                if level and module is not None:
                    target_mod = resolve_relative(modname, module, level)
                elif level and module is None:
                    target_mod = resolve_relative(modname, None, level)
                else:
                    target_mod = module

            if not target_mod:
                continue

            # постепенно укорачиваем (import a.b.c -> a.b.c, a.b, a)
            parts = target_mod.split(".")
            for i in range(len(parts), 0, -1):
                candidate = ".".join(parts[:i])
                p = module_map.get(candidate)
                if p:
                    queue.append(p)
                    break

    # стабильный порядок
    result = sorted(set(result))
    return result

# ------------------------------ индекс сущностей и граф зависимостей ------------------------------

@dataclass(frozen=True)
class Symbol:
    fqname: str   # "pkg.mod:Class.method" | "pkg.mod:function" | "pkg.mod:CONST"
    kind: str     # "class"|"method"|"function"|"const"

@dataclass
class ModuleIndex:
    module: str
    path: str
    defs: Dict[str, Symbol]                # local name -> Symbol (Class, function, CONST)
    methods: Dict[Tuple[str, str], Symbol] # (Class, method) -> Symbol
    imported_names: Dict[str, str]         # alias -> target fq ("pkg.mod" or "pkg.mod:Name")
    # для разрешения атрибутов: alias корня -> модуль
    import_roots: Dict[str, str]           # alias -> module

def index_module(path: str, module: str) -> Optional[ModuleIndex]:
    try:
        src = read_text(path)
        tree = ast.parse(src, filename=path, type_comments=True)
    except Exception:
        return None

    defs: Dict[str, Symbol] = {}
    methods: Dict[Tuple[str, str], Symbol] = {}
    imported_names: Dict[str, str] = {}
    import_roots: Dict[str, str] = {}

    # imports
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                name = alias.name  # pkg.mod
                asname = alias.asname or name.split(".")[0]
                import_roots[asname] = name
        elif isinstance(node, ast.ImportFrom):
            level = node.level or 0
            base = node.module or ""
            # относительные импортируем как "module" — точное восстановление fq м.б. неполное
            if level > 0:
                # отметим, что корень относительный; разрешим позже по месту вызова при необходимости
                pass
            for alias in node.names:
                nm = alias.name
                asname = alias.asname or nm
                if base:
                    imported_names[asname] = f"{base}:{nm}"  # "pkg.mod:Name"
                else:
                    # `from . import X` — оставим как просто имя (разрешится локально)
                    imported_names[asname] = nm

    # defs: functions, classes(+methods), constants
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            fq = f"{module}:{node.name}"
            defs[node.name] = Symbol(fq, "function")
        elif isinstance(node, ast.ClassDef):
            c_fq = f"{module}:{node.name}"
            defs[node.name] = Symbol(c_fq, "class")
            # методы
            for m in node.body:
                if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    m_fq = f"{module}:{node.name}.{m.name}"
                    methods[(node.name, m.name)] = Symbol(m_fq, "method")
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            # грубо: все UPPER_CASE считаем константами
            names: List[str] = []
            if isinstance(node, ast.Assign):
                names = [t.id for t in node.targets if isinstance(t, ast.Name)]
            else:
                if isinstance(node.target, ast.Name):
                    names = [node.target.id]
            for nm in names:
                if CONST_NAME_RE.match(nm):
                    defs[nm] = Symbol(f"{module}:{nm}", "const")

    return ModuleIndex(module, path, defs, methods, imported_names, import_roots)

def build_project_index(py_files: List[str], root: str) -> Dict[str, ModuleIndex]:
    module_map = discover_project_modules(root)
    path_to_module = {v: k for k, v in module_map.items()}
    index: Dict[str, ModuleIndex] = {}
    for p in py_files:
        mod = path_to_module.get(norm(p))
        if not mod:
            continue
        mi = index_module(p, mod)
        if mi:
            index[mod] = mi
    return index

def extract_name_chain(node: ast.AST) -> List[str]:
    """ Возвращает цепочку имён для выражений вида a.b.c """
    parts: List[str] = []
    cur = node
    while isinstance(cur, ast.Attribute):
        parts.append(cur.attr)
        cur = cur.value
    if isinstance(cur, ast.Name):
        parts.append(cur.id)
    parts.reverse()
    return parts

def collect_symbol_deps_for_module(mi: ModuleIndex, src: str, index: Dict[str, ModuleIndex]) -> Dict[str, Set[str]]:
    """
    Для каждого локального Symbol в модуле mi собираем набор fq зависимостей.
    Пытаемся разрешать:
      - локальные ссылки (на defs/methods)
      - import-roots (a.b.c -> alias 'a' -> module 'pkg', далее к сущностям не всегда можно добраться, но связь модульная сохраняется)
      - imported_names (from pkg.mod import X as Y -> Y -> pkg.mod:X)
    """
    tree = ast.parse(src, filename=mi.path, type_comments=True)
    deps: Dict[str, Set[str]] = defaultdict(set)

    # удобные карты:
    local_names = set(mi.defs.keys())
    class_to_methods = defaultdict(set)
    for (cls, m), sym in mi.methods.items():
        class_to_methods[cls].add(m)

    def add_dep(owner_fq: str, target_fq: str):
        if owner_fq == target_fq:
            return
        deps[owner_fq].add(target_fq)

    # Карта быстрых ссылок: локальные имя -> fq
    local_to_fq: Dict[str, str] = {k: v.fqname for k, v in mi.defs.items()}
    for (cls, m), sym in mi.methods.items():
        local_to_fq[m] = sym.fqname  # допускаем shadowing, но ок

    # Обход: для каждой функции/класса/метода собираем атачи
    class FunctionVisitor(ast.NodeVisitor):
        def __init__(self, owner_fq: str):
            self.owner_fq = owner_fq

        def visit_Call(self, node: ast.Call):
            # попытка разобрать имя вызываемого
            name_chain = extract_name_chain(node.func)
            if not name_chain:
                return self.generic_visit(node)
            # single name?
            if len(name_chain) == 1:
                nm = name_chain[0]
                # локально определённая сущность?
                if nm in local_to_fq:
                    add_dep(self.owner_fq, local_to_fq[nm])
                elif nm in mi.imported_names:
                    add_dep(self.owner_fq, mi.imported_names[nm])  # может быть "pkg.mod:Name" или просто "Name"
                elif nm in mi.import_roots:
                    # ссылается на модуль
                    add_dep(self.owner_fq, mi.import_roots[nm])  # модульная зависимость
            else:
                root = name_chain[0]
                if root in mi.import_roots:
                    mod = mi.import_roots[root]
                    # если есть индекс по этому модулю — связываем модульно
                    add_dep(self.owner_fq, mod)
            self.generic_visit(node)

        def visit_Attribute(self, node: ast.Attribute):
            # может быть обращение к CONST/CLASS через локальное имя
            name_chain = extract_name_chain(node)
            if len(name_chain) == 1:
                nm = name_chain[0]
                if nm in local_to_fq:
                    add_dep(self.owner_fq, local_to_fq[nm])
            elif len(name_chain) >= 2:
                root = name_chain[0]
                leaf = name_chain[-1]
                if root in mi.import_roots:
                    add_dep(self.owner_fq, mi.import_roots[root])
                elif root in mi.imported_names:
                    add_dep(self.owner_fq, mi.imported_names[root])
            self.generic_visit(node)

    # обходим модуль и для каждой сущности запускаем визитор
    with open(mi.path, "r", encoding="utf-8") as f:
        src_mod = f.read()
    tree = ast.parse(src_mod, filename=mi.path, type_comments=True)

    # модульные функции/константы/классы
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            owner = mi.defs.get(node.name)
            if owner:
                FunctionVisitor(owner.fqname).visit(node)
        elif isinstance(node, ast.ClassDef):
            owner = mi.defs.get(node.name)  # сам класс
            if owner:
                # базовые классы/декораторы как зависимости (грубо)
                for b in node.bases:
                    chain = extract_name_chain(b)
                    if chain:
                        root = chain[0]
                        if root in mi.import_roots:
                            add_dep(owner.fqname, mi.import_roots[root])
                        elif root in mi.imported_names:
                            add_dep(owner.fqname, mi.imported_names[root])
                        elif root in local_to_fq:
                            add_dep(owner.fqname, local_to_fq[root])
                for d in node.decorator_list:
                    chain = extract_name_chain(d)
                    if chain:
                        rt = chain[0]
                        if rt in mi.import_roots:
                            add_dep(owner.fqname, mi.import_roots[rt])
                        elif rt in mi.imported_names:
                            add_dep(owner.fqname, mi.imported_names[rt])
                        elif rt in local_to_fq:
                            add_dep(owner.fqname, local_to_fq[rt])
            # методы
            for m in node.body:
                if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    sym = mi.methods.get((node.name, m.name))
                    if sym:
                        FunctionVisitor(sym.fqname).visit(m)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            # константы — зависимости по RHS (грубо)
            targets: List[str] = []
            if isinstance(node, ast.Assign):
                targets = [t.id for t in node.targets if isinstance(t, ast.Name) and CONST_NAME_RE.match(t.id)]
            elif isinstance(node, ast.AnnAssign):
                if isinstance(node.target, ast.Name) and CONST_NAME_RE.match(node.target.id):
                    targets = [node.target.id]
            if targets:
                for t in targets:
                    owner = mi.defs.get(t)
                    if not owner:
                        continue
                    # просканируем RHS для импортов
                    class RHSVisitor(ast.NodeVisitor):
                        def visit_Name(self, n: ast.Name):
                            nm = n.id
                            if nm in local_to_fq:
                                add_dep(owner.fqname, local_to_fq[nm])
                            elif nm in mi.import_roots:
                                add_dep(owner.fqname, mi.import_roots[nm])
                            elif nm in mi.imported_names:
                                add_dep(owner.fqname, mi.imported_names[nm])
                    if isinstance(node, ast.Assign):
                        RHSVisitor().visit(node.value)
                    elif isinstance(node, ast.AnnAssign) and node.value is not None:
                        RHSVisitor().visit(node.value)

    return deps

def build_deps_graph(py_files: List[str], root: str) -> Tuple[Dict[str, Set[str]], Dict[str, Symbol]]:
    """
    Возвращает (graph, symbol_table), где graph: owner_fq -> set(deps),
    symbol_table: fq -> Symbol
    """
    index = build_project_index(py_files, root)
    fq_to_symbol: Dict[str, Symbol] = {}
    graph: Dict[str, Set[str]] = defaultdict(set)

    for mi in index.values():
        # собрать символы
        for s in mi.defs.values():
            fq_to_symbol[s.fqname] = s
        for s in mi.methods.values():
            fq_to_symbol[s.fqname] = s
    # зависимости
    for mi in index.values():
        src = read_text(mi.path)
        deps = collect_symbol_deps_for_module(mi, src, index)
        for owner, targets in deps.items():
            graph[owner].update(targets)
    return graph, fq_to_symbol

# ------------------------------ форматирование графа зависимостей ------------------------------

def print_deps_tree(graph: Dict[str, Set[str]], roots: List[str], max_depth: Optional[int] = None) -> None:
    visited: Set[str] = set()

    def dfs(node: str, prefix: str, depth: int, is_last: bool):
        print(prefix + ("└── " if is_last else "├── ") + node)
        if max_depth is not None and depth >= max_depth:
            return
        if node in visited:
            return
        visited.add(node)
        children = sorted(graph.get(node, []))
        for i, ch in enumerate(children):
            last = (i == len(children) - 1)
            dfs(ch, prefix + ("    " if is_last else "│   "), depth + 1, last)

    for r in roots:
        dfs(r, "", 0, True)

def print_deps_list(graph: Dict[str, Set[str]], roots: List[str]) -> None:
    seen: Set[str] = set()
    q = deque(roots)
    order: List[str] = []
    while q:
        x = q.popleft()
        if x in seen:
            continue
        seen.add(x)
        order.append(x)
        for y in sorted(graph.get(x, [])):
            q.append(y)
    for x in order:
        print(x)

def print_deps_dot(graph: Dict[str, Set[str]], roots: List[str]) -> None:
    # фильтруем подграф достижимых вершин
    reachable: Set[str] = set()
    q = deque(roots)
    while q:
        x = q.popleft()
        if x in reachable:
            continue
        reachable.add(x)
        for y in graph.get(x, []):
            q.append(y)
    print("digraph G {")
    for a in sorted(reachable):
        print(f'  "{a}";')
    for a in sorted(reachable):
        for b in sorted(graph.get(a, [])):
            if b in reachable:
                print(f'  "{a}" -> "{b}";')
    print("}")

# ------------------------------ CLI ------------------------------

def cmd_tree(args: argparse.Namespace) -> None:
    roots = args.paths or ["."]
    print_tree(
        roots=roots,
        max_depth=args.max_depth,
        exclude_globs=args.exclude_glob or [],
        include_globs=args.include_glob or [],
    )

def cmd_print(args: argparse.Namespace) -> None:
    root = norm(args.root or ".")
    paths = [norm(p) for p in (args.paths or ["."])]

    if args.with_imports:
        # соберём импортные зависимости только для указанных файлов
        start_files: List[str] = []
        for p in paths:
            if os.path.isdir(p):
                # берём все py из папки (не рекурсивно/рекурсивно по флагу)
                py_found = list(iter_files([p], recursive=args.recursive, include_globs=["*.py"], exclude_globs=args.exclude_glob or []))
                start_files.extend(py_found)
            else:
                start_files.append(p)
        files = resolve_inproject_imports(start_files, root)
        # при этом пользовательские include/exclude применим дополнительно
        files = [f for f in files
                 if (not args.include_glob or match_any(f.replace(os.sep, "/"), args.include_glob))
                 and (not args.exclude_glob or not match_any(f.replace(os.sep, "/"), args.exclude_glob))]
    else:
        files = list(iter_files(
            roots=paths,
            recursive=args.recursive,
            include_globs=args.include_glob or [],
            exclude_globs=args.exclude_glob or []
        ))

    files = sorted(set(files))
    print_selection(
        files=files,
        root=root,
        skeleton=bool(args.skeleton),
        nonpy_skeleton_lines=int(args.nonpy_skeleton_lines),
        show_tree=bool(args.show_tree),
    )

def parse_symbol_arg(s: str) -> str:
    """
    Принимает строку вида:
      "pkg.mod:Name" | "pkg.mod:Class.method" | "pkg.mod:CONST"
    Возвращает то же — просто валидация синтаксиса по месту.
    """
    if ":" not in s:
        raise ValueError("Symbol must be 'module:path' like 'pkg.mod:Name' or 'pkg.mod:Class.method'")
    return s

def cmd_deps(args: argparse.Namespace) -> None:
    root = norm(args.root or ".")
    symbols: List[str] = []
    py_files: List[str] = []

    if args.symbols:
        symbols = [parse_symbol_arg(s.strip()) for s in args.symbols.split(",") if s.strip()]

    if args.file:
        # достраиваем список файлов для индексации: файл + все py внутри root (или хотя бы рядом)
        base = norm(args.file)
        if not os.path.isabs(base):
            base = norm(os.path.join(".", base))
        if not os.path.exists(base):
            raise SystemExit(f"File not found: {args.file}")
        # построим список для индекса: либо только весь проект, либо ближайшее подмножество
        # простой вариант — весь root (стабильно)
        py_files = list(iter_files([root], recursive=True, include_globs=["*.py"], exclude_globs=args.exclude_glob or []))
    else:
        # без файла — индексируем весь root
        py_files = list(iter_files([root], recursive=True, include_globs=["*.py"], exclude_globs=args.exclude_glob or []))

    graph, symtab = build_deps_graph(py_files, root)

    if args.all:
        # собрать все символы из указанного файла как корни
        if not args.file:
            raise SystemExit("--all требует --file")
        module_map = discover_project_modules(root)
        # файл -> модуль
        mod = None
        p = norm(args.file)
        for m, path in module_map.items():
            if norm(path) == norm(p):
                mod = m
                break
        if not mod:
            raise SystemExit(f"Не удалось определить модуль для файла: {args.file}")
        # берем все символы из этого модуля
        roots = sorted([fq for fq in symtab.keys() if fq.startswith(mod + ":")])
    else:
        roots = symbols

    if not roots:
        raise SystemExit("Не заданы корневые символы (--symbols) и не указан --all.")

    fmt = args.format
    if fmt == "tree":
        print_deps_tree(graph, roots, max_depth=args.max_depth)
    elif fmt == "list":
        print_deps_list(graph, roots)
    elif fmt == "dot":
        print_deps_dot(graph, roots)
    else:
        raise SystemExit(f"Unknown format: {fmt}")

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="llm_prep.py", description="Project tree / printer / deps for LLM prompts")
    sub = p.add_subparsers(dest="command", required=True)

    # tree
    pt = sub.add_parser("tree", help="Show tree of directories/files")
    pt.add_argument("paths", nargs="*", help="Paths to show (files/dirs)")
    pt.add_argument("--max-depth", type=int, default=None, help="Limit printed depth")
    pt.add_argument("--exclude-glob", action="append", help="Exclude by glob (repeatable)")
    pt.add_argument("--include-glob", action="append", help="Include by glob (repeatable)")
    pt.set_defaults(func=cmd_tree)

    # print
    pp = sub.add_parser("print", help="Print files (full or skeleton), with optional recursive imports for Python")
    pp.add_argument("paths", nargs="*", help="Files/dirs to print (default: .)")
    pp.add_argument("--root", default=".", help="Project root (for headers/module resolution)")
    pp.add_argument("--recursive", action="store_true", help="Recurse into dirs")
    pp.add_argument("--include-glob", action="append", help="Include by glob (repeatable)")
    pp.add_argument("--exclude-glob", action="append", help="Exclude by glob (repeatable)")
    pp.add_argument("--skeleton", action="store_true", help="Skeleton print (Python AST / first N lines for non-Python)")
    pp.add_argument("--nonpy-skeleton-lines", type=int, default=80, help="Lines for non-Python skeleton (default: 80)")
    pp.add_argument("--show-tree", action="store_true", help="Show preliminary tree of selected files")
    pp.add_argument("--with-imports", action="store_true", help="For Python files: include in-project imports recursively")
    pp.set_defaults(func=cmd_print)

    # deps
    pd = sub.add_parser("deps", help="Print dependency tree for symbols or all from file")
    pd.add_argument("--root", default=".", help="Project root")
    pd.add_argument("--symbols", help="Comma-separated list like 'pkg.mod:Name,pkg.mod:Class.method'")
    pd.add_argument("--file", help="Use all symbols from this file's module if --all")
    pd.add_argument("--all", action="store_true", help="Take all symbols from the file's module as roots")
    pd.add_argument("--exclude-glob", action="append", help="Exclude by glob while indexing")
    pd.add_argument("--format", choices=["tree", "list", "dot"], default="tree", help="Output format")
    pd.add_argument("--max-depth", type=int, default=None, help="Limit depth for 'tree' format")
    pd.set_defaults(func=cmd_deps)

    return p

def main(argv: Optional[List[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main()
