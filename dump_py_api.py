#!/usr/bin/env python3
# dump_py_api.py
# Recursively prints interface skeletons for *.py files and can also emit a JSON dump
# suitable for CI diffs:
# - classes, methods, functions with full signatures (no bodies → "...")
# - Enums with original member values
# - module-level constants: ALL_CAPS, __all__, annotated (PEP 526)
# Output is grouped per file with "# FILE: relative/path.py" header (Markdown),
# and/or written as a JSON structure (--json).

from __future__ import annotations

import ast
import os
import sys
import re
import json
import argparse
from typing import List, Optional, Tuple, Dict, Any

SKIP_DIRS = {
    "__pycache__", ".git", ".hg", ".svn",
    ".mypy_cache", ".pytest_cache", ".ruff_cache",
    "venv", ".venv", "env", ".env", "build", "dist"
}

ENUM_BASE_SUFFIXES = ("Enum", "IntEnum", "StrEnum", "Flag", "IntFlag", "StrFlag", "AutoEnum")
CONST_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")


def relpath(path: str, root: str) -> str:
    try:
        return os.path.relpath(path, root)
    except Exception:
        return path


def get_source(node: ast.AST, source: str) -> str:
    seg = ast.get_source_segment(source, node)
    if seg is not None:
        return seg.strip()
    try:
        return ast.unparse(node)
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


def is_dataclass(c: ast.ClassDef, source: str) -> bool:
    for d in c.decorator_list:
        txt = unparse(d, source)
        if txt.endswith("dataclass") or txt.endswith("dataclasses.dataclass"):
            return True
    return False


def is_protocol(c: ast.ClassDef, source: str) -> bool:
    for b in c.bases:
        txt = unparse(b, source)
        if txt.endswith("Protocol"):
            return True
    return False


def is_typeddict(c: ast.ClassDef, source: str) -> bool:
    for b in c.bases:
        txt = unparse(b, source)
        if txt.endswith("TypedDict"):
            return True
    return False


def is_namedtuple(c: ast.ClassDef, source: str) -> bool:
    for b in c.bases:
        txt = unparse(b, source)
        if txt.endswith("NamedTuple"):
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
    """
    Best-effort conversion from file path to Python module name.
    If package_root_hint ('src') exists inside path, strip up to it; else strip root.
    """
    norm = os.path.normpath(path)
    parts = norm.split(os.sep)
    mod_parts = parts[:]
    if package_root_hint and package_root_hint in parts:
        idx = parts.index(package_root_hint)
        mod_parts = parts[idx + 1 :]
    else:
        # strip root from abs path if possible
        try:
            rel = os.path.relpath(norm, root)
            mod_parts = rel.split(os.sep)
        except Exception:
            pass
    if mod_parts[-1].endswith(".py"):
        mod_parts[-1] = mod_parts[-1][:-3]
    return ".".join(p for p in mod_parts if p and p != "__init__")


def process_file(path: str, root: str) -> Optional[Dict[str, Any]]:
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


def iter_py_files(root: str) -> List[str]:
    out: List[str] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS and not d.startswith(".")]
        for fn in filenames:
            if fn.endswith(".py"):
                out.append(os.path.join(dirpath, fn))
    out.sort()
    return out


def build_markdown(blocks: List[Tuple[str, Dict[str, Any]]], root: str) -> str:
    chunks: List[str] = []
    for path, data in blocks:
        rel = relpath(path, root)
        header = f"# FILE: {rel}\n```python"
        footer = "```"
        lines: List[str] = [header]
        if not data["has_future_annotations"]:
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


def main() -> None:
    ap = argparse.ArgumentParser(description="Dump Python API skeleton and/or JSON for CI.")
    ap.add_argument("root", nargs="?", default=".", help="Root directory to scan (default: .)")
    ap.add_argument("--json", dest="json_path", help="Write machine-readable API dump to this JSON file")
    ap.add_argument("--skeleton", dest="skeleton_path", help="Write Markdown skeleton to this file")
    ap.add_argument("--package-root", dest="pkg_root", default="src", help="Package root hint (default: src)")
    args = ap.parse_args()

    root = args.root
    files = iter_py_files(root)

    collected: List[Tuple[str, Dict[str, Any]]] = []
    json_out: Dict[str, Any] = {"files": {}}

    for p in files:
        data = process_file(p, root)
        if data:
            collected.append((p, data))
            modname = to_module_name(p, root, args.pkg_root)
            json_out["files"][relpath(p, root)] = {
                "module": modname,
                "imports": data["imports"],
                "constants": data["constants"],
                "functions": data["functions"],
                "classes": data["classes"],
            }

    # Write skeleton
    if collected:
        md = build_markdown(collected, root)
        if args.skeleton_path:
            os.makedirs(os.path.dirname(args.skeleton_path) or ".", exist_ok=True)
            with open(args.skeleton_path, "w", encoding="utf-8") as f:
                f.write(md + "\n")
        else:
            print(md)

    # Write JSON
    if args.json_path:
        os.makedirs(os.path.dirname(args.json_path) or ".", exist_ok=True)
        with open(args.json_path, "w", encoding="utf-8") as f:
            json.dump(json_out, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    main()
