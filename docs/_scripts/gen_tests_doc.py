# mkdocs-gen-files script that generates a Test Suite Reference page from pytest docstrings.
#
# Enable in mkdocs.yml:
#   plugins:
#     - search
#     - gen-files:
#         scripts:
#           - docs/_scripts/gen_tests_doc.py
#
# Requires:
#   pip install mkdocs-gen-files
from __future__ import annotations

import ast
import html
import textwrap
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

import mkdocs_gen_files

REPO_ROOT = Path(__file__).resolve().parents[2]
TESTS_DIR = REPO_ROOT / "tests"
OUTPUT_PATH = "reference/testing.md"

# Both sync and async tests
TEST_DEF_TYPES: tuple[type[ast.AST], ...] = (ast.FunctionDef, ast.AsyncFunctionDef)


def _read(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8")
    except Exception:
        return ""


def _is_test_file(p: Path) -> bool:
    name = p.name
    return name.endswith(".py") and (name.startswith("test_") or name.endswith("_test.py"))


def _iter_test_files(base: Path) -> Iterable[Path]:
    if not base.exists():
        return []
    for p in sorted(base.rglob("*.py")):
        if _is_test_file(p):
            yield p


def _decorator_mark_name(dec: ast.AST) -> str | None:
    """Extract pytest mark name from decorator AST, e.g. pytest.mark.<name>"""
    target = None
    if isinstance(dec, ast.Attribute):
        target = dec  # @pytest.mark.slow
    elif isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute):
        target = dec.func  # @pytest.mark.parametrize(...)
    if isinstance(target, ast.Attribute):
        parts: list[str] = []
        cur: ast.AST | None = target
        while isinstance(cur, ast.Attribute):
            parts.append(cur.attr)
            cur = cur.value  # type: ignore[assignment]
        if isinstance(cur, ast.Attribute):
            parts.append(cur.attr)
            cur = cur.value  # type: ignore[assignment]
        if isinstance(cur, ast.Name):
            parts.append(cur.id)
        full = ".".join(reversed(parts))
        if full.startswith("pytest.mark."):
            return full.split(".", 2)[-1]
    return None


def _extract_marks(decorators: Sequence[ast.AST]) -> list[str]:
    return list(filter(None, (_decorator_mark_name(d) for d in decorators)))  # type: ignore[arg-type]


def _first_sentence(s: str) -> str:
    s = (s or "").strip().replace("\n", " ").replace("  ", " ")
    # heuristics for splitting
    for sep in (". ", "… ", " - ", " — "):
        if sep in s:
            head = s.split(sep, 1)[0].strip()
            return head + ("" if head.endswith(".") else ".")
    return s


def _slug(*parts: str) -> str:
    raw = "-".join(parts)
    return (
        raw.replace("/", "-")
        .replace("\\", "-")
        .replace(".", "-")
        .replace(" ", "-")
        .replace("::", "-")
        .replace("--", "-")
        .strip("-")
        .lower()
    )


def _esc_md(s: str) -> str:
    # keep it simple; we use HTML for the index anyway
    return s.replace("|", r"\|")


@dataclass
class TestItem:
    kind: str  # "function" or "method"
    name: str  # "test_name" or "Class::test_name"
    short_name: str
    cls_name: str | None
    lineno: int
    doc: str
    summary: str
    marks: list[str]
    rel_path: str

    @property
    def anchor(self) -> str:
        base = self.rel_path.replace("/", "-").replace(".", "-")
        tail = self.name.replace(".", "-").replace("::", "-")
        return _slug(base, tail)


def build() -> str:
    header = (
        "# Test Suite Reference\n\n"
        "> This page is **auto-generated** from pytest docstrings at build time.\n"
        "> Do not edit manually. See `docs/_scripts/gen_tests_doc.py`.\n\n"
    )

    howto = (
        "## How it works\n\n"
        "The generator scans `tests/` for `test_*.py` modules, reads docstrings of modules, test classes, and\n"
        "test functions (including `async def`), and renders them here. The first sentence becomes a short\n"
        "summary in the index; the full docstring appears in **Details**.\n\n"
        "### Writing good test docstrings\n\n"
        "```python\n"
        "async def test_outbox_retry_backoff():\n"
        "    \"\"\"\n"
        "    First two sends fail → item moves to *retry* with exponential backoff;\n"
        "    on the 3rd attempt the item becomes *sent*.\n"
        "    \"\"\"\n"
        "```\n"
    )

    modules_index_html: list[str] = []
    body: list[str] = []

    total_tests = 0
    total_modules = 0

    for test_file in _iter_test_files(TESTS_DIR):
        rel_path = test_file.relative_to(REPO_ROOT).as_posix()
        source = _read(test_file)
        if not source.strip():
            continue

        try:
            tree = ast.parse(source, filename=str(test_file))
        except SyntaxError:
            continue

        mod_doc = ast.get_docstring(tree) or ""
        module_anchor = _slug(rel_path)

        test_items: list[TestItem] = []

        # module-level tests
        for node in tree.body:
            if isinstance(node, TEST_DEF_TYPES) and getattr(node, "name", "").startswith("test_"):
                doc = ast.get_docstring(node) or ""
                marks = _extract_marks(getattr(node, "decorator_list", []))
                test_items.append(
                    TestItem(
                        kind="function",
                        name=node.name,  # type: ignore[attr-defined]
                        short_name=node.name,  # type: ignore[attr-defined]
                        cls_name=None,
                        lineno=node.lineno,  # type: ignore[attr-defined]
                        doc=doc,
                        summary=_first_sentence(doc) or "(no docstring)",
                        marks=marks,
                        rel_path=rel_path,
                    )
                )

        # class-based tests
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and node.name.startswith(("Test", "Tests")):
                for sub in node.body:
                    if isinstance(sub, TEST_DEF_TYPES) and getattr(sub, "name", "").startswith("test_"):
                        doc = ast.get_docstring(sub) or ""
                        marks = _extract_marks(getattr(sub, "decorator_list", []))
                        test_items.append(
                            TestItem(
                                kind="method",
                                name=f"{node.name}::{sub.name}",  # type: ignore[attr-defined]
                                short_name=sub.name,  # type: ignore[attr-defined]
                                cls_name=node.name,
                                lineno=sub.lineno,  # type: ignore[attr-defined]
                                doc=doc,
                                summary=_first_sentence(doc) or "(no docstring)",
                                marks=marks,
                                rel_path=rel_path,
                            )
                        )

        if not (mod_doc or test_items):
            continue

        total_modules += 1
        total_tests += len(test_items)

        # ── Collapsible index entry per module
        marks_pool = sorted({m for ti in test_items for m in ti.marks})
        marks_str = (
            f" &nbsp; <small>(marks: {', '.join(html.escape(m) for m in marks_pool)})</small>"
            if marks_pool
            else ""
        )
        modules_index_html.append(
            f'<details>\n'
            f'  <summary><code>{html.escape(rel_path)}</code>'
            f' &nbsp; <em>{len(test_items)} test{"s" if len(test_items)!=1 else ""}</em>{marks_str}</summary>\n'
        )
        if mod_doc.strip():
            modules_index_html.append(
                f'  <div style="margin: 0.3rem 0 0.6rem 0; color: var(--md-default-fg-color--light)">'
                f'{html.escape(_first_sentence(textwrap.dedent(mod_doc).strip()))}'
                f'</div>\n'
            )
        modules_index_html.append("  <ul>\n")
        for ti in sorted(test_items, key=lambda t: (t.cls_name or "", t.short_name, t.lineno)):
            summary = html.escape(ti.summary)
            modules_index_html.append(
                f'    <li><a href="#{ti.anchor}">{html.escape(ti.name)}</a>'
                f' — <span style="opacity:.85">{summary}</span></li>\n'
            )
        modules_index_html.append("  </ul>\n</details>\n")

        # ── Full module section below the index
        body.append("\n---\n")
        body.append(f"\n### {rel_path}  \n")
        body.append(f"<a id=\"{module_anchor}\"></a>\n\n")

        if mod_doc.strip():
            body.append(textwrap.dedent(mod_doc).strip() + "\n\n")

        if test_items:
            # Summary table
            body.append("#### Tests\n\n")
            body.append("| Test | Summary | Marks | Location |\n")
            body.append("|---|---|---|---|\n")
            for ti in sorted(test_items, key=lambda t: (t.cls_name or "", t.short_name, t.lineno)):
                test_link = f"[{ti.name}](#{ti.anchor})"
                marks = ", ".join(ti.marks) if ti.marks else "—"
                loc = f"`{ti.rel_path}:{ti.lineno}`"
                body.append(f"| {test_link} | {_esc_md(ti.summary)} | {_esc_md(marks)} | {loc} |\n")

            # Full docstrings
            body.append("\n#### Details\n\n")
            for ti in sorted(test_items, key=lambda t: (t.cls_name or "", t.short_name, t.lineno)):
                body.append(f"##### {ti.name}\n")
                body.append(f"<a id=\"{ti.anchor}\"></a>\n\n")
                meta = []
                if ti.marks:
                    meta.append(f"marks: `{', '.join(ti.marks)}`")
                meta.append(f"location: `{ti.rel_path}:{ti.lineno}`")
                body.append(f"*{' • '.join(meta)}*\n\n")
                if ti.doc.strip():
                    body.append(textwrap.dedent(ti.doc).strip() + "\n\n")
                else:
                    body.append("_No docstring._\n\n")

    stats = f"**Modules:** {total_modules} &nbsp;&nbsp; **Tests:** {total_tests}\n\n"
    content = header + stats + howto

    if modules_index_html:
        content += "## Modules\n\n" + "".join(modules_index_html) + "\n"
    else:
        content += "_No tests found._\n"

    content += "".join(body) if body else "\n_No tests found or docstrings missing._\n"
    return content


# Generate file
md = build()
with mkdocs_gen_files.open(OUTPUT_PATH, "w") as f:
    f.write(md)

# Make "Edit on GitHub" for the generated page point to this script
mkdocs_gen_files.set_edit_path(OUTPUT_PATH, "docs/_scripts/gen_tests_doc.py")
