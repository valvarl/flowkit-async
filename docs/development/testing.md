# Test Suite Reference

> This page is generated from pytest docstrings at build time.
> If you see this placeholder, enable the generator plugin and rebuild docs.

## How it works

We use **mkdocs-gen-files** to scan `tests/` and pull docstrings from test modules, classes, and functions,
then render them as documentation. See `docs/_scripts/gen_tests_doc.py`.

### Writing good test docstrings

```python
def test_example():
    """
    Brief summary explaining what the test validates and why.
    Mention key behaviors, invariants, or failure modes.
    """
    ...
```
