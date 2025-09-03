# Make "tests" importable as a package for absolute imports used in conftest/tests.
# Also ensure project root is on sys.path when running pytest from odd CWDs.
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
