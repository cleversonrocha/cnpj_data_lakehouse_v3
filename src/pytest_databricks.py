import pytest
import os
import sys

# Safely resolve the root directory (handles Databricks Job execution where __file__ is undefined)
if "__file__" in globals():
    dir_root = os.path.dirname(os.path.realpath(__file__))
else:
    dir_root = os.getcwd()

# Switch to the root directory and ensure it's on sys.path for imports
os.chdir(dir_root)
if dir_root not in sys.path:
    sys.path.insert(0, dir_root)

# Skip writing .pyc files to the bytecode cache on the cluster.
sys.dont_write_bytecode = True

# Use CLI/VS Code args if provided; otherwise default to running the tests/ folder
pytest_args = sys.argv[1:] if len(sys.argv) > 1 else [
    "-v",
    "--tb=short",
    "--disable-warnings",
    "-p", "no:cacheprovider",
    "tests/"
]

retcode = pytest.main(pytest_args)

# Fail the Databricks job task if any tests fail
if retcode != 0:
    sys.exit(retcode)