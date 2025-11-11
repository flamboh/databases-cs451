import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _run_script(script_name: str) -> None:
    script_path = PROJECT_ROOT / script_name
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"{script_name} failed with exit code {result.returncode}\n"
            f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
        )


def test_m1_tester_script():
    _run_script("m1_tester.py")


def test_exam_tester_m1_script():
    _run_script("exam_tester_m1.py")
