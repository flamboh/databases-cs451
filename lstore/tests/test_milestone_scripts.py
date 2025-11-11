import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CS451_PATHS = [
    PROJECT_ROOT / "CS451",
    PROJECT_ROOT / "cs451",
]


def _cleanup_cs451():
    for path in CS451_PATHS:
        if path.exists():
            shutil.rmtree(path)


def _run_script(script_name: str, cleanup_before: bool = False) -> None:
    if cleanup_before:
        _cleanup_cs451()
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
    _run_script("m1_tester.py", cleanup_before=True)


def test_exam_tester_m1_script():
    _run_script("exam_tester_m1.py", cleanup_before=True)


def test_m2_tester_scripts():
    _cleanup_cs451()
    _run_script("m2_tester_part1.py")
    _run_script("m2_tester_part2.py")
    _cleanup_cs451()


def test_exam_tester_m2_scripts():
    _cleanup_cs451()
    _run_script("exam_tester_m2_part1.py")
    _run_script("exam_tester_m2_part2.py")
    _cleanup_cs451()
