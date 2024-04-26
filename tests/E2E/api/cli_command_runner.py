import subprocess
import sys
import typing as t


def _run_command(command: t.List[str]):
    try:
        return subprocess.run(command, check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        print(f"Command: {e.cmd}", file=sys.stderr)
        print(f"Return value: {e.returncode}", file=sys.stderr)
        print(f"stdout:\n{e.stdout}", file=sys.stderr)
        print(f"stderr:\n{e.stderr}", file=sys.stderr)
        raise


def run_orq_command(command: t.List[str]):
    return _run_command(["orq", *command])
