#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import trio
import trio.testing

printed_diagnostics: set[str] = set()


def run_pyright(platform: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [
            "pyright",
            f"--pythonplatform={platform}",
            "--pythonversion=3.10",
            "--verifytypes=trio",
            "--outputjson",
            "--ignoreexternal",
        ],
        capture_output=True,
    )


def has_docstring_at_runtime(name: str) -> bool:
    """Pyright gives us an object identifier of xx.yy.zz
    This function tries to decompose that into its constituent parts, such that we
    can resolve it, in order to check whether it has a `__doc__` at runtime and
    verifytypes misses it because we're doing overly fancy stuff.
    """
    assert trio.testing is not None

    name_parts = name.split(".")
    assert name_parts[0] == "trio"
    if name_parts[1] == "tests":
        return True

    obj = trio
    try:
        for obj_name in name_parts[1:]:
            obj = getattr(obj, obj_name)
    except AttributeError as exc:
        if "AsyncIOWrapper" in str(exc) or name in (
            "trio.lowlevel.current_kqueue",
            "trio.lowlevel.monitor_kevent",
            "trio.lowlevel.wait_kevent",
            "trio._core._io_kqueue._KqueueStatistics",
            "trio._socket.SocketType.share",
            "trio._core._io_windows._WindowsStatistics",
            "trio._core._windows_cffi.Handle",
            "trio.lowlevel.current_iocp",
            "trio.lowlevel.monitor_completion_key",
            "trio.lowlevel.readinto_overlapped",
            "trio.lowlevel.register_with_iocp",
            "trio.lowlevel.wait_overlapped",
            "trio.lowlevel.write_overlapped",
            "trio.lowlevel.WaitForSingleObject",
            "trio.socket.fromshare",
            "trio._highlevel_generic.StapledStream.send_stream",
            "trio._highlevel_generic.StapledStream.receive_stream",
            "trio._ssl.SSLStream.transport_stream",
            "trio._file_io._HasFileNo",
            "trio._file_io._HasFileNo.fileno",
        ):
            return True

        else:
            print(
                f"Pyright sees {name} at runtime, but unable to getattr({obj.__name__}, {obj_name}).",
                file=sys.stderr,
            )
            return False
    return bool(obj.__doc__)


def check_type(
    platform: str,
    full_diagnostics_file: Path | None,
    expected_errors: list[object],
) -> list[object]:
    assert trio is not None

    res = run_pyright(platform)
    current_result = json.loads(res.stdout)

    if res.stderr:
        print(res.stderr, file=sys.stderr)

    if full_diagnostics_file:
        with open(full_diagnostics_file, "a") as f:
            json.dump(current_result, f, sort_keys=True, indent=4)

    errors = []

    for symbol in current_result["typeCompleteness"]["symbols"]:
        diagnostics = symbol["diagnostics"]
        name = symbol["name"]
        for diagnostic in diagnostics:
            message = diagnostic["message"]
            if name in (
                "trio._path.PosixPath",
                "trio._path.WindowsPath",
            ) and message.startswith("Type of base class "):
                continue

            if name.startswith("trio._path.Path"):
                if message.startswith("No docstring found for"):
                    continue
                if message.startswith(
                    "Type is missing type annotation and could be inferred differently by type checkers",
                ):
                    continue

            if message.startswith("No docstring found for"):
                if has_docstring_at_runtime(symbol["name"]):
                    continue
            else:
                message = f"{name}: {message}"
            if message not in expected_errors and message not in printed_diagnostics:
                print(f"new error: {message}", file=sys.stderr)
            errors.append(message)
            printed_diagnostics.add(message)

        continue

    return errors


def main(args: argparse.Namespace) -> int:
    if args.full_diagnostics_file:
        full_diagnostics_file = Path(args.full_diagnostics_file)
        full_diagnostics_file.write_text("")
    else:
        full_diagnostics_file = None

    errors_by_platform_file = Path(__file__).parent / "_check_type_completeness.json"
    if errors_by_platform_file.exists():
        with open(errors_by_platform_file) as f:
            errors_by_platform = json.load(f)
    else:
        errors_by_platform = {"Linux": [], "Windows": [], "Darwin": [], "all": []}

    changed = False
    for platform in "Linux", "Windows", "Darwin":
        platform_errors = errors_by_platform[platform] + errors_by_platform["all"]
        print("*" * 20, f"\nChecking {platform}...")
        errors = check_type(platform, full_diagnostics_file, platform_errors)

        new_errors = [e for e in errors if e not in platform_errors]
        missing_errors = [e for e in platform_errors if e not in errors]

        if new_errors:
            print(
                f"New errors introduced in `pyright --verifytypes`. Fix them, or ignore them by modifying {errors_by_platform_file}, either manually or with '--overwrite-file'.",
                file=sys.stderr,
            )
            changed = True
        if missing_errors:
            print(
                f"Congratulations, you have resolved existing errors! Please remove them from {errors_by_platform_file}, either manually or with '--overwrite-file'.",
                file=sys.stderr,
            )
            changed = True
            print(missing_errors, file=sys.stderr)

        errors_by_platform[platform] = errors
    print("*" * 20)

    errors_by_platform["all"] = []
    for e in errors_by_platform["Linux"].copy():
        if e in errors_by_platform["Darwin"] and e in errors_by_platform["Windows"]:
            for platform in "Linux", "Windows", "Darwin":
                errors_by_platform[platform].remove(e)
            errors_by_platform["all"].append(e)

    if changed and args.overwrite_file:
        with open(errors_by_platform_file, "w") as f:
            json.dump(errors_by_platform, f, indent=4, sort_keys=True)
            f.write("\n")

    return changed


parser = argparse.ArgumentParser()
parser.add_argument(
    "--overwrite-file",
    action="store_true",
    default=False,
    help="Use this flag to overwrite the current stored results. Either in CI together with a diff check, or to avoid having to manually correct it.",
)
parser.add_argument(
    "--full-diagnostics-file",
    type=Path,
    default=None,
    help="Use this for debugging, it will dump the output of all three pyright runs by platform into this file.",
)
args = parser.parse_args()

assert __name__ == "__main__", "This script should be run standalone"
sys.exit(main(args))
