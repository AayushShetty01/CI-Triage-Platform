#!/usr/bin/env python3
"""Create a pass/fail pair for demonstrating Query's flaky-test projection."""

from pathlib import Path
from xml.etree import ElementTree


OUTPUT_DIR = Path("reports")
CLASSNAME = "simulation.network.NetworkTests"
TEST_NAME = "intermittent_connection_recovers"


def _report(failed: bool) -> bytes:
    root = ElementTree.Element(
        "testsuite",
        {
            "name": "failure-simulation-flaky",
            "tests": "1",
            "failures": "1" if failed else "0",
            "errors": "0",
            "skipped": "0",
        },
    )
    testcase = ElementTree.SubElement(
        root, "testcase", classname=CLASSNAME, name=TEST_NAME
    )
    if failed:
        failure = ElementTree.SubElement(
            testcase,
            "failure",
            type="TimeoutError",
            message="intermittent connection timeout on first attempt",
        )
        failure.text = "intermittent connection timeout on first attempt"
    return ElementTree.tostring(root, encoding="utf-8", xml_declaration=True)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "simulation-flaky-failure.xml").write_bytes(_report(failed=True))
    (OUTPUT_DIR / "simulation-flaky-recovery.xml").write_bytes(_report(failed=False))


if __name__ == "__main__":
    main()
