import io
import re
from dataclasses import dataclass, field
from xml.etree.ElementTree import Element, ParseError

from defusedxml import ElementTree as SafeET
from defusedxml.common import DefusedXmlException


class XmlParseError(Exception):
    """Raised when XML is malformed, invalid, or violates safety limits."""


@dataclass
class ParsedTestCase:
    test_id: str
    test_name: str
    class_name: str
    file_path: str | None
    duration_seconds: float
    status: str  # "passed", "failed", "error", "skipped"
    error_type: str | None = None
    error_message: str | None = None
    stack_trace: str | None = None


@dataclass
class ParseSummary:
    total_tests: int = 0
    passed_tests: int = 0
    failed_tests: int = 0
    skipped_tests: int = 0
    error_tests: int = 0
    test_cases: list[ParsedTestCase] = field(default_factory=list)

    @property
    def failures(self) -> list[ParsedTestCase]:
        return [tc for tc in self.test_cases if tc.status in ("failed", "error")]


def _extract_error_details(elem: Element, default_type: str) -> tuple[str, str | None, str | None]:
    error_type = elem.get("type") or default_type
    message = elem.get("message")
    stack_trace = elem.text.strip() if elem.text else None

    if not message and stack_trace:
        first_line = stack_trace.splitlines()[0].strip()
        message = first_line[:500] if first_line else None

    return error_type, message, stack_trace


def parse_junit_xml(raw_xml: bytes | str) -> ParseSummary:
    """Safely parse JUnit XML content and extract test cases and failure summaries."""
    if isinstance(raw_xml, str):
        raw_xml = raw_xml.encode("utf-8")

    if not raw_xml or not raw_xml.strip():
        raise XmlParseError("JUnit XML content is empty")

    try:
        root = SafeET.fromstring(raw_xml)
    except (DefusedXmlException, ParseError, ValueError) as err:
        raise XmlParseError(f"Failed to parse JUnit XML: {err}") from err

    tag = root.tag.lower() if root.tag else ""
    # Strip namespaces if present (e.g. {http://...}testsuites)
    if "}" in tag:
        tag = tag.split("}", 1)[1]

    if tag not in ("testsuites", "testsuite") and not root.findall(".//testcase"):
        raise XmlParseError(f"Root XML element '{root.tag}' is not a recognized JUnit element")

    summary = ParseSummary()
    seen_test_ids: set[str] = set()

    # Traverse all testcases in the XML document
    # Also find parent testsuite if possible
    suite_names: dict[int, str] = {}
    for suite in root.iter():
        stitle = suite.tag.lower()
        if "}" in stitle:
            stitle = stitle.split("}", 1)[1]
        if stitle == "testsuite":
            sname = suite.get("name", "")
            for tc in suite.findall("testcase"):
                suite_names[id(tc)] = sname

    for tc_elem in root.findall(".//testcase"):
        name = tc_elem.get("name") or "unnamed_test"
        classname = tc_elem.get("classname") or suite_names.get(id(tc_elem)) or ""
        file_path = tc_elem.get("file")

        raw_time = tc_elem.get("time", "0.0")
        try:
            duration_seconds = max(0.0, float(raw_time))
        except (ValueError, TypeError):
            duration_seconds = 0.0

        if classname:
            test_id = f"{classname}::{name}"
        else:
            test_id = name

        # Disambiguate if multiple testcases have the same test_id within one report
        base_id = test_id
        counter = 1
        while test_id in seen_test_ids:
            counter += 1
            test_id = f"{base_id}#{counter}"
        seen_test_ids.add(test_id)

        status = "passed"
        error_type: str | None = None
        error_message: str | None = None
        stack_trace: str | None = None

        # Check for failure or error elements
        failure_elem = tc_elem.find("failure")
        error_elem = tc_elem.find("error")
        skipped_elem = tc_elem.find("skipped")
        if skipped_elem is None:
            skipped_elem = tc_elem.find("ignored")

        if failure_elem is not None:
            status = "failed"
            error_type, error_message, stack_trace = _extract_error_details(
                failure_elem, default_type="AssertionError"
            )
        elif error_elem is not None:
            status = "error"
            error_type, error_message, stack_trace = _extract_error_details(
                error_elem, default_type="Error"
            )
        elif skipped_elem is not None:
            status = "skipped"
            error_message = skipped_elem.get("message")
        else:
            status = "passed"

        parsed = ParsedTestCase(
            test_id=test_id,
            test_name=name,
            class_name=classname,
            file_path=file_path,
            duration_seconds=duration_seconds,
            status=status,
            error_type=error_type,
            error_message=error_message,
            stack_trace=stack_trace,
        )

        summary.test_cases.append(parsed)
        summary.total_tests += 1
        if status == "passed":
            summary.passed_tests += 1
        elif status == "failed":
            summary.failed_tests += 1
        elif status == "error":
            summary.error_tests += 1
            summary.failed_tests += 1  # Standard CI convention: errors count towards total failures
        elif status == "skipped":
            summary.skipped_tests += 1

    return summary
