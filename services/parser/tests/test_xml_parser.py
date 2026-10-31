import pytest

from app.xml_parser import XmlParseError, parse_junit_xml
from tests.conftest import (
    MALFORMED_XML,
    SAMPLE_EMPTY_SUITE_XML,
    SAMPLE_FAILURES_XML,
    SAMPLE_PASS_XML,
)


def test_parse_passing_junit_xml():
    summary = parse_junit_xml(SAMPLE_PASS_XML)
    assert summary.total_tests == 2
    assert summary.passed_tests == 2
    assert summary.failed_tests == 0
    assert summary.skipped_tests == 0
    assert len(summary.failures) == 0

    tc1 = summary.test_cases[0]
    assert tc1.test_name == "test_login_success"
    assert tc1.class_name == "tests.test_auth"
    assert tc1.test_id == "tests.test_auth::test_login_success"
    assert tc1.status == "passed"
    assert tc1.duration_seconds == 0.12


def test_parse_failures_and_errors():
    summary = parse_junit_xml(SAMPLE_FAILURES_XML)
    assert summary.total_tests == 4
    assert summary.passed_tests == 1
    assert summary.failed_tests == 2  # 1 failure + 1 error
    assert summary.skipped_tests == 1
    assert summary.error_tests == 1

    failures = summary.failures
    assert len(failures) == 2

    # Check failure
    f1 = failures[0]
    assert f1.test_name == "rejects_expired_card"
    assert f1.class_name == "payments.CheckoutTests"
    assert f1.test_id == "payments.CheckoutTests::rejects_expired_card"
    assert f1.status == "failed"
    assert f1.error_type == "AssertionError"
    assert f1.error_message == "expected HTTP 402, got 200"
    assert "AssertionError: expected HTTP 402, got 200" in f1.stack_trace
    assert f1.file_path == "tests/test_checkout.py"

    # Check error
    f2 = failures[1]
    assert f2.test_name == "connects_to_cluster"
    assert f2.class_name == "db.ConnectionTests"
    assert f2.test_id == "db.ConnectionTests::connects_to_cluster"
    assert f2.status == "error"
    assert f2.error_type == "TimeoutError"
    assert f2.error_message == "DB connection timed out after 30s"
    assert "raise TimeoutError" in f2.stack_trace


def test_parse_empty_suite():
    summary = parse_junit_xml(SAMPLE_EMPTY_SUITE_XML)
    assert summary.total_tests == 0
    assert summary.passed_tests == 0
    assert summary.failed_tests == 0
    assert len(summary.test_cases) == 0


def test_parse_empty_xml_raises():
    with pytest.raises(XmlParseError, match="JUnit XML content is empty"):
        parse_junit_xml(b"   ")


def test_parse_malformed_xml_raises():
    with pytest.raises(XmlParseError, match="Failed to parse JUnit XML"):
        parse_junit_xml(MALFORMED_XML)


def test_parse_unrecognized_root():
    xml = b"""<configuration><setting name="foo">bar</setting></configuration>"""
    with pytest.raises(XmlParseError, match="not a recognized JUnit element"):
        parse_junit_xml(xml)


def test_fallback_when_attributes_missing():
    xml = b"""<testsuite name="fallback">
        <testcase name="test_no_class" time="invalid">
            <failure>First line of error\nMore stack trace</failure>
        </testcase>
    </testsuite>"""
    summary = parse_junit_xml(xml)
    assert summary.total_tests == 1
    tc = summary.test_cases[0]
    assert tc.test_name == "test_no_class"
    assert tc.class_name == "fallback"
    assert tc.test_id == "fallback::test_no_class"
    assert tc.duration_seconds == 0.0
    assert tc.error_type == "AssertionError"
    assert tc.error_message == "First line of error"
    assert "More stack trace" in tc.stack_trace


def test_safe_entity_expansion_defense():
    # Attempt XML entity expansion (billion laughs / quadratic blowup)
    xml_attack = b"""<?xml version="1.0"?>
    <!DOCTYPE lolz [
     <!ENTITY lol "lol">
     <!ELEMENT lolz (#PCDATA)>
     <!ENTITY lol1 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">
     <!ENTITY lol2 "&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;">
    ]>
    <testsuite name="attack">
        <testcase name="&lol2;" />
    </testsuite>
    """
    with pytest.raises(XmlParseError):
        parse_junit_xml(xml_attack)
