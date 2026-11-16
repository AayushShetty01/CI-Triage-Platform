from tools.publish_junit import _add_stable_properties


def test_stable_ci_properties_make_report_repeatable_and_service_specific():
    report = b'<testsuite name="sample"><testcase name="passes" /></testsuite>'
    first = _add_stable_properties(report, "123-1-parser", "parser", "org/repo", "CI", "abc")
    replay = _add_stable_properties(report, "123-1-parser", "parser", "org/repo", "CI", "abc")
    other_service = _add_stable_properties(
        report, "123-1-classifier", "classifier", "org/repo", "CI", "abc"
    )

    assert first == replay
    assert first != other_service
