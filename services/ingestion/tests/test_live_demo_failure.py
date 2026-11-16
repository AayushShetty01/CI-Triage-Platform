def test_simulated_pipeline_failure():
    """Simulated intentional assertion error for live triage demo."""
    expected_status = 200
    actual_status = 500
    assert actual_status == expected_status, "API server responded with HTTP 500 Internal Error"
