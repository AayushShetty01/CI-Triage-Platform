from tools.dashboard_snapshot import _new_failure_signatures


def test_new_failure_signatures_excludes_previous_and_missing_fingerprints():
    previous = {
        "run_id": "previous",
        "failures": [{"fingerprint": "known"}, {"fingerprint": None}],
    }
    current = {
        "run_id": "current",
        "failures": [
            {"fingerprint": "known"},
            {"fingerprint": "new"},
            {"fingerprint": "new"},
        ],
    }

    assert _new_failure_signatures([previous, current], [current]) == ["new"]
