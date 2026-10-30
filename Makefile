.PHONY: failure-tests

failure-tests:
	python -m pytest -c services/ingestion/pytest.ini services/ingestion/failure_simulation/test_failure_scenarios.py -m failure -v
