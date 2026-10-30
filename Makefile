.PHONY: failure-tests load-test

failure-tests:
	python -m pytest -c services/ingestion/pytest.ini services/ingestion/failure_simulation/test_failure_scenarios.py -m failure -v

load-test:
	python tools/load/ingestion_load.py --requests 50 --concurrency 5 --report-size small --label load-smoke --output-dir artifacts/load-smoke
