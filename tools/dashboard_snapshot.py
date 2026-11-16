#!/usr/bin/env python3
"""Export Query's current API data as a portable dashboard snapshot."""

import html
import json
import os
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen


QUERY_URL = os.getenv("QUERY_URL", "http://localhost:8005")
OUTPUT_DIR = Path(os.getenv("REPORTS_DIR", "reports"))


def _get_json(path: str):
    with urlopen(f"{QUERY_URL}{path}", timeout=15) as response:
        return json.loads(response.read())


def _cell(value) -> str:
    return html.escape(str(value if value is not None else "—"))


def _new_failure_signatures(runs: list[dict], current_runs: list[dict]) -> list[str]:
    current_run_ids = {run["run_id"] for run in current_runs}
    previous = set()
    current = set()
    for run in runs:
        target = current if run["run_id"] in current_run_ids else previous
        target.update(
            failure["fingerprint"]
            for failure in run.get("failures", [])
            if failure.get("fingerprint")
        )
    return sorted(current - previous)


def _render(data: dict) -> str:
    insight = data["insights"]
    counts = insight["test_counts"]
    run_rows = []
    failure_rows = []
    for run in data["runs"]:
        ci = run.get("ci_metadata") or {}
        run_link = ""
        if ci.get("run_url"):
            run_link = (
                f'<a href="{html.escape(ci["run_url"], quote=True)}" '
                'rel="noopener noreferrer">GitHub run</a>'
            )
        test_counts = run["test_counts"]
        run_rows.append(
            "<tr>"
            f"<td>{_cell(ci.get('service', 'unknown'))}</td>"
            f"<td>{_cell(run.get('branch'))}</td>"
            f"<td><code>{_cell((run.get('commit_sha') or '')[:12])}</code></td>"
            f"<td>{test_counts['total']}</td><td>{test_counts['passed']}</td>"
            f"<td>{test_counts['failed']}</td><td>{test_counts['skipped']}</td>"
            f"<td>{_cell(run.get('state'))}</td><td>{run_link}</td></tr>"
        )
        for failure in run.get("failures", []):
            failure_rows.append(
                "<tr>"
                f"<td>{_cell(ci.get('service', 'unknown'))}</td>"
                f"<td><code>{_cell(failure.get('test_id'))}</code></td>"
                f"<td>{_cell(failure.get('category'))}</td>"
                f"<td>{_cell(failure.get('confidence'))}</td>"
                f"<td>{_cell(failure.get('source'))}</td>"
                f"<td><code>{_cell(failure.get('fingerprint'))}</code></td>"
                f"<td>{_cell(failure.get('error_message'))}</td></tr>"
            )
    service_rows = [
        "<tr>" + "".join(
            f"<td>{_cell(service.get(key))}</td>"
            for key in ("service", "runs", "tests", "passed", "failed", "skipped", "failures")
        ) + "</tr>"
        for service in insight["services"]
    ]
    flaky_rows = [
        "<tr>"
        f"<td><code>{_cell(item['test_id'])}</code></td>"
        f"<td>{item['passed_runs']}</td><td>{item['failed_runs']}</td>"
        "<td>Potentially flaky</td></tr>"
        for item in data["flaky"]["candidates"]
    ]
    cards = (
        ("CI runs", insight["runs"]),
        ("Tests", counts["total"]),
        ("Passed", counts["passed"]),
        ("Failed", counts["failed"]),
        ("Skipped", counts["skipped"]),
    )
    card_html = "".join(
        f'<article><small>{html.escape(label)}</small><strong>{value}</strong></article>'
        for label, value in cards
    )
    return f"""<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>CI Triage dashboard snapshot</title>
<style>
body{{margin:0;background:#0b1220;color:#e8f0f6;font:15px/1.5 system-ui,sans-serif}}
main{{max-width:1440px;margin:auto;padding:36px}}h1{{margin:0;font-size:32px}}
p,small{{color:#9bb0c0}}section{{margin-top:26px;background:#152332;border:1px solid #304658;
border-radius:14px;padding:20px;overflow:auto}}.cards{{display:grid;grid-template-columns:repeat(5,1fr);
gap:12px;margin-top:24px}}article{{background:#1b2b3a;border-radius:10px;padding:16px}}
article small,article strong{{display:block}}article strong{{font-size:30px;color:#79dfb6}}
table{{border-collapse:collapse;width:100%;margin-top:14px}}th,td{{padding:10px;text-align:left;
border-bottom:1px solid #304658;vertical-align:top}}th{{color:#9bb0c0;font-size:11px}}
code{{color:#a8c9fa}}a{{color:#8dbaf4}}.empty{{color:#9bb0c0;padding:16px}}
@media(max-width:800px){{main{{padding:18px}}.cards{{grid-template-columns:repeat(2,1fr)}}}}
</style><main><h1>CI Triage</h1>
<p>Static snapshot exported from Query API · {html.escape(data['generated_at'])}</p>
<div class="cards">{card_html}</div>
<section><h2>Recent runs</h2><table><thead><tr><th>Service</th><th>Branch</th><th>Commit</th>
<th>Tests</th><th>Passed</th><th>Failed</th><th>Skipped</th><th>State</th><th>Link</th></tr></thead>
<tbody>{''.join(run_rows) or '<tr><td class="empty" colspan="9">No runs</td></tr>'}</tbody></table></section>
<section><h2>Per-service breakdown</h2><table><thead><tr><th>Service</th><th>Runs</th><th>Tests</th>
<th>Passed</th><th>Failed</th><th>Skipped</th><th>Classified failures</th></tr></thead>
<tbody>{''.join(service_rows) or '<tr><td class="empty" colspan="7">No service data</td></tr>'}</tbody></table></section>
<section><h2>Failure signatures</h2><table><thead><tr><th>Service</th><th>Test</th><th>Category</th>
<th>Confidence</th><th>Source</th><th>Fingerprint</th><th>Error</th></tr></thead>
<tbody>{''.join(failure_rows) or '<tr><td class="empty" colspan="7">No failures</td></tr>'}</tbody></table></section>
<section><h2>Flaky candidates</h2><table><thead><tr><th>Test</th><th>Passed runs</th><th>Failed runs</th>
<th>Assessment</th></tr></thead><tbody>{''.join(flaky_rows) or '<tr><td class="empty" colspan="4">No candidates</td></tr>'}</tbody></table></section>
</main></html>"""


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    insights = _get_json("/api/insights?days=90")
    runs = _get_json("/api/runs?days=90&limit=200")
    flaky = _get_json("/api/flaky?days=90")
    for run in runs:
        detail = _get_json(f"/api/runs/{run['run_id']}")
        run["failures"] = detail.get("failures", [])
    data = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "insights": insights,
        "runs": runs,
        "flaky": flaky,
    }
    data_path = OUTPUT_DIR / "dashboard-data.json"
    html_path = OUTPUT_DIR / "dashboard-snapshot.html"
    data_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    html_path.write_text(_render(data), encoding="utf-8")

    run_id = os.getenv("GITHUB_RUN_ID", "")
    attempt = os.getenv("GITHUB_RUN_ATTEMPT", "")
    current = [
        run for run in runs
        if (run.get("ci_metadata") or {}).get("run_id", "").startswith(f"{run_id}-{attempt}-")
    ]
    new_failures = _new_failure_signatures(runs, current)
    current_counts = {
        key: sum(run["test_counts"][key] for run in current)
        for key in ("passed", "failed", "skipped")
    }
    repository = os.getenv("GITHUB_REPOSITORY", "")
    server_url = os.getenv("GITHUB_SERVER_URL", "https://github.com")
    workflow_url = f"{server_url}/{repository}/actions/runs/{run_id}"
    categories = sorted(insights["categories"].items(), key=lambda item: item[1], reverse=True)
    summary = os.getenv("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a", encoding="utf-8") as output:
            output.write("## CI Triage dashboard snapshot\n\n")
            output.write(
                f"**This workflow attempt:** {len(current)} runs · "
                f"{current_counts['passed']} passed · {current_counts['failed']} failed · "
                f"{current_counts['skipped']} skipped\n\n"
            )
            output.write(
                f"**New failure signatures in this attempt:** {len(new_failures)}\n\n"
            )
            if new_failures:
                output.write(
                    "**New fingerprints:** "
                    + ", ".join(f"`{fingerprint}`" for fingerprint in new_failures[:5])
                    + "\n\n"
                )
            output.write("**Top failure categories (90 days):** ")
            output.write(", ".join(f"{name}: {count}" for name, count in categories[:5]) or "none")
            output.write("\n\n")
            output.write(
                f"**Flaky candidates:** {len(flaky['candidates'])} · "
                f"[Download dashboard snapshot artifact]({workflow_url})\n\n"
            )
            output.write(
                "The artifact includes a self-contained HTML dashboard, JSON data, "
                "and a PNG screenshot.\n"
            )
    print(f"Dashboard data and HTML written to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
