const $ = (selector) => document.querySelector(selector);
const state = { selectedRun: null };

function element(tag, className, value) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (value != null) node.textContent = String(value);
  return node;
}

function emptyRow(body, columns, message) {
  body.replaceChildren();
  const row = element("tr");
  const cell = element("td", "empty", message);
  cell.colSpan = columns;
  row.append(cell);
  body.append(row);
}

function badge(value) {
  return element("span", `badge ${value || "pending"}`, value || "pending");
}

function date(value) {
  return value ? new Date(value).toLocaleString() : "—";
}

function params() {
  const value = new URLSearchParams({ days: $("#window").value });
  const branch = $("#branch").value.trim();
  if (branch) value.set("branch", branch);
  return value;
}

async function json(url) {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) throw new Error(`${url}: HTTP ${response.status}`);
  return response.json();
}

function renderCategories(data) {
  const target = $("#categories");
  target.replaceChildren();
  const entries = Object.entries(data.categories).sort((a, b) => b[1] - a[1]);
  if (!entries.length) { target.append(element("p", "empty", "No classified failures in this window.")); return; }
  const max = Math.max(...entries.map((entry) => entry[1]));
  for (const [name, count] of entries) {
    const wrap = element("div");
    const title = element("div", "category-head");
    title.append(element("span", "", name), element("strong", "", count));
    const bar = element("div", "bar");
    const fill = element("span");
    fill.style.width = `${Math.round(count / max * 100)}%`;
    bar.append(fill); wrap.append(title, bar); target.append(wrap);
  }
}

function renderGroups(groups) {
  const target = $("#top-failures"); target.replaceChildren();
  if (!groups.length) { target.append(element("p", "empty", "No recurring failures yet.")); return; }
  for (const group of groups) {
    const row = element("div", "group");
    const detail = element("div", "name", group.example || group.fingerprint || "Unknown failure");
    detail.title = group.example || "";
    detail.append(element("small", "", group.category));
    row.append(detail, element("span", "count", group.count)); target.append(row);
  }
}

function renderRuns(runs) {
  const target = $("#run-list"); target.replaceChildren();
  if (!runs.length) { emptyRow(target, 6, "No CI runs have arrived yet."); return; }
  for (const run of runs) {
    const row = element("tr"); row.dataset.run = run.run_id; row.tabIndex = 0;
    row.append(element("td", "mono", run.run_id.slice(0, 12)), element("td", "", run.branch || "—"),
      element("td", "", run.total_tests ?? "—"), element("td", "", run.failed_tests ?? "—"));
    const status = element("td"); status.append(badge(run.state)); row.append(status);
    row.append(element("td", "muted", date(run.received_at || run.parsed_at)));
    row.addEventListener("click", () => openRun(run.run_id));
    row.addEventListener("keydown", (event) => { if (event.key === "Enter") openRun(run.run_id); });
    target.append(row);
  }
}

function renderFlaky(candidates) {
  const target = $("#flaky-list"); target.replaceChildren();
  if (!candidates.length) { emptyRow(target, 3, "No candidates in this window."); return; }
  for (const test of candidates) {
    const row = element("tr");
    row.append(element("td", "mono", test.test_id), element("td", "", test.passed_runs),
      element("td", "", test.failed_runs)); target.append(row);
  }
}

async function openRun(runId) {
  state.selectedRun = runId;
  const run = await json(`/api/runs/${encodeURIComponent(runId)}`);
  if (state.selectedRun !== runId) return;
  $("#run-detail").hidden = false;
  $("#detail-title").textContent = `Run ${runId.slice(0, 12)}`;
  $("#detail-subtitle").textContent = `${run.branch || "Unknown branch"} · ${run.state} · ${run.classification_counts.pending} pending`;
  const target = $("#failure-list"); target.replaceChildren();
  if (!run.failures.length) { emptyRow(target, 6, run.parsed_at ? "No failures in this run." : "Waiting for the Parser."); return; }
  for (const failure of run.failures) {
    const row = element("tr");
    const version = failure.rules_version ? `rules ${failure.rules_version}${failure.prompt_version ? ` · prompt ${failure.prompt_version}` : ""}` : "—";
    row.append(element("td", "mono", failure.test_id), element("td", "", failure.error_message || failure.error_type || "—"),
      element("td", "", failure.category || "—"));
    const status = element("td"); status.append(badge(failure.status)); row.append(status);
    row.append(element("td", "", failure.source || "—"), element("td", "", version)); target.append(row);
  }
}

async function refresh() {
  try {
    const [runs, overview, flaky] = await Promise.all([
      json("/api/runs"), json(`/api/insights?${params()}`), json(`/api/flaky?${params()}`)
    ]);
    $("#runs-count").textContent = overview.runs;
    $("#pending-count").textContent = overview.pending;
    $("#degraded-count").textContent = overview.degraded;
    $("#flaky-count").textContent = flaky.candidates.length;
    renderCategories(overview); renderGroups(overview.top_failures);
    renderRuns(runs); renderFlaky(flaky.candidates);
    if (state.selectedRun) await openRun(state.selectedRun);
    $("#connection").textContent = "Live";
    $("#connection").classList.remove("offline");
    $("#error").hidden = true;
  } catch (error) {
    $("#connection").textContent = "Unavailable";
    $("#connection").classList.add("offline");
    $("#error").textContent = `Unable to load current data: ${error.message}`;
    $("#error").hidden = false;
  }
}

$("#refresh").addEventListener("click", refresh);
$("#window").addEventListener("change", refresh);
$("#branch").addEventListener("change", refresh);
$("#close-detail").addEventListener("click", () => { state.selectedRun = null; $("#run-detail").hidden = true; });
refresh(); setInterval(refresh, 10000);
