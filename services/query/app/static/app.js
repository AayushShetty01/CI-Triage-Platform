const $ = (selector) => document.querySelector(selector);
const state = { selectedRun: null };
const palette = {
  assertion: "#79dfb6",
  timeout: "#a8a0fb",
  infrastructure: "#eec17b",
  flaky: "#ec99ac",
  unknown: "#91a8bc"
};

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

function updateNavigation() {
  const sections = { "#overview": "Overview", "#runs": "CI runs", "#insights": "Insights", "#flaky": "Flaky signals" };
  const selected = sections[location.hash] ? location.hash : "#overview";
  for (const link of document.querySelectorAll(".nav-links a")) {
    const active = link.getAttribute("href") === selected;
    link.classList.toggle("active", active);
    if (active) link.setAttribute("aria-current", "page");
    else link.removeAttribute("aria-current");
  }
  $("#current-section").textContent = sections[selected];
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
  const total = entries.reduce((sum, entry) => sum + entry[1], 0);
  $("#classified-total").textContent = total;
  $("#category-donut").setAttribute("aria-label", `${total} classified failures by category`);
  if (!entries.length) {
    $("#category-donut").style.background = "#304655";
    target.append(element("p", "empty", "No classified failures in this window."));
    return;
  }
  let offset = 0;
  const slices = entries.map(([name, count]) => {
    const start = offset;
    offset += count / total * 100;
    return `${palette[name] || palette.unknown} ${start}% ${offset}%`;
  });
  $("#category-donut").style.background = `conic-gradient(${slices.join(", ")})`;
  const max = Math.max(...entries.map((entry) => entry[1]));
  for (const [name, count] of entries) {
    const wrap = element("div");
    const title = element("div", "category-head");
    const label = element("span", "category-name");
    const swatch = element("span", "swatch");
    swatch.style.background = palette[name] || palette.unknown;
    label.append(swatch, document.createTextNode(name));
    title.append(label, element("strong", "", count));
    const bar = element("div", "bar");
    const fill = element("span");
    fill.style.width = `${Math.round(count / max * 100)}%`;
    fill.style.background = palette[name] || palette.unknown;
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
    detail.append(element("small", "", `${group.category} · ${group.fingerprint || "unfingerprinted"}`));
    row.append(detail, element("span", "count", group.count)); target.append(row);
  }
}

function renderRuns(runs) {
  const target = $("#run-list"); target.replaceChildren();
  if (!runs.length) { emptyRow(target, 10, "No CI runs match this window and branch."); return; }
  for (const run of runs) {
    const row = element("tr"); row.dataset.run = run.run_id;
    const runCell = element("td");
    const link = element("button", "run-link", run.run_id.slice(0, 12));
    link.type = "button";
    link.setAttribute("aria-label", `View run ${run.run_id}`);
    link.addEventListener("click", (event) => { event.stopPropagation(); openRun(run.run_id); });
    runCell.append(link);
    const ci = run.ci_metadata || {};
    const serviceCell = element("td");
    serviceCell.append(element("span", "branch-pill", ci.service || "—"));
    if (ci.workflow) serviceCell.append(element("small", "muted", ci.workflow));
    const branch = element("td");
    branch.append(element("span", "branch-pill", run.branch || "—"));
    branch.append(element("small", "mono muted", (run.commit_sha || "").slice(0, 8)));
    const counts = run.test_counts;
    if (counts.failed > 0) {
      row.classList.add("has-failures");
    }
    row.append(runCell, serviceCell, branch, element("td", "", counts.total),
      element("td", "", counts.passed), element("td", "", counts.failed),
      element("td", "", counts.skipped));
    const status = element("td"); status.append(badge(run.state)); row.append(status);
    row.append(element("td", "muted", date(run.received_at || run.parsed_at)));
    const arrow = element("td");
    const icon = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    icon.setAttribute("class", "table-arrow");
    const use = document.createElementNS("http://www.w3.org/2000/svg", "use");
    use.setAttribute("href", "#icon-arrow");
    icon.append(use); arrow.append(icon); row.append(arrow);
    row.addEventListener("click", () => openRun(run.run_id));
    target.append(row);
  }
}

function renderServices(services) {
  const target = $("#service-list");
  target.replaceChildren();
  if (!services.length) { emptyRow(target, 7, "No service runs match this window and branch."); return; }
  for (const service of services) {
    const row = element("tr");
    row.append(element("td", "", service.service), element("td", "", service.runs),
      element("td", "", service.tests), element("td", "", service.passed),
      element("td", "", service.failed), element("td", "", service.skipped),
      element("td", "", service.failures));
    target.append(row);
  }
}

function renderFlaky(candidates) {
  const target = $("#flaky-list"); target.replaceChildren();
  if (!candidates.length) { emptyRow(target, 4, "No candidates in this window."); return; }
  for (const test of candidates) {
    const row = element("tr");
    const assessment = element("td");
    assessment.append(element("span", "signal", "Potentially flaky"));
    row.append(element("td", "mono", test.test_id), element("td", "", test.passed_runs),
      element("td", "", test.failed_runs), assessment);
    target.append(row);
  }
}

async function openRun(runId, reveal = true) {
  state.selectedRun = runId;
  let run;
  try {
    run = await json(`/api/runs/${encodeURIComponent(runId)}`);
  } catch (error) {
    $("#error").textContent = `Unable to open run: ${error.message}`;
    $("#error").hidden = false;
    return;
  }
  if (state.selectedRun !== runId) return;
  $("#run-detail").hidden = false;
  $("#detail-title").textContent = `Run ${runId.slice(0, 12)}`;
  const ci = run.ci_metadata || {};
  $("#detail-subtitle").textContent = [
    run.branch || "Unknown branch",
    run.commit_sha || "Unknown commit",
    ci.workflow || "CI run",
    ci.repository || "",
    ci.actor ? `by ${ci.actor}` : "",
    run.state,
    `${run.classification_counts.pending} pending`,
  ].filter(Boolean).join(" · ");
  const ciLink = $("#detail-ci-link");
  ciLink.replaceChildren();
  if (ci.run_url) {
    const link = element("a", "", `Open GitHub Actions run${ci.service ? ` · ${ci.service}` : ""}`);
    link.href = ci.run_url;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    ciLink.append(link);
  }
  const target = $("#failure-list"); target.replaceChildren();
  if (!run.failures.length) {
    emptyRow(target, 7, run.parsed_at ? "No failures in this run." : "Waiting for the Parser.");
    if (reveal) $("#run-detail").scrollIntoView({ behavior: "smooth", block: "start" });
    return;
  }
  for (const failure of run.failures) {
    const row = element("tr");
    const version = failure.rules_version ? `rules ${failure.rules_version}${failure.prompt_version ? ` · prompt ${failure.prompt_version}` : ""}` : "—";
    row.append(element("td", "mono", failure.test_id), element("td", "", failure.error_message || failure.error_type || "—"),
      element("td", "", failure.category || "—"),
      element("td", "", failure.confidence == null ? "—" : `${Math.round(failure.confidence * 100)}%`));
    const status = element("td"); status.append(badge(failure.status)); row.append(status);
    row.append(element("td", "", failure.source || "—"), element("td", "", version)); target.append(row);
  }
  if (reveal) $("#run-detail").scrollIntoView({ behavior: "smooth", block: "start" });
}

async function refresh() {
  try {
    const [runs, overview, flaky] = await Promise.all([
      json(`/api/runs?${params()}`), json(`/api/insights?${params()}`), json(`/api/flaky?${params()}`)
    ]);
    $("#runs-count").textContent = overview.runs;
    $("#pending-count").textContent = overview.pending;
    $("#degraded-count").textContent = overview.degraded;
    $("#flaky-count").textContent = flaky.candidates.length;
    $("#tests-count").textContent = overview.test_counts.total;
    $("#passed-count").textContent = overview.test_counts.passed;
    $("#failed-count").textContent = overview.test_counts.failed;
    $("#skipped-count").textContent = overview.test_counts.skipped;
    const versions = overview.classifier_versions || [];
    $("#versions-note").textContent = versions.length > 1
      ? `This window mixes ${versions.length} classifier versions; compare categories with care.`
      : versions.length === 1
        ? `Classifier: rules ${versions[0].rules_version}, model ${versions[0].model || "none"}, prompt ${versions[0].prompt_version || "none"}.`
        : "Classification versions are preserved per result.";
    renderCategories(overview); renderGroups(overview.top_failures);
    renderServices(overview.services);
    renderRuns(runs); renderFlaky(flaky.candidates);
    if (state.selectedRun) await openRun(state.selectedRun, false);
    $("#connection").textContent = "Live";
    $("#connection").classList.remove("offline");
    $("#system-status").classList.remove("offline");
    $("#system-status-text").textContent = "Connected";
    $("#last-updated").textContent = `Updated ${new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
    $("#error").hidden = true;
  } catch (error) {
    $("#connection").textContent = "Unavailable";
    $("#connection").classList.add("offline");
    $("#system-status").classList.add("offline");
    $("#system-status-text").textContent = "Unavailable";
    $("#error").textContent = `Unable to load current data: ${error.message}`;
    $("#error").hidden = false;
  }
}

$("#refresh").addEventListener("click", refresh);
$("#window").addEventListener("change", refresh);
$("#branch").addEventListener("change", refresh);
$("#branch").addEventListener("keydown", (event) => { if (event.key === "Enter") refresh(); });
$("#close-detail").addEventListener("click", () => { state.selectedRun = null; $("#run-detail").hidden = true; });
window.addEventListener("hashchange", updateNavigation);
updateNavigation();
refresh(); setInterval(refresh, 10000);
