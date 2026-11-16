#!/usr/bin/env python3
"""Render the generated static dashboard snapshot to PNG."""

import os
from pathlib import Path

from playwright.sync_api import sync_playwright


reports = Path(os.getenv("REPORTS_DIR", "reports"))
snapshot = (reports / "dashboard-snapshot.html").resolve()
output = reports / "dashboard-snapshot.png"

with sync_playwright() as playwright:
    browser = playwright.chromium.launch()
    page = browser.new_page(viewport={"width": 1440, "height": 1000}, device_scale_factor=1)
    page.goto(snapshot.as_uri(), wait_until="load")
    page.screenshot(path=str(output), full_page=True)
    browser.close()

print(f"Dashboard screenshot written to {output}")
