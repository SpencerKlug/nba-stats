"""Playwright-based fetcher for stats.ncaa.org to bypass Akamai bot protection."""

import time
from contextlib import contextmanager
from urllib.parse import urlencode

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth  # type: ignore[import-untyped]

from constants import NCAA_BASE, NCAA_HEADERS

# Delay between requests to avoid rate limiting
REQUEST_DELAY_SECONDS = 1.0


def _launch_browser(p, *, headless: bool = True):
    """Launch browser, preferring Chrome over Chromium for better stealth."""
    try:
        return p.chromium.launch(channel="chrome", headless=headless)
    except Exception:
        return p.chromium.launch(headless=headless)


@contextmanager
def ncaa_session(*, headless: bool = True):
    """Context manager that yields a fetch function for NCAA pages.

    If you still get Access Denied, try headless=False to run a visible browser
    (may help with some bot detection).
    """
    with Stealth().use_sync(sync_playwright()) as p:
        browser = _launch_browser(p, headless=headless)
        context = browser.new_context(
            user_agent=NCAA_HEADERS["User-Agent"],
            viewport={"width": 1920, "height": 1080},
            extra_http_headers={
                "Accept": NCAA_HEADERS["Accept"],
                "Accept-Language": NCAA_HEADERS["Accept-Language"],
                "Referer": NCAA_HEADERS["Referer"],
            },
        )
        page = context.new_page()

        # Visit homepage first to establish session/cookies (mimics real user flow)
        page.goto(f"{NCAA_BASE}/", wait_until="networkidle", timeout=30000)
        time.sleep(REQUEST_DELAY_SECONDS)

        def get_html(url: str, params: dict | None = None) -> str:
            time.sleep(REQUEST_DELAY_SECONDS)
            full_url = url
            if params:
                full_url = f"{url}?{urlencode(params)}"
            page.goto(full_url, wait_until="networkidle", timeout=30000)
            return page.content()

        try:
            yield get_html
        finally:
            browser.close()
