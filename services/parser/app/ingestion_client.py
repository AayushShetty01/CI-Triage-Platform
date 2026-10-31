import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class IngestionError(Exception):
    """Base error for Ingestion client calls."""


class IngestionTransientError(IngestionError):
    """Temporary failure (network, 502/503/504) that can be retried."""


class IngestionPermanentError(IngestionError):
    """Non-retryable failure (404, 401, 403, etc.)."""


class IngestionClient:
    def __init__(
        self,
        base_url: str = "http://localhost:8001",
        internal_key: str = "dev-internal-key-change-me",
        timeout: float = 30.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.internal_key = internal_key
        self.timeout = timeout
        self._client = client

    async def get_raw_xml(self, raw_ref: str) -> bytes:
        if raw_ref.startswith("http://") or raw_ref.startswith("https://"):
            url = raw_ref
        elif raw_ref.startswith("/"):
            url = f"{self.base_url}{raw_ref}"
        else:
            url = f"{self.base_url}/{raw_ref}"

        headers = {
            "X-Internal-Key": self.internal_key,
            "Accept": "application/xml, text/xml, */*",
        }

        should_close = False
        client = self._client
        if client is None:
            client = httpx.AsyncClient(timeout=self.timeout)
            should_close = True

        try:
            response = await client.get(url, headers=headers)
            if response.status_code == 200:
                return response.content
            elif response.status_code in (502, 503, 504, 429):
                raise IngestionTransientError(
                    f"Ingestion service temporarily unavailable (HTTP {response.status_code})"
                )
            elif response.status_code in (401, 403):
                raise IngestionPermanentError(
                    f"Authentication failure calling Ingestion (HTTP {response.status_code})"
                )
            elif response.status_code == 404:
                raise IngestionPermanentError(
                    f"Run report not found at Ingestion (HTTP 404: {url})"
                )
            else:
                raise IngestionTransientError(
                    f"Unexpected HTTP status {response.status_code} from Ingestion: {response.text[:200]}"
                )
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout, httpx.NetworkError) as err:
            raise IngestionTransientError(f"Network error contacting Ingestion at {url}: {err}") from err
        except httpx.HTTPError as err:
            raise IngestionTransientError(f"HTTP error contacting Ingestion at {url}: {err}") from err
        finally:
            if should_close:
                await client.aclose()
