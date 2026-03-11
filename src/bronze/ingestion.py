"""
Bronze layer – ingesta dados brutos da API e salva como JSON.
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class BreweryAPIClient:

    BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
    DEFAULT_PAGE_SIZE = 200
    MAX_PAGES = 500

    def __init__(self, page_size: int = DEFAULT_PAGE_SIZE):
        self.page_size = page_size
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers.update({
            "Accept": "application/json",
            "User-Agent": "BEES-DataEngineering/1.0",
        })
        return session

    def fetch_page(self, page: int) -> list[dict]:
        params = {"page": page, "per_page": self.page_size}
        resp = self.session.get(self.BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def fetch_total_count(self) -> int:
        try:
            resp = self.session.get(f"{self.BASE_URL}/meta", timeout=15)
            resp.raise_for_status()
            return int(resp.json().get("total", 0))
        except Exception:
            logger.warning("Não foi possível buscar o total via /meta, paginando até acabar.")
            return -1

    def fetch_all(self) -> list[dict]:
        total = self.fetch_total_count()
        if total > 0:
            import math
            expected_pages = math.ceil(total / self.page_size)
            logger.info(f"Total: {total} cervejarias | Páginas esperadas: {expected_pages}")
        else:
            expected_pages = self.MAX_PAGES

        all_records: list[dict] = []
        page = 1

        while page <= expected_pages:
            try:
                records = self.fetch_page(page)
            except requests.HTTPError as exc:
                logger.error(f"Erro HTTP na página {page}: {exc}")
                raise

            if not records:
                break

            all_records.extend(records)
            logger.info(f"Página {page}: {len(records)} registros (acumulado: {len(all_records)})")

            if len(records) < self.page_size:
                break

            page += 1
            time.sleep(0.1)

        logger.info(f"Paginação concluída. Total: {len(all_records)} registros")
        return all_records


class BronzeIngestion:

    def __init__(self, base_path: str = "/opt/airflow/data/bronze"):
        self.base_path = Path(base_path)
        self.client = BreweryAPIClient()

    def _output_path(self, execution_date: str) -> Path:
        dt = datetime.strptime(execution_date, "%Y-%m-%d")
        path = self.base_path / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def run(self, execution_date: str) -> dict[str, Any]:
        logger.info(f"Iniciando ingestão Bronze — {execution_date}")
        start = datetime.utcnow()

        records = self.client.fetch_all()

        if not records:
            raise ValueError("API não retornou registros.")

        output_path = self._output_path(execution_date)
        file_path = output_path / f"breweries_{execution_date}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2, default=str)

        meta = {
            "execution_date": execution_date,
            "ingestion_timestamp": start.isoformat(),
            "total_records": len(records),
            "source_url": BreweryAPIClient.BASE_URL,
            "output_path": str(file_path),
            "duration_seconds": (datetime.utcnow() - start).total_seconds(),
        }

        with open(output_path / "_metadata.json", "w") as f:
            json.dump(meta, f, indent=2, default=str)

        logger.info(f"Bronze concluído: {len(records)} registros em {file_path}")
        return meta
