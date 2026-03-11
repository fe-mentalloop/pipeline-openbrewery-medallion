"""
Gold layer – agrega os dados Silver em visões analíticas.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class GoldAggregation:

    def __init__(
        self,
        silver_base: str = "/opt/airflow/data/silver",
        gold_base: str = "/opt/airflow/data/gold",
    ):
        self.silver_base = Path(silver_base)
        self.gold_base = Path(gold_base)

    def _load_silver(self, execution_date: str) -> pd.DataFrame:
        dt = datetime.strptime(execution_date, "%Y-%m-%d")
        silver_day = (
            self.silver_base
            / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
        )

        files = list(silver_day.rglob("*.parquet"))
        if not files:
            raise FileNotFoundError(f"Nenhum arquivo Silver encontrado em: {silver_day}")

        dfs = []
        for f in files:
            parts = f.parts
            country = next((p.split("=")[1] for p in parts if p.startswith("country=")), "unknown")
            state = next((p.split("=")[1] for p in parts if p.startswith("state=")), "unknown")
            df = pd.read_parquet(f)
            df["country_partition"] = country
            df["state_partition"] = state
            dfs.append(df)

        full = pd.concat(dfs, ignore_index=True)
        logger.info(f"Silver carregado: {len(full)} registros ({len(files)} arquivos)")
        return full

    def _save(self, df: pd.DataFrame, name: str, execution_date: str) -> str:
        dt = datetime.strptime(execution_date, "%Y-%m-%d")
        path = (
            self.gold_base / name
            / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
        )
        path.mkdir(parents=True, exist_ok=True)
        file_path = path / "data.parquet"
        df.to_parquet(file_path, index=False, engine="pyarrow", compression="snappy")
        logger.info(f"Gold '{name}': {len(df)} linhas → {file_path}")
        return str(file_path)

    def run(self, execution_date: str, silver_meta: dict | None = None) -> dict[str, Any]:
        logger.info(f"Iniciando agregação Gold — {execution_date}")
        start = datetime.utcnow()

        df = self._load_silver(execution_date)

        tables = {
            "breweries_by_type_country": (
                df.groupby(["brewery_type", "country"], observed=True, dropna=False)
                .agg(brewery_count=("id", "count"))
                .reset_index()
                .sort_values("brewery_count", ascending=False)
            ),
            "breweries_by_type_country_state": (
                df.groupby(["brewery_type", "country", "state_province"], observed=True, dropna=False)
                .agg(brewery_count=("id", "count"))
                .reset_index()
                .sort_values("brewery_count", ascending=False)
            ),
            "breweries_by_country": (
                df.groupby("country", observed=True, dropna=False)
                .agg(
                    total_breweries=("id", "count"),
                    brewery_types=("brewery_type", lambda x: list(x.dropna().unique())),
                    cities_covered=("city", "nunique"),
                )
                .reset_index()
                .sort_values("total_breweries", ascending=False)
            ),
            "brewery_type_summary": (
                df.groupby("brewery_type", observed=True, dropna=False)
                .agg(
                    total_count=("id", "count"),
                    countries_present=("country", "nunique"),
                    has_website=("website_url", lambda x: x.notna().sum()),
                    has_phone=("phone", lambda x: x.notna().sum()),
                )
                .reset_index()
                .sort_values("total_count", ascending=False)
            ),
        }

        for name, agg_df in tables.items():
            self._save(agg_df, name, execution_date)

        meta = {
            "execution_date": execution_date,
            "source_records": len(df),
            "tables_created": list(tables.keys()),
            "duration_seconds": (datetime.utcnow() - start).total_seconds(),
        }
        logger.info(f"Gold concluído: {meta}")
        return meta
