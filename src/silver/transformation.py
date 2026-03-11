"""
Silver layer – limpa e particiona os dados do Bronze em Parquet.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

SCHEMA: dict[str, str] = {
    "id": "string",
    "name": "string",
    "brewery_type": "string",
    "address_1": "string",
    "address_2": "string",
    "address_3": "string",
    "city": "string",
    "state_province": "string",
    "postal_code": "string",
    "country": "string",
    "longitude": "Float64",
    "latitude": "Float64",
    "phone": "string",
    "website_url": "string",
    "state": "string",
    "street": "string",
}

VALID_BREWERY_TYPES = {
    "micro", "nano", "regional", "brewpub", "large",
    "planning", "bar", "contract", "proprietor", "closed",
}


class SilverTransformation:

    def __init__(
        self,
        bronze_base: str = "/opt/airflow/data/bronze",
        silver_base: str = "/opt/airflow/data/silver",
    ):
        self.bronze_base = Path(bronze_base)
        self.silver_base = Path(silver_base)

    def _load_bronze(self, execution_date: str) -> pd.DataFrame:
        dt = datetime.strptime(execution_date, "%Y-%m-%d")
        path = (
            self.bronze_base
            / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
            / f"breweries_{execution_date}.json"
        )
        if not path.exists():
            raise FileNotFoundError(f"Bronze não encontrado: {path}")

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        return pd.DataFrame(data)

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        # Garante que todas as colunas do schema existem
        for col in SCHEMA:
            if col not in df.columns:
                df[col] = pd.NA

        df = df[[c for c in SCHEMA if c in df.columns]].copy()

        # Cast de tipos
        for col, dtype in SCHEMA.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                except Exception:
                    df[col] = df[col].astype("string")

        # Normaliza strings
        for col, dtype in SCHEMA.items():
            if dtype == "string" and col in df.columns:
                df[col] = df[col].str.strip()
                df[col] = df[col].replace({"": pd.NA, "None": pd.NA, "null": pd.NA})

        # brewery_type: lowercase e valida contra lista conhecida
        df["brewery_type"] = df["brewery_type"].str.lower().str.strip()
        invalid = ~df["brewery_type"].isin(VALID_BREWERY_TYPES) & df["brewery_type"].notna()
        if invalid.sum():
            logger.warning(f"{invalid.sum()} registros com brewery_type desconhecido → 'unknown'")
            df.loc[invalid, "brewery_type"] = "unknown"

        # Limpa telefone
        if "phone" in df.columns:
            df["phone"] = df["phone"].str.replace(r"[^\d+\-\(\) ]", "", regex=True)

        # Invalida coordenadas fora do range
        if "latitude" in df.columns:
            df.loc[df["latitude"].abs() > 90, "latitude"] = pd.NA
        if "longitude" in df.columns:
            df.loc[df["longitude"].abs() > 180, "longitude"] = pd.NA

        # Chaves de partição normalizadas
        df["country_partition"] = self._partition_key(df["country"])
        df["state_partition"] = self._partition_key(
            df.get("state_province", df.get("state"))
        )

        df["ingestion_timestamp"] = datetime.utcnow().isoformat()

        # Remove duplicatas
        before = len(df)
        df = df.drop_duplicates(subset=["id"])
        if len(df) < before:
            logger.warning(f"Removidas {before - len(df)} duplicatas")

        return df

    @staticmethod
    def _partition_key(series: pd.Series) -> pd.Series:
        return (
            series.fillna("unknown")
            .str.lower()
            .str.strip()
            .str.replace(r"[^a-z0-9]+", "_", regex=True)
            .str.strip("_")
        )

    def _save(self, df: pd.DataFrame, execution_date: str) -> list[str]:
        dt = datetime.strptime(execution_date, "%Y-%m-%d")
        base = self.silver_base / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"

        paths = []
        for (country, state), group in df.groupby(
            ["country_partition", "state_partition"], observed=True
        ):
            out_path = base / f"country={country}" / f"state={state}"
            out_path.mkdir(parents=True, exist_ok=True)
            file_path = out_path / "data.parquet"
            group.drop(columns=["country_partition", "state_partition"]).to_parquet(
                file_path, index=False, engine="pyarrow", compression="snappy"
            )
            paths.append(str(file_path))

        logger.info(f"Silver: {len(paths)} partições escritas")
        return paths

    def run(self, execution_date: str, bronze_meta: dict | None = None) -> dict[str, Any]:
        logger.info(f"Iniciando transformação Silver — {execution_date}")
        start = datetime.utcnow()

        df = self._load_bronze(execution_date)
        df = self._clean(df)
        paths = self._save(df, execution_date)

        meta = {
            "execution_date": execution_date,
            "total_records": len(df),
            "partition_files": len(paths),
            "duration_seconds": (datetime.utcnow() - start).total_seconds(),
            "null_counts": df.isnull().sum().to_dict(),
        }
        logger.info(f"Silver concluído: {meta}")
        return meta
