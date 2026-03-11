"""
Checks de qualidade de dados — roda após o Gold e loga o resultado de cada check.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class DataQualityChecker:

    def __init__(
        self,
        bronze_base: str = "/opt/airflow/data/bronze",
        silver_base: str = "/opt/airflow/data/silver",
        gold_base: str = "/opt/airflow/data/gold",
        min_expected_records: int = 5000,
    ):
        self.bronze_base = Path(bronze_base)
        self.silver_base = Path(silver_base)
        self.gold_base = Path(gold_base)
        self.min_expected_records = min_expected_records

    @staticmethod
    def _result(name: str, passed: bool, details: str) -> dict:
        return {"check_name": name, "passed": passed, "details": details}

    def _load_silver(self, execution_date: str) -> pd.DataFrame:
        dt = datetime.strptime(execution_date, "%Y-%m-%d")
        day = self.silver_base / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
        files = list(day.rglob("*.parquet"))
        if not files:
            return pd.DataFrame()
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    def _load_gold(self, table: str, execution_date: str) -> pd.DataFrame:
        dt = datetime.strptime(execution_date, "%Y-%m-%d")
        path = (
            self.gold_base / table
            / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
            / "data.parquet"
        )
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    def check_bronze_file_exists(self, execution_date: str) -> dict:
        dt = datetime.strptime(execution_date, "%Y-%m-%d")
        path = (
            self.bronze_base
            / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
            / f"breweries_{execution_date}.json"
        )
        return self._result("bronze_file_exists", path.exists(), str(path))

    def check_minimum_records(self, execution_date: str) -> dict:
        df = self._load_silver(execution_date)
        count = len(df)
        return self._result(
            "minimum_records",
            count >= self.min_expected_records,
            f"{count} registros (mínimo: {self.min_expected_records})",
        )

    def check_no_duplicate_ids(self, execution_date: str) -> dict:
        df = self._load_silver(execution_date)
        if df.empty:
            return self._result("no_duplicate_ids", False, "Sem dados Silver")
        dupes = df["id"].duplicated().sum()
        return self._result("no_duplicate_ids", dupes == 0, f"Duplicatas: {dupes}")

    def check_id_not_null(self, execution_date: str) -> dict:
        df = self._load_silver(execution_date)
        if df.empty:
            return self._result("id_not_null", False, "Sem dados Silver")
        nulls = df["id"].isnull().sum()
        return self._result("id_not_null", nulls == 0, f"IDs nulos: {nulls}")

    def check_brewery_type_values(self, execution_date: str) -> dict:
        valid = {
            "micro", "nano", "regional", "brewpub", "large",
            "planning", "bar", "contract", "proprietor", "closed", "unknown",
        }
        df = self._load_silver(execution_date)
        if df.empty:
            return self._result("brewery_type_values", False, "Sem dados Silver")
        invalid = df.loc[~df["brewery_type"].isin(valid), "brewery_type"].unique()
        return self._result(
            "brewery_type_values",
            len(invalid) == 0,
            f"Tipos inválidos: {list(invalid)}" if len(invalid) else "OK",
        )

    def check_gold_tables_exist(self, execution_date: str) -> dict:
        tables = [
            "breweries_by_type_country",
            "breweries_by_type_country_state",
            "breweries_by_country",
            "brewery_type_summary",
        ]
        missing = [t for t in tables if self._load_gold(t, execution_date).empty]
        return self._result(
            "gold_tables_exist",
            len(missing) == 0,
            f"Faltando: {missing}" if missing else "OK",
        )

    def check_gold_counts_positive(self, execution_date: str) -> dict:
        df = self._load_gold("breweries_by_type_country", execution_date)
        if df.empty:
            return self._result("gold_counts_positive", False, "Tabela não encontrada")
        negative = (df["brewery_count"] <= 0).sum()
        return self._result("gold_counts_positive", negative == 0, f"Contagens <= 0: {negative}")

    def run_all_checks(self, execution_date: str) -> list[dict[str, Any]]:
        checks = [
            self.check_bronze_file_exists,
            self.check_minimum_records,
            self.check_no_duplicate_ids,
            self.check_id_not_null,
            self.check_brewery_type_values,
            self.check_gold_tables_exist,
            self.check_gold_counts_positive,
        ]
        results = []
        for fn in checks:
            try:
                result = fn(execution_date)
            except Exception as exc:
                result = self._result(fn.__name__, False, f"Exceção: {exc}")
            status = "✅" if result["passed"] else "❌"
            logger.info(f"DQ {status} {result['check_name']}: {result['details']}")
            results.append(result)
        return results
