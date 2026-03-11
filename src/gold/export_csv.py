"""
Exportação CSV – Lê todas as tabelas Gold e salva como CSV na pasta /opt/airflow/data/csv/
"""

import logging
import glob
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

TABELAS = [
    "breweries_by_type_country",
    "breweries_by_type_country_state",
    "breweries_by_country",
    "brewery_type_summary",
]


class GoldExportCSV:
    """Exporta as tabelas Gold para CSV."""

    def __init__(
        self,
        gold_base: str = "/opt/airflow/data/gold",
        csv_base: str = "/opt/airflow/data/csv",
    ):
        self.gold_base = Path(gold_base)
        self.csv_base = Path(csv_base)

    def _load_tabela(self, tabela: str, execution_date: str):
        """Carrega o Parquet via PyArrow e retorna uma Arrow Table (sem conversão para Pandas)."""
        dt = datetime.strptime(execution_date, "%Y-%m-%d")
        padrao = str(
            self.gold_base / tabela
            / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
            / "data.parquet"
        )
        arquivos = glob.glob(padrao)
        if not arquivos:
            logger.warning(f"Tabela '{tabela}' não encontrada para {execution_date}")
            return None
        return pq.read_table(arquivos[0])

    def run(self, execution_date: str) -> dict[str, Any]:
        logger.info(f"Iniciando exportação CSV para {execution_date}")

        self.csv_base.mkdir(parents=True, exist_ok=True)

        exportados = []
        for tabela in TABELAS:
            arrow_table = self._load_tabela(tabela, execution_date)
            if arrow_table is None:
                continue

            caminho = self.csv_base / f"{tabela}.csv"

            # Converte cada coluna para string pura via PyArrow antes de escrever o CSV
            # Isso evita problemas com list<string>, Categorical, e outros tipos complexos
            import pyarrow.compute as pc
            import pyarrow as pa
            colunas_convertidas = []
            for i, col in enumerate(arrow_table.schema):
                arr = arrow_table.column(i)
                # Listas viram string separada por vírgula
                if pa.types.is_list(col.type):
                    arr = pa.array([
                        ", ".join(str(v) for v in val.as_py()) if val.is_valid else None
                        for val in arr
                    ])
                # Qualquer outro tipo não primitivo vira string
                elif not (
                    pa.types.is_string(col.type)
                    or pa.types.is_integer(col.type)
                    or pa.types.is_floating(col.type)
                    or pa.types.is_boolean(col.type)
                ):
                    arr = arr.cast(pa.string())
                colunas_convertidas.append(arr)

            tabela_limpa = pa.table(
                {col.name: colunas_convertidas[i] for i, col in enumerate(arrow_table.schema)}
            )

            # Escreve CSV diretamente via PyArrow — sem passar pelo Pandas
            import pyarrow.csv as pa_csv
            with open(caminho, "wb") as f:
                pa_csv.write_csv(tabela_limpa, f)

            logger.info(f"CSV exportado: {caminho} ({tabela_limpa.num_rows} linhas)")
            exportados.append(str(caminho))

        meta = {
            "execution_date": execution_date,
            "arquivos_exportados": exportados,
            "total_tabelas": len(exportados),
        }
        logger.info(f"Exportação CSV concluída: {meta}")
        return meta
