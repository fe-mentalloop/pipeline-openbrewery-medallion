"""
Suite de testes do Pipeline Medallion de Cervejarias.
Cobre: cliente da API, ingestão Bronze, transformação Silver, agregação Gold e checks de DQ.
Todos os testes usam mocks para evitar chamadas reais à API.
"""

import json
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ─────────────────────────────────────────────────────────────────────────────
# Dados de exemplo usados em todos os testes
# ─────────────────────────────────────────────────────────────────────────────

SAMPLE_BREWERIES = [
    {
        "id": "brew-001",
        "name": "Acme Brewing Co",
        "brewery_type": "micro",
        "address_1": "123 Main St",
        "address_2": None,
        "address_3": None,
        "city": "Portland",
        "state_province": "Oregon",
        "postal_code": "97201",
        "country": "United States",
        "longitude": "-122.6787",
        "latitude": "45.5231",
        "phone": "5031234567",
        "website_url": "https://acme.beer",
        "state": "Oregon",
        "street": "123 Main St",
    },
    {
        "id": "brew-002",
        "name": "Berlin Craft Bier",
        "brewery_type": "brewpub",
        "address_1": "Berliner Str. 99",
        "address_2": None,
        "address_3": None,
        "city": "Berlin",
        "state_province": "Berlin",
        "postal_code": "10115",
        "country": "Germany",
        "longitude": "13.4050",
        "latitude": "52.5200",
        "phone": None,
        "website_url": None,
        "state": "Berlin",
        "street": "Berliner Str. 99",
    },
    {
        "id": "brew-003",
        "name": "São Paulo Micro",
        "brewery_type": "nano",
        "address_1": "Av. Paulista 1000",
        "address_2": None,
        "address_3": None,
        "city": "São Paulo",
        "state_province": "São Paulo",
        "postal_code": "01310-000",
        "country": "Brazil",
        "longitude": "-46.6333",
        "latitude": "-23.5505",
        "phone": "11987654321",
        "website_url": "https://spmicro.com.br",
        "state": "São Paulo",
        "street": "Av. Paulista 1000",
    },
]


@pytest.fixture
def temp_dir():
    """Cria um diretório temporário para cada teste, removido ao final."""
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def sample_df():
    return pd.DataFrame(SAMPLE_BREWERIES)


# ─────────────────────────────────────────────────────────────────────────────
# Testes do Cliente da API
# ─────────────────────────────────────────────────────────────────────────────

class TestBreweryAPIClient:
    def _get_client(self):
        from src.bronze.ingestion import BreweryAPIClient
        return BreweryAPIClient(page_size=2)

    def test_fetch_page_retorna_lista(self):
        """fetch_page deve retornar uma lista de dicionários."""
        client = self._get_client()
        mock_resp = MagicMock()
        mock_resp.json.return_value = SAMPLE_BREWERIES[:2]
        mock_resp.raise_for_status.return_value = None

        with patch.object(client.session, "get", return_value=mock_resp):
            result = client.fetch_page(1)

        assert isinstance(result, list)
        assert len(result) == 2

    def test_fetch_all_pagina_corretamente(self):
        """fetch_all deve percorrer todas as páginas e consolidar os registros."""
        client = self._get_client()
        pages = [SAMPLE_BREWERIES[:2], SAMPLE_BREWERIES[2:], []]

        def mock_get(url, params=None, timeout=None):
            r = MagicMock()
            r.raise_for_status.return_value = None
            if "meta" in url:
                r.json.return_value = {"total": 3}
            else:
                page_num = (params or {}).get("page", 1)
                r.json.return_value = pages[min(page_num - 1, len(pages) - 1)]
            return r

        with patch.object(client.session, "get", side_effect=mock_get):
            result = client.fetch_all()

        assert len(result) == 3

    def test_pagina_vazia_encerra_paginacao(self):
        """Uma página vazia retornada pela API deve encerrar a paginação."""
        client = self._get_client()

        def mock_get(url, params=None, timeout=None):
            r = MagicMock()
            r.raise_for_status.return_value = None
            if "meta" in url:
                r.json.return_value = {"total": 10}
            else:
                r.json.return_value = []  # Simula API retornando lista vazia
            return r

        with patch.object(client.session, "get", side_effect=mock_get):
            result = client.fetch_all()

        assert result == []


# ─────────────────────────────────────────────────────────────────────────────
# Testes da Ingestão Bronze
# ─────────────────────────────────────────────────────────────────────────────

class TestBronzeIngestion:
    def test_run_cria_arquivo_json(self, temp_dir):
        """A ingestão deve criar um arquivo JSON com os dados da API."""
        from src.bronze.ingestion import BronzeIngestion

        ingestion = BronzeIngestion(base_path=str(temp_dir))

        with patch.object(ingestion.client, "fetch_all", return_value=SAMPLE_BREWERIES):
            result = ingestion.run("2024-06-15")

        assert result["total_records"] == 3
        assert Path(result["output_path"]).exists()

    def test_run_falha_quando_api_retorna_vazio(self, temp_dir):
        """A ingestão deve levantar ValueError quando a API não retorna registros."""
        from src.bronze.ingestion import BronzeIngestion

        ingestion = BronzeIngestion(base_path=str(temp_dir))

        with patch.object(ingestion.client, "fetch_all", return_value=[]):
            with pytest.raises(ValueError, match="não retornou registros"):
                ingestion.run("2024-06-15")

    def test_arquivo_de_metadata_criado(self, temp_dir):
        """Um arquivo _metadata.json deve ser criado junto com os dados."""
        from src.bronze.ingestion import BronzeIngestion

        ingestion = BronzeIngestion(base_path=str(temp_dir))

        with patch.object(ingestion.client, "fetch_all", return_value=SAMPLE_BREWERIES):
            ingestion.run("2024-06-15")

        meta_file = temp_dir / "year=2024" / "month=06" / "day=15" / "_metadata.json"
        assert meta_file.exists()
        with open(meta_file) as f:
            meta = json.load(f)
        assert meta["total_records"] == 3


# ─────────────────────────────────────────────────────────────────────────────
# Testes da Transformação Silver
# ─────────────────────────────────────────────────────────────────────────────

class TestSilverTransformation:
    def _setup(self, temp_dir: Path, execution_date: str = "2024-06-15"):
        """Cria o arquivo Bronze de teste e retorna uma instância do transformador."""
        from src.silver.transformation import SilverTransformation

        dt = datetime.strptime(execution_date, "%Y-%m-%d")
        bronze_path = temp_dir / "bronze" / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
        bronze_path.mkdir(parents=True)
        with open(bronze_path / f"breweries_{execution_date}.json", "w") as f:
            json.dump(SAMPLE_BREWERIES, f)

        return SilverTransformation(
            bronze_base=str(temp_dir / "bronze"),
            silver_base=str(temp_dir / "silver"),
        )

    def test_transformacao_processa_todos_registros(self, temp_dir):
        """A transformação deve processar todos os registros do Bronze."""
        transformer = self._setup(temp_dir)
        result = transformer.run("2024-06-15")
        assert result["total_records"] == 3

    def test_arquivos_parquet_criados(self, temp_dir):
        """A transformação deve criar ao menos um arquivo Parquet na camada Silver."""
        transformer = self._setup(temp_dir)
        transformer.run("2024-06-15")
        parquet_files = list((temp_dir / "silver").rglob("*.parquet"))
        assert len(parquet_files) >= 1

    def test_normaliza_chave_de_particao(self):
        """A chave de partição deve ser normalizada para lowercase com underscores."""
        from src.silver.transformation import SilverTransformation
        series = pd.Series(["United States", "São Paulo", None, "  New Zealand  "])
        result = SilverTransformation._normalise_partition_key(series)
        assert result.iloc[0] == "united_states"
        assert result.iloc[2] == "unknown"  # Nulo deve virar "unknown"

    def test_tipo_invalido_vira_unknown(self, temp_dir):
        """Tipos de cervejaria não reconhecidos devem ser substituídos por 'unknown'."""
        from src.silver.transformation import SilverTransformation

        bad = SAMPLE_BREWERIES.copy()
        bad[0] = {**bad[0], "brewery_type": "nave_espacial"}  # Tipo inválido proposital
        dt_str = "2024-06-15"
        dt = datetime.strptime(dt_str, "%Y-%m-%d")
        bronze_path = temp_dir / "bronze" / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
        bronze_path.mkdir(parents=True)
        with open(bronze_path / f"breweries_{dt_str}.json", "w") as f:
            json.dump(bad, f)

        transformer = SilverTransformation(
            bronze_base=str(temp_dir / "bronze"),
            silver_base=str(temp_dir / "silver"),
        )
        transformer.run(dt_str)

        parquet_files = list((temp_dir / "silver").rglob("*.parquet"))
        df = pd.concat([pd.read_parquet(p) for p in parquet_files])
        assert "nave_espacial" not in df["brewery_type"].values

    def test_duplicatas_sao_removidas(self, temp_dir):
        """Registros com IDs duplicados devem ser removidos na transformação Silver."""
        from src.silver.transformation import SilverTransformation

        duped = SAMPLE_BREWERIES + [SAMPLE_BREWERIES[0]]  # Adiciona duplicata proposital
        dt_str = "2024-06-15"
        dt = datetime.strptime(dt_str, "%Y-%m-%d")
        bronze_path = temp_dir / "bronze" / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
        bronze_path.mkdir(parents=True)
        with open(bronze_path / f"breweries_{dt_str}.json", "w") as f:
            json.dump(duped, f)

        transformer = SilverTransformation(
            bronze_base=str(temp_dir / "bronze"),
            silver_base=str(temp_dir / "silver"),
        )
        result = transformer.run(dt_str)
        assert result["total_records"] == 3  # Duplicata deve ter sido removida


# ─────────────────────────────────────────────────────────────────────────────
# Testes da Agregação Gold
# ─────────────────────────────────────────────────────────────────────────────

class TestGoldAggregation:
    def _setup(self, temp_dir: Path, execution_date: str = "2024-06-15"):
        """Executa Bronze e Silver para preparar o estado necessário para os testes Gold."""
        from src.silver.transformation import SilverTransformation
        from src.gold.aggregation import GoldAggregation

        dt = datetime.strptime(execution_date, "%Y-%m-%d")
        bronze_path = temp_dir / "bronze" / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
        bronze_path.mkdir(parents=True)
        with open(bronze_path / f"breweries_{execution_date}.json", "w") as f:
            json.dump(SAMPLE_BREWERIES, f)

        SilverTransformation(
            bronze_base=str(temp_dir / "bronze"),
            silver_base=str(temp_dir / "silver"),
        ).run(execution_date)

        return GoldAggregation(
            silver_base=str(temp_dir / "silver"),
            gold_base=str(temp_dir / "gold"),
        )

    def test_quatro_tabelas_criadas(self, temp_dir):
        """A agregação Gold deve criar exatamente 4 tabelas analíticas."""
        agg = self._setup(temp_dir)
        result = agg.run("2024-06-15")
        assert len(result["tables_created"]) == 4

    def test_contagens_sao_positivas(self, temp_dir):
        """Todas as contagens na tabela Gold principal devem ser maiores que zero."""
        agg = self._setup(temp_dir)
        agg.run("2024-06-15")
        dt = datetime.strptime("2024-06-15", "%Y-%m-%d")
        path = (
            temp_dir / "gold" / "breweries_by_type_country"
            / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
            / "data.parquet"
        )
        df = pd.read_parquet(path)
        assert (df["brewery_count"] > 0).all()

    def test_todos_paises_cobertos(self, temp_dir):
        """A tabela de agregação por país deve incluir todos os países dos dados de teste."""
        agg = self._setup(temp_dir)
        agg.run("2024-06-15")
        dt = datetime.strptime("2024-06-15", "%Y-%m-%d")
        path = (
            temp_dir / "gold" / "breweries_by_country"
            / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
            / "data.parquet"
        )
        df = pd.read_parquet(path)
        assert set(df["country"]) == {"United States", "Germany", "Brazil"}


# ─────────────────────────────────────────────────────────────────────────────
# Testes dos Checks de Qualidade de Dados
# ─────────────────────────────────────────────────────────────────────────────

class TestDataQualityChecker:
    def _setup(self, temp_dir: Path, execution_date: str = "2024-06-15"):
        """Executa o pipeline completo para preparar os dados de todos os checks."""
        from src.silver.transformation import SilverTransformation
        from src.gold.aggregation import GoldAggregation
        from src.utils.data_quality import DataQualityChecker

        dt = datetime.strptime(execution_date, "%Y-%m-%d")
        bronze_path = temp_dir / "bronze" / f"year={dt.year}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
        bronze_path.mkdir(parents=True)
        with open(bronze_path / f"breweries_{execution_date}.json", "w") as f:
            json.dump(SAMPLE_BREWERIES, f)

        SilverTransformation(
            bronze_base=str(temp_dir / "bronze"),
            silver_base=str(temp_dir / "silver"),
        ).run(execution_date)

        GoldAggregation(
            silver_base=str(temp_dir / "silver"),
            gold_base=str(temp_dir / "gold"),
        ).run(execution_date)

        return DataQualityChecker(
            bronze_base=str(temp_dir / "bronze"),
            silver_base=str(temp_dir / "silver"),
            gold_base=str(temp_dir / "gold"),
            min_expected_records=1,  # Mínimo reduzido para os dados de teste
        )

    def test_todos_checks_passam(self, temp_dir):
        """Todos os 7 checks de qualidade devem passar com um pipeline executado corretamente."""
        checker = self._setup(temp_dir)
        results = checker.run_all_checks("2024-06-15")
        failed = [r for r in results if not r["passed"]]
        assert failed == [], f"Checks que falharam: {failed}"

    def test_check_falha_quando_bronze_ausente(self, temp_dir):
        """O check de existência do arquivo Bronze deve falhar para uma data sem dados."""
        from src.utils.data_quality import DataQualityChecker
        checker = DataQualityChecker(
            bronze_base=str(temp_dir / "bronze"),
            silver_base=str(temp_dir / "silver"),
            gold_base=str(temp_dir / "gold"),
        )
        result = checker.check_bronze_file_exists("2099-01-01")  # Data futura sem dados
        assert not result["passed"]
