# bees-brewery-pipeline

Pipeline de dados para ingestão e processamento das cervejarias da [Open Brewery DB API](https://www.openbrewerydb.org/), seguindo arquitetura medallion (Bronze → Silver → Gold), orquestrado com Airflow e containerizado com Docker.

---

## Estrutura

```
bees-brewery-pipeline/
├── dags/
│   └── brewery_pipeline_dag.py
├── src/
│   ├── bronze/ingestion.py
│   ├── silver/transformation.py
│   ├── gold/
│   │   ├── aggregation.py
│   │   └── export_csv.py
│   └── utils/
│       ├── data_quality.py
│       └── alerts.py
├── tests/
│   └── test_pipeline.py
├── data/                        ← gerado após rodar o pipeline
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── csv/
├── docker-compose.yml
└── requirements.txt
```

---

## Como rodar

### Pré-requisitos
- Docker Desktop instalado e rodando
- ~4 GB de RAM disponível

```bash
# 1. Sobe os containers
docker compose up --build -d

# 2. Inicializa o banco e cria o usuário admin (só na primeira vez)
docker compose run --rm airflow-init

# 3. Acessa a interface
# http://localhost:8080  →  admin / admin
```

Para disparar o pipeline manualmente:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger brewery_medallion_pipeline
```

Após a execução, a pasta `data/` aparece no projeto com os arquivos gerados:

```
data/
├── bronze/   → JSON bruto da API
├── silver/   → Parquet particionado por país/estado
├── gold/     → Parquet com as agregações
└── csv/      → CSVs prontos para análise
```

### Rodando os testes

```bash
docker compose exec airflow-scheduler \
  pytest /opt/airflow/tests/ -v
```

---

## Arquitetura

```
API (paginada)
     │
     ▼
  BRONZE        JSON bruto, particionado por data
     │
     ▼
  SILVER        Parquet/Snappy, particionado por país e estado
     │           limpeza de tipos, validação de coordenadas,
     │           normalização de brewery_type, deduplicação
     ▼
   GOLD          4 tabelas analíticas agregadas
     │
     ▼
   CSV           exportação para análise direta
     │
     ▼
  DQ checks     7 verificações automáticas de qualidade
```

O DAG roda diariamente às 06:00 UTC com retry exponencial (3x, até 30 min entre tentativas).

---

## Camada Silver — transformações aplicadas

| Transformação | Motivo |
|---|---|
| Cast de tipos via schema | `longitude` como Float64, não string |
| Normalização de strings | strips e strings vazias → nulo |
| Validação de `brewery_type` | valores fora da lista oficial → `"unknown"` |
| Limpeza de telefone | remove caracteres não numéricos |
| Validação de coordenadas | lat fora de ±90° e lon fora de ±180° → nulo |
| Chaves de partição | `"United States"` → `"united_states"` |
| Deduplicação | remove duplicatas por `id` |

---

## Camada Gold — tabelas geradas

| Tabela | Descrição |
|---|---|
| `breweries_by_type_country` | contagem por tipo × país |
| `breweries_by_type_country_state` | contagem por tipo × país × estado |
| `breweries_by_country` | total por país, tipos presentes, cidades cobertas |
| `brewery_type_summary` | visão global por tipo com métricas de completude |

---

## Decisões técnicas

**Pandas em vez de PySpark** — a API retorna ~8k registros. PySpark tem overhead de 15-30s só pra subir a JVM, o que não faz sentido pra esse volume. Se o dataset crescer pra dezenas de milhões de linhas, a migração é direta: trocar os DataFrames e ajustar os writes para `spark.write.parquet`.

**Parquet + Snappy** — colunar, permite leitura seletiva de colunas, e Snappy tem bom custo-benefício entre compressão e velocidade de leitura/escrita.

**Particionamento por país/estado** — queries analíticas quase sempre filtram por localização. Particionar evita full scan.

**LocalExecutor** — suficiente pra esse volume e facilita o setup local. Em produção trocaria por CeleryExecutor ou KubernetesExecutor.

---

## Monitoramento e alertas

Checks de qualidade automáticos após cada execução:

- Arquivo Bronze existe para a data
- Mínimo de 5.000 registros na Silver
- Sem IDs duplicados
- Sem IDs nulos
- Tipos de cervejaria válidos
- Todas as tabelas Gold presentes
- Contagens > 0

Alertas via Slack ou e-mail, configuráveis em `.env`:

```env
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
SMTP_HOST=smtp.gmail.com
SMTP_USER=seu@email.com
SMTP_PASSWORD=senha
ALERT_EMAIL_RECIPIENTS=time@empresa.com
```

---

## Serviços de nuvem

A solução roda localmente por padrão para facilitar reprodutibilidade. Em produção, a troca seria apenas nos caminhos base de cada camada:

| Componente | Local | Produção |
|---|---|---|
| Data Lake | `./data/` | S3 / GCS / ADLS |
| Orquestração | Airflow (Docker) | MWAA / Cloud Composer |
| Processamento | Pandas | EMR / Dataproc / Databricks |

Para apontar para S3, por exemplo, basta instalar `s3fs` e trocar o `base_path`:

```python
# de:
BronzeIngestion(base_path="/opt/airflow/data/bronze")
# para:
BronzeIngestion(base_path="s3://seu-bucket/brewery/bronze")
```

Credenciais de nuvem nunca devem ir no repositório — use variáveis de ambiente ou um serviço de secrets (AWS Secrets Manager, GCP Secret Manager, Azure Key Vault).
