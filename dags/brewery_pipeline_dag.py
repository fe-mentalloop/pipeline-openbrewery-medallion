"""
BEES Data Engineering – DAG do Pipeline de Cervejarias
Arquitetura Medallion: Bronze → Silver → Gold → CSV
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import logging

logger = logging.getLogger(__name__)

# Argumentos padrão aplicados a todas as tasks do DAG
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
}

dag = DAG(
    dag_id="brewery_medallion_pipeline",
    default_args=default_args,
    description="Pipeline de cervejarias: API → Bronze → Silver → Gold → CSV",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bees", "brewery", "medallion"],
)


def ingest_bronze(**context):
    """Ingere os dados brutos da Open Brewery DB API na camada Bronze."""
    from src.bronze.ingestion import BronzeIngestion
    from src.utils.alerts import send_alert
    try:
        ingestion = BronzeIngestion()
        result = ingestion.run(execution_date=context["ds"])
        logger.info(f"Ingestão Bronze concluída. Registros: {result['total_records']}")
        context["ti"].xcom_push(key="bronze_result", value=result)
        return result
    except Exception as e:
        send_alert(f"Falha na ingestão Bronze: {str(e)}", level="CRITICAL")
        raise


def transform_silver(**context):
    """Transforma os dados Bronze na camada Silver (Parquet, particionado)."""
    from src.silver.transformation import SilverTransformation
    from src.utils.alerts import send_alert
    try:
        bronze_result = context["ti"].xcom_pull(key="bronze_result", task_ids="bronze_ingestion")
        transformer = SilverTransformation()
        result = transformer.run(execution_date=context["ds"], bronze_meta=bronze_result)
        logger.info(f"Transformação Silver concluída. Registros: {result['total_records']}")
        context["ti"].xcom_push(key="silver_result", value=result)
        return result
    except Exception as e:
        send_alert(f"Falha na transformação Silver: {str(e)}", level="CRITICAL")
        raise


def aggregate_gold(**context):
    """Agrega os dados Silver na camada Gold."""
    from src.gold.aggregation import GoldAggregation
    from src.utils.alerts import send_alert
    try:
        silver_result = context["ti"].xcom_pull(key="silver_result", task_ids="silver_transformation")
        aggregator = GoldAggregation()
        result = aggregator.run(execution_date=context["ds"], silver_meta=silver_result)
        logger.info(f"Agregação Gold concluída.")
        context["ti"].xcom_push(key="gold_result", value=result)
        return result
    except Exception as e:
        send_alert(f"Falha na agregação Gold: {str(e)}", level="CRITICAL")
        raise


def export_csv(**context):
    """Exporta as tabelas Gold para CSV na pasta /data/csv/."""
    from src.gold.export_csv import GoldExportCSV
    from src.utils.alerts import send_alert
    try:
        exporter = GoldExportCSV()
        result = exporter.run(execution_date=context["ds"])
        logger.info(f"Exportação CSV concluída: {result['total_tabelas']} arquivos")
        return result
    except Exception as e:
        send_alert(f"Falha na exportação CSV: {str(e)}", level="WARNING")
        raise


def run_data_quality_checks(**context):
    """Executa os checks de qualidade de dados em todas as camadas."""
    from src.utils.data_quality import DataQualityChecker
    from src.utils.alerts import send_alert
    checker = DataQualityChecker()
    results = checker.run_all_checks(execution_date=context["ds"])
    failed = [r for r in results if not r["passed"]]
    if failed:
        msg = f"Checks de qualidade falharam: {[r['check_name'] for r in failed]}"
        send_alert(msg, level="WARNING")
        logger.warning(msg)
    else:
        logger.info("Todos os checks de qualidade passaram.")
    return results


def pipeline_failure_callback(context):
    """Callback disparado quando qualquer task falha no pipeline."""
    from src.utils.alerts import send_alert
    task_id = context.get("task_instance").task_id
    dag_id = context.get("dag").dag_id
    exec_date = context.get("execution_date")
    send_alert(
        f"Pipeline FALHOU | DAG: {dag_id} | Task: {task_id} | Data: {exec_date}",
        level="CRITICAL",
    )


# ── Definição das Tasks ────────────────────────────────────────────────────────

start = EmptyOperator(task_id="start", dag=dag)

bronze_ingestion = PythonOperator(
    task_id="bronze_ingestion",
    python_callable=ingest_bronze,
    on_failure_callback=pipeline_failure_callback,
    dag=dag,
)

silver_transformation = PythonOperator(
    task_id="silver_transformation",
    python_callable=transform_silver,
    on_failure_callback=pipeline_failure_callback,
    dag=dag,
)

gold_aggregation = PythonOperator(
    task_id="gold_aggregation",
    python_callable=aggregate_gold,
    on_failure_callback=pipeline_failure_callback,
    dag=dag,
)

# Exporta CSVs logo após o Gold — roda mesmo se DQ falhar
export_csv_task = PythonOperator(
    task_id="export_csv",
    python_callable=export_csv,
    on_failure_callback=pipeline_failure_callback,
    dag=dag,
)

# Roda somente se todas as tasks anteriores tiverem sucesso
data_quality_checks = PythonOperator(
    task_id="data_quality_checks",
    python_callable=run_data_quality_checks,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# Roda independente do resultado dos checks de qualidade
end = EmptyOperator(
    task_id="end",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# ── Dependências entre Tasks ───────────────────────────────────────────────────
start >> bronze_ingestion >> silver_transformation >> gold_aggregation >> export_csv_task >> data_quality_checks >> end
