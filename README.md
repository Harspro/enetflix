// 
"""
BigQuery Table Management DAG module for terraform table migration.
"""
import logging
from copy import deepcopy
from datetime import timedelta
from typing import Optional
import pendulum
import util.constants as consts
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from dag_factory.terminus_dag_factory import add_tags
from google.cloud import bigquery
from util.miscutils import read_variable_or_file, read_yamlfile_env

logger = logging.getLogger(__name__)


class BigQueryTableConfig:
    """Handles BigQuery table metadata extraction and audit inserts."""

    def __init__(self, project_id: str):
        self.project_id = project_id

    @property
    def client(self):
        if not hasattr(self, '_client'):
            self._client = bigquery.Client(project=self.project_id)
        return self._client

    def get_table_config(self, table_fqn: str):
        """
        Fetch schema, partitioning, clustering and creation_time.
        creation_time is sourced from INFORMATION_SCHEMA.TABLES
        """
        try:
            project, dataset, table = table_fqn.split('.')
        except ValueError:
            raise ValueError('Expected format: project.dataset.table')

        table_obj = self.client.get_table(table_fqn)

        schema_json = {
            'columns': self._extract_fields(table_obj.schema)
        }

        metadata_json = {
            'partitioning': self._extract_partitioning(table_obj),
            'clustering': table_obj.clustering_fields or []
        }

        creation_query = f"""
        SELECT creation_time
        FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name = @table_name
        """
        job = self.client.query(
            creation_query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter('table_name', 'STRING', table)
                ]
            )
        )
        result = list(job.result())
        if not result:
            raise ValueError(f'Creation time not found for {table_fqn}')

        creation_time = result[0].creation_time
        return schema_json, metadata_json, creation_time

    def _extract_fields(self, fields):
        cols = []
        for field in fields:
            col = {
                'name': field.name,
                'type': field.field_type,
                'mode': field.mode,
            }
            if field.default_value_expression:
                col['defaultValueExpression'] = field.default_value_expression
            if field.field_type == 'RECORD' and field.fields:
                col['fields'] = self._extract_fields(field.fields)
            cols.append(col)
        return cols

    def _extract_partitioning(self, table):
        if table.time_partitioning:
            return {
                'type': table.time_partitioning.type_,
                'field': table.time_partitioning.field or '_PARTITIONTIME',
            }
        return {}

    def build_table_rows(self, tables: list):
        rows = {}
        for table_id in tables:
            schema, metadata, creation_time = self.get_table_config(table_id)
            rows[table_id] = {
                'table_id': table_id,
                'version': 0,
                'applied_script': 'terraform',
                'insert_timestamp': creation_time,
                'json_table_schema': schema,
                'metadata': metadata,
            }
        return rows

    def get_latest_version(self, audit_table: str, table_id: str) -> Optional[int]:
        query = f"""
        SELECT version
        FROM `{audit_table}`
        WHERE table_id = @table_id
        ORDER BY insert_timestamp DESC
        LIMIT 1
        """
        job = self.client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter('table_id', 'STRING', table_id)
                ]
            )
        )
        res = list(job.result())
        return res[0].version if res else None

    def insert_row(self, audit_table: str, row: dict):
        query = f"""
        INSERT INTO `{audit_table}`
        (table_id, version, applied_script, insert_timestamp, json_table_schema, metadata)
        VALUES
        (@table_id, @version, @applied_script, @insert_timestamp, @json_table_schema, @metadata)
        """
        self.client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter('table_id', 'STRING', row['table_id']),
                    bigquery.ScalarQueryParameter('version', 'INT64', row['version']),
                    bigquery.ScalarQueryParameter('applied_script', 'STRING', row['applied_script']),
                    bigquery.ScalarQueryParameter('insert_timestamp', 'TIMESTAMP', row['insert_timestamp']),
                    bigquery.ScalarQueryParameter('json_table_schema', 'JSON', row['json_table_schema']),
                    bigquery.ScalarQueryParameter('metadata', 'JSON', row['metadata']),
                ]
            )
        ).result()

    def apply_version_zero(self, rows: dict, table_id: str, audit_table: str):
        row = rows[table_id]
        latest = self.get_latest_version(audit_table, table_id)
        if latest is None:
            self.insert_row(audit_table, row)
        elif latest == 0:
            raise ValueError(f'Record already exists for version 0: {table_id}')
        else:
            logger.info(f'Skipping non-terraform history for {table_id}')


class BigQueryTableManager:
    """Airflow DAG factory for terraform table audit backfill."""

    def __init__(self, config_filename: str, config_dir: str = None):
        self.gcp_config = read_variable_or_file(consts.GCP_CONFIG)
        self.deploy_env = self.gcp_config[consts.DEPLOYMENT_ENVIRONMENT_NAME]
        self.config_dir = config_dir or f"{settings.DAGS_FOLDER}/config"
        self.job_config = read_yamlfile_env(f'{self.config_dir}/{config_filename}', self.deploy_env)

        self.default_args = {
            'owner': 'team-centaurs',
            'retries': 2,
            'retry_delay': timedelta(minutes=1),
        }

    def create_dag(self, dag_id: str, config: dict) -> DAG:
        dag = DAG(
            dag_id=dag_id,
            default_args=deepcopy(self.default_args),
            schedule=None,
            start_date=pendulum.today('America/Toronto').subtract(days=1),
            catchup=False,
            max_active_runs=1,
            is_paused_upon_creation=True,
        )

        with dag:
            start = EmptyOperator(task_id='start')
            end = EmptyOperator(task_id='end')

            manager = BigQueryTableConfig(config['project_id'])
            tables = [t.replace('{env}', self.deploy_env) for t in config['table_name']]
            rows = manager.build_table_rows(tables)

            for table_id in tables:
                task = PythonOperator(
                    task_id=f'audit_{table_id.replace('.', '_')}',
                    python_callable=manager.apply_version_zero,
                    op_args=[rows, table_id, config['audit_table']],
                )
                start >> task >> end

        return add_tags(dag)

    def create_dags(self) -> dict:
        return {job: self.create_dag(job, cfg) for job, cfg in self.job_config.items()}
