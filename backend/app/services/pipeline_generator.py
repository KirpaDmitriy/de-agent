import os
from typing import Dict, List, Any
from datetime import datetime, timedelta
from ..models.schemas import DataSourceConfig, DataRelationship
import logging

logger = logging.getLogger(__name__)

class PipelineGenerator:
    """Генератор ETL пайплайнов"""
    
    def ___init__(self, airflow_dags_path: str = "/opt/airflow/dags"):
        self.airflow_dags_path = airflow_dags_path
    
    def generate_airflow_dag(
        self, 
        sources: List[DataSourceConfig],
        recommendations: Dict[str, Any],
        relationships: List[DataRelationship],
        project_name: str
    ) -> str:
        """Генерация Airflow DAG"""
        
        dag_id = f"{project_name.lower().replace(' ', '_')}_etl"
        
        # Определяем расписание
        schedule = recommendations.get("etl_pipeline", {}).get("schedule", "0 2 * * *")
        if schedule.startswith("#"):
            schedule_interval = None
        else:
            schedule_interval = schedule
        
        dag_code = f'''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging

# DAG конфигурация
default_args = {{
    'owner': 'ai-assistant',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

dag = DAG(
    '{dag_id}',
    default_args=default_args,
    description='Auto-generated ETL pipeline',
    schedule_interval={f'"{schedule_interval}"' if schedule_interval else "None"},
    catchup=False,
    tags=['auto-generated', 'etl'],
)

# Функции для извлечения данных
'''
        
        # Добавляем функции извлечения для каждого источника
        for source in sources:
            if source.type.value == "csv":
                dag_code += self._generate_csv_extract_function(source)
            elif source.type.value == "postgresql":
                dag_code += self._generate_postgres_extract_function(source)
            elif source.type.value == "json":
                dag_code += self._generate_json_extract_function(source)
        
        # Функция трансформации
        dag_code += self._generate_transform_function(sources, relationships, recommendations)
        
        # Функция загрузки
        dag_code += self._generate_load_function(recommendations)
        
        # Определение задач
        dag_code += f'''

# Задачи DAG
extract_tasks = []
'''
        
        # Создаем задачи извлечения
        for i, source in enumerate(sources):
            task_id = f"extract_{source.id}"
            dag_code += f'''
{task_id} = PythonOperator(
    task_id='{task_id}',
    python_callable=extract_{source.id},
    dag=dag,
)
extract_tasks.append({task_id})
'''
        
        # Задача трансформации
        dag_code += '''
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Задача загрузки
load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Определение зависимостей
extract_tasks >> transform_task >> load_task
'''
        
        return dag_code
    
    def _generate_csv_extract_function(self, source: DataSourceConfig) -> str:
        """Генерация функции извлечения из CSV"""
        return f'''
def extract_{source.id}(**context):
    """Extract data from CSV source: {source.name}"""
    try:
        import pandas as pd
        
        df = pd.read_csv(
            '{source.config.get("file_path", "")}',
            delimiter='{source.config.get("delimiter", ",")}',
            encoding='{source.config.get("encoding", "utf-8")}'
        )
        
        # Сохраняем во временное хранилище (XCom или файл)
        df.to_parquet(f'/tmp/{source.id}_data.parquet', index=False)
        
        logging.info(f"Extracted {{len(df)}} rows from {source.name}")
        return f'/tmp/{source.id}_data.parquet'
        
    except Exception as e:
        logging.error(f"Error extracting from {source.name}: {{str(e)}}")
        raise
'''
    
    def _generate_postgres_extract_function(self, source: DataSourceConfig) -> str:
        """Генерация функции извлечения из PostgreSQL"""
        return f'''
def extract_{source.id}(**context):
    """Extract data from PostgreSQL source: {source.name}"""
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import pandas as pd
        
        # Используем Airflow connection или создаем новое подключение
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Альтернативный способ подключения с явными параметрами
        # connection_string = "postgresql://{source.config.get('username')}:{source.config.get('password')}@{source.config.get('host')}:{source.config.get('port', 5432)}/{source.config.get('database')}"
        
        query = "SELECT * FROM {source.config.get('table')}"
        df = pg_hook.get_pandas_df(sql=query)
        
        # Сохраняем во временное хранилище
        df.to_parquet(f'/tmp/{source.id}_data.parquet', index=False)
        
        logging.info(f"Extracted {{len(df)}} rows from {source.name}")
        return f'/tmp/{source.id}_data.parquet'
        
    except Exception as e:
        logging.error(f"Error extracting from {source.name}: {{str(e)}}")
        raise
'''
    
    def _generate_json_extract_function(self, source: DataSourceConfig) -> str:
        """Генерация функции извлечения из JSON"""
        return f'''
def extract_{source.id}(**context):
    """Extract data from JSON source: {source.name}"""
    try:
        import pandas as pd
        import json
        
        with open('{source.config.get("file_path", "")}', 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Преобразуем в DataFrame
        if isinstance(data, list):
            df = pd.json_normalize(data)
        else:
            df = pd.json_normalize([data])
        
        # Сохраняем во временное хранилище
        df.to_parquet(f'/tmp/{source.id}_data.parquet', index=False)
        
        logging.info(f"Extracted {{len(df)}} rows from {source.name}")
        return f'/tmp/{source.id}_data.parquet'
        
    except Exception as e:
        logging.error(f"Error extracting from {source.name}: {{str(e)}}")
        raise
'''
    
    def _generate_transform_function(
        self, 
        sources: List[DataSourceConfig], 
        relationships: List[DataRelationship],
        recommendations: Dict[str, Any]
    ) -> str:
        """Генерация функции трансформации данных"""
        
        # Создаем код для JOIN'ов
        join_code = ""
        if len(sources) > 1 and relationships:
            join_code = "        # Объединение данных\n"
            
            # Находим основную таблицу (первую по списку)
            main_source = sources[0].id
            join_code += f"        result_df = dfs['{main_source}'].copy()\n"
            
            for rel in relationships:
                if rel.source1_id == main_source:
                    other_source = rel.source2_id
                else:
                    other_source = rel.source1_id
                
                # Создаем JOIN
                join_keys = list(rel.join_keys.keys())
                if join_keys:
                    join_key = join_keys[0]
                    join_code += f'''        
        if '{other_source}' in dfs and '{join_key}' in result_df.columns and '{join_key}' in dfs['{other_source}'].columns:
            result_df = result_df.merge(
                dfs['{other_source}'], 
                on='{join_key}', 
                how='left',
                suffixes=('', '_{other_source}')
            )
            logging.info(f"Joined with {other_source} on {join_key}")
'''
        else:
            # Если нет связей, просто объединяем все данные
            join_code = """        # Объединение всех источников данных
        if len(dfs) == 1:
            result_df= list(dfs.values())[0]
        else:
            # Простое вертикальное объединение (если структуры схожи)
            result_df = pd.concat(list(dfs.values()), ignore_index=True, sort=False)
"""
        
        return f'''
def transform_data(**context):
    """Transform and join data from all sources"""
    try:
        import pandas as pd
        from datetime import datetime
        
        # Загружаем данные из всех источников
        dfs = {{}}
        {chr(10).join([f"        dfs['{source.id}'] = pd.read_parquet(f'/tmp/{source.id}_data.parquet')" for source in sources])}
        
        logging.info("Loaded data from all sources")
        
{join_code}
        
        # Дополнительные трансформации
        if 'result_df' in locals():
            # Добавляем метку времени загрузки
            result_df['etl_timestamp'] = datetime.now()
            
            # Удаляем дублирующиеся строки
            result_df = result_df.drop_duplicates()
            
            # Обработка пустых значений
            result_df = result_df.fillna('')
            
            logging.info(f"Transformed data: {{len(result_df)}} rows, {{len(result_df.columns)}} columns")
        else:
            result_df = pd.DataFrame()  # Пустой DataFrame в случае ошибки
        
        # Сохраняем результат
        result_df.to_parquet('/tmp/transformed_data.parquet', index=False)
        return '/tmp/transformed_data.parquet'
        
    except Exception as e:
        logging.error(f"Error in data transformation: {{str(e)}}")
        raise
'''
    
    def _generate_load_function(self, recommendations: Dict[str, Any]) -> str:
        """Генерация функции загрузки данных"""
        
        storage_type = recommendations.get("storage_recommendation", {}).get("primary", "postgresql")
        table_name = recommendations.get("schema_design", {}).get("main_table", "processed_data")
        
        if storage_type.lower() == "clickhouse":
            return self._generate_clickhouse_load(table_name)
        else:
            return self._generate_postgres_load(table_name)
    
    def _generate_clickhouse_load(self, table_name: str) -> str:
        """Генерация функции загрузки в ClickHouse"""
        return f'''
def load_data(**context):
    """Load transformed data to ClickHouse"""
    try:
        import pandas as pd
        from clickhouse_driver import Client
        
        # Загружаем трансформированные данные
        df = pd.read_parquet('/tmp/transformed_data.parquet')
        
        if len(df) == 0:
            logging.warning("No data to load")
            return
        
        # Подключение к ClickHouse
        client = Client(
            host='{os.getenv("CLICKHOUSE_HOST", "localhost")}',
            port={os.getenv("CLICKHOUSE_PORT", "9000")},
            user='{os.getenv("CLICKHOUSE_USER", "default")}',
            password='{os.getenv("CLICKHOUSE_PASSWORD", "")}',
            database='{os.getenv("CLICKHOUSE_DATABASE", "default")}'
        )
        
        # Создаем таблицу если не существует
        create_table_query = """
        CREATE TABLE IF NOT EXISTS {table_name} (
            {chr(10).join([f"    {col} String," for col in ['col1', 'col2']])}  -- Автогенерация колонок
            etl_timestamp DateTime
        ) ENGINE = MergeTree()
        ORDER BY etl_timestamp
        """
        
        try:
            client.execute(create_table_query)
            logging.info("Table {table_name} created/verified")
        except Exception as e:
            logging.warning(f"Error creating table: {{str(e)}}")
        
        # Загружаем данные
        data_tuples = [tuple(row) for row in df.values]
        
        client.execute(
            f"INSERT INTO {table_name} VALUES",
            data_tuples
        )
        
        logging.info(f"Loaded {{len(df)}} rows to ClickHouse table {table_name}")
        
    except Exception as e:
        logging.error(f"Error loading data to ClickHouse: {{str(e)}}")
        raise
'''
    
    def _generate_postgres_load(self, table_name: str) -> str:
        """Генерация функции загрузки в PostgreSQL"""
        return f'''
def load_data(**context):
    """Load transformed data to PostgreSQL"""
    try:
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        # Загружаем трансформированные данные
        df = pd.read_parquet('/tmp/transformed_data.parquet')
        
        if len(df) == 0:
            logging.warning("No data to load")
            return
        
        # Подключение к PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Создаем таблицу если не существует (базовая структура)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            data JSONB,  -- Для гибкости храним как JSONB
            etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        pg_hook.run(create_table_query)
        logging.info("Table {table_name} created/verified")
        
        # Загружаем данные построчно
        insert_query = f"""
        INSERT INTO {table_name} (data, etl_timestamp) 
        VALUES (%(data)s, %(timestamp)s)
        """
        
        for _, row in df.iterrows():
            row_data = row.to_dict()
            timestamp = row_data.pop('etl_timestamp', 'now()')
            
            pg_hook.run(
                insert_query,
                parameters={{
                    'data': str(row_data),  # Конвертируем в JSON string
                    'timestamp': timestamp
                }}
            )
        
        logging.info(f"Loaded {{len(df)}} rows to PostgreSQL table {table_name}")
        
    except Exception as e:
        logging.error(f"Error loading data to PostgreSQL: {{str(e)}}")
        raise
'''

    def save_dag_to_file(self, dag_code: str, dag_name: str) -> str:
        """Сохранение сгенерированного DAG в файл"""
        try:
            # Создаем директорию если не существует
            os.makedirs(self.airflow_dags_path, exist_ok=True)
            
            dag_file_path = os.path.join(self.airflow_dags_path, f"{dag_name}.py")
            
            with open(dag_file_path, 'w', encoding='utf-8') as f:
                f.write(dag_code)
            
            logger.info(f"DAG saved to {dag_file_path}")
            return dag_file_path
            
        except Exception as e:
            logger.error(f"Error saving DAG file: {str(e)}")
            raise
    
    def generate_sql_scripts(self, recommendations: Dict[str, Any]) -> Dict[str, str]:
        """Генерация SQL скриптов для создания схемы"""
        
        ddl_script = recommendations.get("schema_design", {}).get("ddl_script", "")
        storage_type = recommendations.get("storage_recommendation", {}).get("primary", "postgresql")
        
        scripts = {
            "ddl": ddl_script
        }
        
        # Добавляем дополнительные скрипты в зависимости от типа БД
        if storage_type == "clickhouse":
            scripts["optimization"] = """
-- Оптимизация для ClickHouse
OPTIMIZE TABLE analytics_data FINAL;

-- Проверка размера таблицы
SELECT 
    table,
    formatReadableSize(sum(bytes)) as size,
    sum(rows) as rows
FROM system.parts 
WHERE table = 'analytics_data' 
GROUP BY table;
"""
        else:
            scripts["optimization"] = """
-- Оптимизация для PostgreSQL
ANALYZE processed_data;

-- Создание дополнительных индексов при необходимости  
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_processed_data_timestamp 
-- ON processed_data(etl_timestamp);

-- Статистика по таблице
SELECT 
    schemaname,
    tablename,
    n_tup_ins as inserts,
    n_tup_upd as updates,
    n_tup_del as deletes
FROM pg_stat_user_tables 
WHERE tablename = 'processed_data';"""
        
        return scripts
