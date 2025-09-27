
from pydantic import BaseModel
from typing import Dict, List, Optional, Any, Union
from enum import Enum

class SourceType(str, Enum):
    CSV = "csv"
    POSTGRESQL = "postgresql" 
    CLICKHOUSE = "clickhouse"
    JSON = "json"
    EXCEL = "excel"
    REST_API = "rest_api"

class TargetType(str, Enum):
    POSTGRESQL = "postgresql"
    CLICKHOUSE = "clickhouse" 
    HDFS = "hdfs"

class UpdateFrequency(str, Enum):
    ONCE = "once"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    REALTIME = "realtime"

class DataSourceConfig(BaseModel):
    id: str
    name: str
    type: SourceType
    config: Dict[str, Any]
    schema_info: Optional[Dict[str, Any]] = None

class PostgreSQLConfig(BaseModel):
    host: str
    port: int = 5432
    database: str
    username: str
    password: str
    table: str

class CSVConfig(BaseModel):
    file_path: str
    delimiter: str = ","
    encoding: str = "utf-8"

class BusinessRequirements(BaseModel):
    goal: str
    target_metrics: List[str]
    update_frequency: UpdateFrequency
    expected_load: str
    data_retention: str

class DataRelationship(BaseModel):
    source1_id: str
    source2_id: str
    join_type: str
    join_keys: Dict[str, str]
    confidence: float

class SchemaInfo(BaseModel):
    columns: List[str]
    dtypes: Dict[str, str]
    sample_data: List[Dict[str, Any]]
    row_count: int
    null_counts: Dict[str, int]
    unique_counts: Dict[str, int]

class StorageRecommendation(BaseModel):
    primary: TargetType
    reasoning: str
    alternatives: List[TargetType]
    estimated_size: str

class SchemaDesign(BaseModel):
    main_table: str
    partitioning: Optional[str]
    indexes: List[str]
    ddl_script: str

class ETLPipeline(BaseModel):
    steps: List[str]
    schedule: str
    estimated_runtime: str
    dag_code: str

class AIRecommendations(BaseModel):
    storage_recommendation: StorageRecommendation
    schema_design: SchemaDesign
    etl_pipeline: ETLPipeline
    relationships: List[DataRelationship]

class AnalysisRequest(BaseModel):
    sources: List[DataSourceConfig]
    business_requirements: BusinessRequirements
