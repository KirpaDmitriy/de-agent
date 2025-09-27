import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import json
import logging
from sqlalchemy import create_engine
from ..models.schemas import SchemaInfo, DataSourceConfig, SourceType, DataRelationship

logger = logging.getLogger(__name__)

class MultiSourceAnalyzer:
    """Анализатор для множественных источников данных"""
    
    def __init__(self):
        self.supported_types = [SourceType.CSV, SourceType.POSTGRESQL, SourceType.JSON, SourceType.EXCEL]
    
    def analyze_source(self, source: DataSourceConfig) -> SchemaInfo:
        """Анализ одного источника данных"""
        try:
            if source.type == SourceType.CSV:
                return self._analyze_csv(source.config)
            elif source.type == SourceType.POSTGRESQL:
                return self._analyze_postgresql(source.config)
            elif source.type == SourceType.JSON:
                return self._analyze_json(source.config)
            elif source.type == SourceType.EXCEL:
                return self._analyze_excel(source.config)
            else:
                raise ValueError(f"Unsupported source type: {source.type}")
                
        except Exception as e:
            logger.error(f"Error analyzing source {source.name}: {str(e)}")
            return SchemaInfo(
                columns=[], dtypes={}, sample_data=[], 
                row_count=0, null_counts={}, unique_counts={}
            )
    
    def _analyze_csv(self, config: Dict[str, Any]) -> SchemaInfo:
        """Анализ CSV файла"""
        if "file_data" in config:
            # Streamlit uploaded file
            df = pd.read_csv(
                config["file_data"], 
                delimiter=config.get("delimiter", ","),
                encoding=config.get("encoding", "utf-8"),
                nrows=1000  # Ограничиваем для быстрого анализа
            )
        else:
            df = pd.read_csv(
                config["file_path"],
                delimiter=config.get("delimiter", ","), 
                encoding=config.get("encoding", "utf-8"),
                nrows=1000
            )
        
        return self._create_schema_info(df)
    
    def _analyze_postgresql(self, config: Dict[str, Any]) -> SchemaInfo:
        """Анализ PostgreSQL таблицы"""
        connection_string = (
            f"postgresql://{config['username']}:{config['password']}"
            f"@{config['host']}:{config.get('port', 5432)}/{config['database']}"
        )
        
        engine = create_engine(connection_string)
        query = f"SELECT * FROM {config['table']} LIMIT 1000"
        
        df = pd.read_sql(query, engine)
        engine.dispose()
        
        return self._create_schema_info(df)
    
    def _analyze_json(self, config: Dict[str, Any]) -> SchemaInfo:
        """Анализ JSON файла"""
        if "file_data" in config:
            data = json.load(config["file_data"])
        else:
            with open(config["file_path"], 'r', encoding='utf-8') as f:
                data = json.load(f)
        
        # Преобразуем JSON в DataFrame
        if isinstance(data, list):
            df = pd.json_normalize(data)
        else:
            df = pd.json_normalize([data])
        
        return self._create_schema_info(df)
    
    def _analyze_excel(self, config: Dict[str, Any]) -> SchemaInfo:
        """Анализ Excel файла"""
        if "file_data" in config:
            df = pd.read_excel(config["file_data"], nrows=1000)
        else:
            df = pd.read_excel(config["file_path"], nrows=1000)
        
        return self._create_schema_info(df)
    
    def _create_schema_info(self, df: pd.DataFrame) -> SchemaInfo:
        """Создание SchemaInfo из DataFrame"""
        # Обработка NaN значений для JSON сериализации
        df_clean = df.fillna("")
        
        return SchemaInfo(
            columns=df.columns.tolist(),
            dtypes={col: str(dtype) for col, dtype in df.dtypes.items()},
            sample_data=df_clean.head(5).to_dict('records'),
            row_count=len(df),
            null_counts={col: int(df[col].isnull().sum()) for col in df.columns},
            unique_counts={col: int(df[col].nunique()) for col in df.columns}
        )
    
    def find_relationships(self, sources: List[DataSourceConfig]) -> List[DataRelationship]:
        """Автоматический поиск связей между источниками"""
        relationships = []
        
        for i, source1 in enumerate(sources):
            for j, source2 in enumerate(sources[i+1:], i+1):
                if not source1.schema_info or not source2.schema_info:
                    continue
                cols1 = set(source1.schema_info.get("columns", []))
                cols2 = set(source2.schema_info.get("columns", []))
                
                common_fields = cols1.intersection(cols2)
                
                if common_fields:
                    # Определяем наиболее вероятный ключ для JOIN
                    primary_key = self._identify_primary_key(common_fields, source1, source2)
                    
                    confidence = len(common_fields) / max(len(cols1), len(cols2))
                    
                    relationships.append(DataRelationship(
                        source1_id=source1.id,
                        source2_id=source2.id,
                        join_type="LEFT JOIN",  # По умолчанию
                        join_keys={primary_key: primary_key} if primary_key else {},
                        confidence=confidence
                    ))
        
        return relationships
    
    def _identify_primary_key(self, common_fields: set, source1: DataSourceConfig, source2: DataSourceConfig) -> Optional[str]:
        """Определение наиболее вероятного первичного ключа"""
        # Приоритет по названиям полей
        priority_patterns = ['id', 'uuid', 'key', 'code']
        
        for pattern in priority_patterns:
            for field in common_fields:
                if pattern in field.lower():
                    return field
        
        # Если не нашли по паттерну, берем поле с наибольшей уникальностью
        if source1.schema_info and source2.schema_info:
            unique1 = source1.schema_info.get("unique_counts", {})
            unique2 = source2.schema_info.get("unique_counts", {})
            
            best_field = None
            max_uniqueness = 0
            
            for field in common_fields:
                uniqueness = min(
                    unique1.get(field, 0) / max(source1.schema_info.get("row_count", 1), 1),
                    unique2.get(field, 0) / max(source2.schema_info.get("row_count", 1), 1)
                )
                if uniqueness > max_uniqueness:
                    max_uniqueness = uniqueness
                    best_field = field
            
            return best_field
        
        return list(common_fields)[0] if common_fields else None
    
    def analyze_data_patterns(self, sources: List[DataSourceConfig]) -> Dict[str, Any]:
        """Анализ паттернов данных для рекомендаций"""
        patterns = {
            "has_temporal_data": False,
            "temporal_columns": [],
            "has_geographical_data": False,
            "geographical_columns": [],
            "total_estimated_rows": 0,
            "data_types_distribution": {},
            "suggested_partitioning": None
        }
        
        temporal_keywords = ['date', 'time', 'created', 'updated', 'timestamp']
        geo_keywords = ['lat', 'lon', 'city', 'country', 'region', 'address']
        
        total_rows = 0
        all_columns = []
        
        for source in sources:
            if source.schema_info:
                total_rows += source.schema_info.get("row_count", 0)
                columns = source.schema_info.get("columns", [])
                all_columns.extend(columns)
                
                # Поиск временных колонок
                for col in columns:
                    col_lower = col.lower()
                    if any(keyword in col_lower for keyword in temporal_keywords):
                        patterns["has_temporal_data"] = True
                        patterns["temporal_columns"].append(col)
                
                # Поиск географических колонок
                for col in columns:
                    col_lower = col.lower()
                    if any(keyword in col_lower for keyword in geo_keywords):
                        patterns["has_geographical_data"] = True
                        patterns["geographical_columns"].append(col)
        
        patterns["total_estimated_rows"] = total_rows
        
        # Рекомендации по партицированию
        if patterns["has_temporal_data"]:
            if total_rows > 1000000:  # Больше 1М записей
                patterns["suggested_partitioning"] = "monthly"
            elif total_rows > 100000:  # Больше 100К записей
                patterns["suggested_partitioning"] = "yearly"
        
        return patterns
