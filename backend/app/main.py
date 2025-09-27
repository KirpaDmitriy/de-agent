from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import logging

from .models.schemas import (
    DataSourceConfig, AnalysisRequest, SchemaInfo
)
from .services.analyzer import MultiSourceAnalyzer
from .services.llm_service import LLMService
from .services.pipeline_generator import PipelineGenerator

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Создаем FastAPI приложение
app = FastAPI(
    title="AI ETL Assistant",
    description="ИИ-ассистент для автоматизации ETL процессов",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшене ограничить конкретными доменами
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Инициализация сервисов
analyzer = MultiSourceAnalyzer()
llm_service = LLMService()
pipeline_generator = PipelineGenerator()

@app.get("/")
async def root():
    return {"message": "AI ETL Assistant API", "status": "running"}

@app.post("/analyze-source")
async def analyze_data_source(source: DataSourceConfig) -> SchemaInfo:
    """Анализ одного источника данных"""
    try:
        schema_info = analyzer.analyze_source(source)
        return schema_info
    except Exception as e:
        logger.error(f"Error analyzing source: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload-csv")
async def upload_csv_file(file: UploadFile = File(...)) -> dict:
    """Загрузка и анализ CSV файла"""
    try:
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="File must be a CSV")
        
        # Создаем временную конфигурацию источника
        source_config = DataSourceConfig(
            id="uploaded_csv",
            name=file.filename,
            type="csv",
            config={
                "file_data": file.file,
                "delimiter": ",",
                "encoding": "utf-8"
            }
        )
        
        # Анализируем файл
        schema_info = analyzer.analyze_source(source_config)
        
        return {
            "filename": file.filename,
            "schema": schema_info.dict(),
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"Error uploading CSV: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/find-relationships")
async def find_data_relationships(sources: List[DataSourceConfig]) -> dict:
    """Поиск связей между источниками данных"""
    try:
        # Сначала анализируем все источники
        for source in sources:
            if not source.schema_info:
                source.schema_info = analyzer.analyze_source(source).dict()
        
        relationships = analyzer.find_relationships(sources)
        
        return {
            "relationships": [rel.dict() for rel in relationships],
            "count": len(relationships)
        }
        
    except Exception as e:
        logger.error(f"Error finding relationships: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/generate-recommendations")
async def generate_recommendations(request: AnalysisRequest) -> dict:
    """Генерация рекомендаций через ИИ"""
    try:
        # Анализируем источники данных
        for source in request.sources:
            if not source.schema_info:
                source.schema_info = analyzer.analyze_source(source).dict()
        
        # Поиск связей между источниками
        relationships = analyzer.find_relationships(request.sources)
        
        # Анализ паттернов данных
        data_patterns = analyzer.analyze_data_patterns(request.sources)
        
        # Генерация рекомендаций через LLM
        ai_recommendations = await llm_service.generate_recommendations(
            sources=request.sources,
            business_requirements=request.business_requirements,
            data_patterns=data_patterns
        )
        
        # Генерация Airflow DAG
        project_name = f"project_{hash(str(request.business_requirements.goal))}"
        dag_code = pipeline_generator.generate_airflow_dag(
            sources=request.sources,
            recommendations=ai_recommendations,
            relationships=relationships,
            project_name=project_name
        )
        
        # Генерация SQL скриптов
        sql_scripts = pipeline_generator.generate_sql_scripts(ai_recommendations)
        
        return {
            "recommendations": ai_recommendations,
            "relationships": [rel.dict() for rel in relationships],
            "data_patterns": data_patterns,
            "generated_code": {
                "airflow_dag": dag_code,
                "sql_scripts": sql_scripts
            },
            "project_info": {
                "name": project_name,
                "estimated_runtime": ai_recommendations.get("etl_pipeline", {}).get("estimated_runtime", "unknown")
            }
        }
        
    except Exception as e:
        logger.error(f"Error generating recommendations: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/test-connection")
async def test_database_connection(config: dict) -> dict:
    """Тестирование подключения к базе данных"""
    try:
        if config.get("type") == "postgresql":
            from sqlalchemy import create_engine
            
            connection_string = (
                f"postgresql://{config['username']}:{config['password']}"
                f"@{config['host']}:{config.get('port', 5432)}/{config['database']}"
            )
            
            engine = create_engine(connection_string)
            
            # Пробуем подключиться
            with engine.connect() as conn:
                conn.execute("SELECT 1").fetchone()
                
            return {"status": "success", "message": "Connection successful"}
            
        elif config.get("type") == "clickhouse":
            from clickhouse_driver import Client
            
            client = Client(
                host=config['host'],
                port=config.get('port', 9000),
                user=config.get('username', 'default'),
                password=config.get('password', ''),
                database=config.get('database', 'default')
            )
            
            client.execute("SELECT 1")
            
            return {"status": "success", "message": "Connection successful"}
            
        else:
            raise HTTPException(status_code=400, detail="Unsupported database type")
            
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/health")
async def health_check():
    """Проверка здоровья сервиса"""
    return {
        "status": "healthy",
        "services": {
            "analyzer": "running",
            "llm_service": "running" if llm_service.yandex_api_key or llm_service.deepseek_api_key else "no_api_key",
            "pipeline_generator": "running"
        }
    }

if __name__ == "main":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
