import httpx
import json
import logging
from typing import Dict, List, Any
from ..models.schemas import BusinessRequirements, DataSourceConfig
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class LLMService:
    """Сервис для работы с языковыми моделями"""
    
    def __init__(self):
        self.yandex_api_key = os.getenv("YANDEX_API_KEY")
        self.yandex_folder_id = os.getenv("YANDEX_FOLDER_ID")
        self.deepseek_api_key = os.getenv("DEEPSEEK_API_KEY")
        
        # URLs для API
        self.yandex_url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
        self.deepseek_url = "https://api.deepseek.com/v1/chat/completions"
    
    async def generate_recommendations(
        self, 
        sources: List[DataSourceConfig], 
        business_requirements: BusinessRequirements,
        data_patterns: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Генерация рекомендаций через LLM"""
        
        prompt = self._create_analysis_prompt(sources, business_requirements, data_patterns)
        
        try:
            # Сначала пробуем YandexGPT
            if self.yandex_api_key:
                response = await self._call_yandex_gpt(prompt)
                if response:
                    return self._parse_llm_response(response)
            
            # Fallback на DeepSeek
            if self.deepseek_api_key:
                response = await self._call_deepseek(prompt)
                if response:
                    return self._parse_llm_response(response)
            
            # Если LLM недоступны, используем rule-based рекомендации
            return self._generate_rule_based_recommendations(sources, business_requirements, data_patterns)
            
        except Exception as e:
            logger.error(f"Error generating LLM recommendations: {str(e)}")
            return self._generate_rule_based_recommendations(sources, business_requirements, data_patterns)
    
    def _create_analysis_prompt(
        self, 
        sources: List[DataSourceConfig], 
        business_requirements: BusinessRequirements,
        data_patterns: Dict[str, Any]
    ) -> str:
        """Создание промпта для анализа"""
        
        sources_info = []
        for source in sources:
            info = f"- {source.name} ({source.type}): {len(source.schema_info.get('columns', []))} колонок, {source.schema_info.get('row_count', 0)} строк"
            if source.schema_info.get('columns'):
                info += f", ключевые поля: {source.schema_info['columns'][:5]}"
            sources_info.append(info)
        
        prompt = f"""
Ты - эксперт Data Engineer. Проанализируй данные и дай рекомендации.

ИСТОЧНИКИ ДАННЫХ:
{chr(10).join(sources_info)}

ПАТТЕРНЫ ДАННЫХ:
- Временные данные: {data_patterns.get('has_temporal_data', False)}
- Временные колонки: {data_patterns.get('temporal_columns', [])}
- Географические данные: {data_patterns.get('has_geographical_data', False)}
- Общее количество строк: {data_patterns.get('total_estimated_rows', 0)}

БИЗНЕС-ТРЕБОВАНИЯ:
- Цель: {business_requirements.goal}
- Метрики: {business_requirements.target_metrics}
- Частота обновления: {business_requirements.update_frequency}
- Ожидаемая нагрузка: {business_requirements.expected_load}

ЗАДАЧА: Предложи оптимальное решение в JSON формате:

{{
  "storage_recommendation": {{
    "primary": "postgresql|clickhouse|hdfs",
    "reasoning": "подробное объяснение выбора",
    "alternatives": ["alternative1", "alternative2"]
  }},
  "schema_design": {{
    "main_table": "название_таблицы",
    "partitioning": "стратегия партицирования или null",
    "indexes": ["список", "индексов"],
    "ddl_script": "CREATE TABLE ..."
  }},
  "etl_pipeline": {{
    "steps": ["шаг1", "шаг2", "шаг3"],
    "schedule": "cron выражение",
    "estimated_runtime": "время выполнения"
  }}
}}
"""
        return prompt
    
    async def _call_yandex_gpt(self, prompt: str) -> str:
        """Вызов YandexGPT API"""
        headers = {
            "Authorization": f"Api-Key {self.yandex_api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "modelUri": f"gpt://{self.yandex_folder_id}/yandexgpt-lite",
            "completionOptions": {
                "stream": False,
                "temperature": 0.3,
                "maxTokens": 2000
            },
            "messages": [
                {
                    "role": "user",
                    "text": prompt
                }
            ]
        }
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(self.yandex_url, headers=headers, json=data)
            
            if response.status_code == 200:
                result = response.json()
                return result["result"]["alternatives"][0]["message"]["text"]
            else:
                logger.error(f"YandexGPT API error: {response.status_code} - {response.text}")
                return None
    
    async def _call_deepseek(self, prompt: str) -> str:
        """Вызов DeepSeek API"""
        headers = {
            "Authorization": f"Bearer {self.deepseek_api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": "deepseek-chat",
            "messages": [
                {
                    "role": "user", 
                    "content": prompt
                }
            ],
            "temperature": 0.3,
            "max_tokens": 2000
        }
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(self.deepseek_url, headers=headers, json=data)
            
            if response.status_code == 200:
                result = response.json()
                return result["choices"][0]["message"]["content"]
            else:
                logger.error(f"DeepSeek API error: {response.status_code} - {response.text}")
                return None
    
    def _parse_llm_response(self, response: str) -> Dict[str, Any]:
        """Парсинг ответа от LLM"""
        try:
            # Извлекаем JSON из ответа
            start = response.find('{')
            end = response.rfind('}') + 1
            
            if start != -1 and end != 0:
                json_str = response[start:end]
                return json.loads(json_str)
            else:
                raise ValueError("JSON not found in response")
                
        except Exception as e:
            logger.error(f"Error parsing LLM response: {str(e)}")
            return {}
    
    def _generate_rule_based_recommendations(
        self, 
        sources: List[DataSourceConfig], 
        business_requirements: BusinessRequirements,
        data_patterns: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Rule-based рекомендации как fallback"""
        
        # Определяем тип хранилища
        total_rows = data_patterns.get('total_estimated_rows', 0)
        has_temporal = data_patterns.get('has_temporal_data', False)
        is_analytics = any(metric in ['продажи', 'аналитика', 'отчет', 'дашборд'] 
                          for metric in business_requirements.target_metrics)
        
        if is_analytics and has_temporal and total_rows > 100000:
            storage = "clickhouse"
            reasoning = "Аналитические запросы по временным данным с большим объемом"
        elif total_rows > 1000000:
            storage = "clickhouse"
            reasoning = "Большой объем данных требует колоночного хранилища"
        else:
            storage = "postgresql"
            reasoning = "Стандартные операционные данные средней нагрузки"
        
        # Партицирование
        partitioning = None
        if has_temporal and storage == "clickhouse":
            if total_rows > 1000000:
                partitioning = "PARTITION BY toYYYYMM(date)"
            else:
                partitioning = "PARTITION BY toYear(date)"
        
        # Индексы
        indexes = []
        temporal_cols = data_patterns.get('temporal_columns', [])
        if temporal_cols:
            indexes.extend(temporal_cols[:2])  # Первые 2 временные колонки
        
        # Генерируем базовый DDL
        main_table = "analytics_data" if is_analytics else "processed_data"
        
        if storage == "clickhouse":
            ddl = f"""
CREATE TABLE {main_table}
(
    date Date,
    timestamp DateTime,
    -- Add your columns here based on source analysis
) ENGINE = MergeTree()
{partitioning if partitioning else ''}
ORDER BY ({', '.join(indexes) if indexes else 'date'})
"""
        else:
            ddl = f"""
CREATE TABLE {main_table} (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Add your columns here based on source analysis
);
{f"CREATE INDEX ON {main_table} ({', '.join(indexes)});" if indexes else ''}
"""
        
        # Расписание
        freq_map = {
            "once": "# Run once manually",
            "hourly": "0 * * * *",
            "daily": "0 2 * * *", 
            "weekly": "0 2 * * 0"
        }
        schedule = freq_map.get(business_requirements.update_frequency.value, "0 2 * * *")
        
        return {
            "storage_recommendation": {
                "primary": storage,
                "reasoning": reasoning,
                "alternatives": ["postgresql", "clickhouse"] if storage == "hdfs" else ["hdfs"]
            },
            "schema_design": {
                "main_table": main_table,
                "partitioning": partitioning,
                "indexes": indexes,
                "ddl_script": ddl.strip()
            },
            "etl_pipeline": {
                "steps": [
                    "Extract from sources",
                    "Join data on common keys",
                    "Apply transformations", 
                    f"Load to {storage}"
                ],
                "schedule": schedule,
                "estimated_runtime": "10-30 minutes"
            }
        }