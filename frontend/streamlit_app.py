import streamlit as st
import requests
import pandas as pd
import os

# Конфигурация
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")

st.set_page_config(
    page_title="🤖 ИИ-ассистент Data Engineer",
    page_icon="🚀",
    layout="wide"
)

def main():
    st.title("🤖 ИИ-ассистент Data Engineer")
    st.markdown("### Автоматическая генерация ETL пайплайнов с помощью ИИ")
    
    # Инициализация session state
    if 'sources' not in st.session_state:
        st.session_state.sources = []
    if 'recommendations' not in st.session_state:
        st.session_state.recommendations = None
    
    # Создаем вкладки
    tab1, tab2, tab3, tab4 = st.tabs(["📥 Источники данных", "🎯 Бизнес-требования", "🚀 Рекомендации", "💻 Сгенерированный код"])
    
    with tab1:
        handle_data_sources()
    
    with tab2:
        business_requirements = handle_business_requirements()
    
    with tab3:
        if business_requirements and st.session_state.sources:
            handle_recommendations(business_requirements)
    
    with tab4:
        if st.session_state.recommendations:
            display_generated_code()

def handle_data_sources():
    """Обработка источников данных"""
    st.header("📥 Источники данных")
    
    # Добавление нового источника
    with st.expander("➕ Добавить источник данных", expanded=len(st.session_state.sources) == 0):
        source_type = st.selectbox(
            "Тип источника:",
            ["CSV файл", "PostgreSQL", "ClickHouse", "JSON файл", "Excel"]
        )
        
        source_name = st.text_input("Название источника")
        
        if source_type == "CSV файл":
            uploaded_file = st.file_uploader("Загрузите CSV файл", type=['csv'])
            if uploaded_file and source_name:
                delimiter = st.selectbox("Разделитель", [",", ";", "\t"])
                encoding = st.selectbox("Кодировка", ["utf-8", "windows-1251"])
                
                if st.button("Анализировать CSV"):
                    with st.spinner("Анализ файла..."):
                        try:
                            files = {"file": uploaded_file}
                            response = requests.post(f"{BACKEND_URL}/upload-csv", files=files)
                            
                            if response.status_code == 200:
                                result = response.json()
                                
                                source = {
                                    "id": f"source_{len(st.session_state.sources) + 1}",
                                    "name": source_name,
                                    "type": "csv",
                                    "config": {
                                        "delimiter": delimiter,
                                        "encoding": encoding
                                    },
                                    "schema_info": result["schema"]
                                }
                                
                                st.session_state.sources.append(source)
                                st.success(f"✅ CSV файл '{source_name}' успешно добавлен!")
                                st.rerun()
                            else:
                                st.error(f"Ошибка анализа файла: {response.text}")
                        except Exception as e:
                            st.error(f"Ошибка: {str(e)}")
        
        elif source_type == "PostgreSQL":
            col1, col2 = st.columns(2)
            
            with col1:
                host = st.text_input("Хост", value="localhost")
                database = st.text_input("База данных")
                username = st.text_input("Пользователь")
            
            with col2:
                port = st.number_input("Порт", value=5432, min_value=1, max_value=65535)
                table = st.text_input("Таблица")
                password = st.text_input("Пароль", type="password")
            
            if all([host, database, username, password, table, source_name]):
                col_test, col_add = st.columns(2)
                
                with col_test:
                    if st.button("Тест подключения"):
                        config = {
                            "type": "postgresql",
                            "host": host,
                            "port": port,
                            "database": database,
                            "username": username,
                            "password": password
                        }
                        
                        try:
                            response = requests.post(f"{BACKEND_URL}/test-connection", json=config)
                            result = response.json()
                            
                            if result["status"] == "success":
                                st.success("✅ Подключение успешно!")
                            else:
                                st.error(f"❌ Ошибка: {result['message']}")
                        except Exception as e:
                            st.error(f"Ошибка тестирования: {str(e)}")
                
                with col_add:
                    if st.button("Добавить источник"):
                        source = {
                            "id": f"source_{len(st.session_state.sources) + 1}",
                            "name": source_name,
                            "type": "postgresql",
                            "config": {
                                "host": host,
                                "port": port,
                                "database": database,
                                "username": username,
                                "password": password,
                                "table": table
                            }
                        }
                        
                        # Анализируем структуру
                        try:
                            response = requests.post(f"{BACKEND_URL}/analyze-source", json=source)
                            if response.status_code == 200:
                                source["schema_info"] = response.json()
                                st.session_state.sources.append(source)
                                st.success(f"✅ PostgreSQL источник '{source_name}' добавлен!")
                                st.rerun()
                            else:
                                st.error(f"Ошибка анализа: {response.text}")
                        except Exception as e:
                            st.error(f"Ошибка: {str(e)}")
    
    # Отображение добавленных источников
    if st.session_state.sources:
        st.subheader("📊 Добавленные источники")
        
        for i, source in enumerate(st.session_state.sources):
            with st.expander(f"🔍 {source['name']} ({source['type']})", expanded=False):
                if source.get('schema_info'):
                    schema = source['schema_info']
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("Колонки", len(schema.get('columns', [])))
                    with col2:
                        st.metric("Строки", schema.get('row_count', 0))
                    with col3:
                        st.metric("Заполненность", f"{((schema.get('row_count', 0) * len(schema.get('columns', [])) - sum(schema.get('null_counts', {}).values())) / max(schema.get('row_count', 0) * len(schema.get('columns', [])), 1) * 100):.1f}%")
                    
                    # Таблица колонок
                    if schema.get('columns'):
                        df_schema = pd.DataFrame({
                            'Колонка': schema['columns'],
                            'Тип': [schema.get('dtypes', {}).get(col, 'unknown') for col in schema['columns']],
                            'Пустые значения': [schema.get('null_counts', {}).get(col, 0) for col in schema['columns']],
                            'Уникальные': [schema.get('unique_counts', {}).get(col, 0) for col in schema['columns']]
                        })
                        st.dataframe(df_schema, use_container_width=True)
                    
                    # Превью данных
                    if schema.get('sample_data'):
                        st.subheader("👀 Превью данных")
                        sample_df = pd.DataFrame(schema['sample_data'])
                        st.dataframe(sample_df, use_container_width=True)
                
                # Кнопка удаления
                if st.button(f"🗑️ Удалить {source['name']}", key=f"delete_{i}"):
                    st.session_state.sources.pop(i)
                    st.rerun()
        
        # Автоматический поиск связей
        if len(st.session_state.sources) > 1:
            if st.button("🔗 Найти связи между источниками"):
                with st.spinner("Поиск связей..."):
                    try:
                        response = requests.post(
                            f"{BACKEND_URL}/find-relationships", 
                            json=st.session_state.sources
                        )
                        
                        if response.status_code == 200:
                            relationships = response.json()["relationships"]
                            
                            if relationships:
                                st.subheader("🔗 Обнаруженные связи")
                                
                                for rel in relationships:
                                    st.info(
                                        f"{rel['source1_id']} ↔️ {rel['source2_id']}\n"
                                        f"Тип JOIN: {rel['join_type']}\n"
                                        f"Общие поля: {list(rel['join_keys'].keys())}\n"
                                        f"Уверенность: {rel['confidence']:.2%}"
                                    )
                            else:
                                st.warning("🤔 Связи между источниками не обнаружены")
                        else:
                            st.error(f"Ошибка поиска связей: {response.text}")
                    except Exception as e:
                        st.error(f"Ошибка: {str(e)}")

def handle_business_requirements():
    """Обработка бизнес-требований"""
    st.header("🎯 Бизнес-требования")
    
    business_goal = st.text_area(
        "Опишите цель проекта:",
        placeholder="""
Например:
• Создать дашборд продаж по регионам и товарным категориям
• Построить отчет по эффективности маркетинговых кампаний  
• Система мониторинга качества обслуживания клиентов
• Анализ поведения пользователей на сайте и конверсий
        """,
        height=120
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        target_metrics = st.multiselect(
            "Важные метрики:",
            [
                "Продажи и выручка", "Конверсия", "Средний чек", 
                "Количество клиентов", "География продаж", 
                "Временные тренды", "Топ товары/услуги", 
                "Сегментация клиентов", "ROI кампаний",
                "Качество обслуживания", "Поведенческие метрики"
            ]
        )
        
        update_frequency = st.selectbox(
            "Частота обновления:",
            ["once", "hourly", "daily", "weekly", "realtime"],
            format_func=lambda x: {
                "once": "Разово",
                "hourly": "Каждый час", 
                "daily": "Каждый день",
                "weekly": "Каждую неделю",
                "realtime": "Реальное время"
            }[x]
        )
    
    with col2:
        expected_load = st.selectbox(
            "Ожидаемая нагрузка:",
            [
                "Низкая (до 10 пользователей)", 
                "Средняя (10-100 пользователей)",
                "Высокая (100+ пользователей)"
            ]
        )
        
        data_retention = st.selectbox(
            "Период хранения данных:",
            ["3 месяца", "1 год", "3 года", "Бессрочно"]
        )
    
    if business_goal:
        return {
            "goal": business_goal,
            "target_metrics": target_metrics,
            "update_frequency": update_frequency,
            "expected_load": expected_load,
            "data_retention": data_retention
        }
    else:
        st.warning("📝 Пожалуйста, опишите цель проекта")
        return None

def handle_recommendations(business_requirements):
    """Генерация и отображение рекомендаций"""
    st.header("🚀 ИИ Рекомендации")
    
    if st.button("🧠 Получить рекомендации от ИИ", type="primary", use_container_width=True):
        with st.spinner("🤖 ИИ анализирует данные и генерирует рекомендации..."):
            try:
                request_data = {
                    "sources": st.session_state.sources,
                    "business_requirements": business_requirements
                }
                
                response = requests.post(
                    f"{BACKEND_URL}/generate-recommendations",
                    json=request_data,
                    timeout=60
                )
                
                if response.status_code == 200:
                    st.session_state.recommendations = response.json()
                    st.success("✅ Рекомендации получены!")
                    st.rerun()
                else:st.error(f"Ошибка генерации рекомендаций: {response.text}")
                    
            except Exception as e:
                st.error(f"Ошибка: {str(e)}")
    
    # Отображение рекомендаций
    if st.session_state.recommendations:
        display_recommendations(st.session_state.recommendations)

def display_recommendations(recommendations):
    """Отображение рекомендаций"""
    st.subheader("📊 Результат анализа")
    
    # Основные рекомендации
    rec = recommendations["recommendations"]
    
    # Рекомендация по хранилищу
    st.markdown("### 🗄️ Рекомендуемое хранилище")
    
    col1, col2 = st.columns([2, 3])
    
    with col1:
        storage = rec["storage_recommendation"]
        st.metric(
            "Основное хранилище", 
            storage["primary"].upper(),
            help=storage["reasoning"]
        )
        
        if storage["alternatives"]:
            st.write("Альтернативы:")
            for alt in storage["alternatives"]:
                st.write(f"• {alt}")
    
    with col2:
        st.info(f"Обоснование: {storage['reasoning']}")
        
        if storage.get("estimated_size"):
            st.write(f"Ожидаемый размер: {storage['estimated_size']}")
    
    # Схема данных
    st.markdown("### 📋 Дизайн схемы")
    
    schema = rec["schema_design"]
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write(f"Основная таблица: {schema['main_table']}")
        
        if schema.get("partitioning"):
            st.write(f"Партицирование: {schema['partitioning']}")
        
        if schema.get("indexes"):
            st.write("Рекомендуемые индексы:")
            for idx in schema["indexes"]:
                st.write(f"• {idx}")
    
    with col2:
        if schema.get("ddl_script"):
            st.code(schema["ddl_script"], language="sql")
    
    # ETL пайплайн
    st.markdown("### ⚙️ ETL Пайплайн")
    
    pipeline = rec["etl_pipeline"]
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("Этапы обработки:")
        for i, step in enumerate(pipeline["steps"], 1):
            st.write(f"{i}. {step}")
        
        st.write(f"Расписание: {pipeline['schedule']}")
        st.write(f"Время выполнения: {pipeline['estimated_runtime']}")
    
    with col2:
        if recommendations.get("relationships"):
            st.write("Обнаруженные связи:")
            for rel in recommendations["relationships"]:
                st.write(f"• {rel['source1_id']} → {rel['source2_id']}")
    
    # Паттерны данных
    if recommendations.get("data_patterns"):
        st.markdown("### 📈 Анализ данных")
        
        patterns = recommendations["data_patterns"]
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Общее количество строк", f"{patterns.get('total_estimated_rows', 0):,}")
        
        with col2:
            temporal_status = "✅ Есть" if patterns.get('has_temporal_data') else "❌ Нет"
            st.metric("Временные данные", temporal_status)
        
        with col3:
            geo_status = "✅ Есть" if patterns.get('has_geographical_data') else "❌ Нет"  
            st.metric("Географические данные", geo_status)

def display_generated_code():
    """Отображение сгенерированного кода"""
    st.header("💻 Сгенерированный код")
    
    if not st.session_state.recommendations:
        st.warning("Сначала получите рекомендации")
        return
    
    generated_code = st.session_state.recommendations.get("generated_code", {})
    
    # Airflow DAG
    st.subheader("🌪️ Airflow DAG")
    
    if generated_code.get("airflow_dag"):
        st.code(generated_code["airflow_dag"], language="python")
        
        # Кнопка скачивания
        st.download_button(
            "📥 Скачать DAG файл",
            data=generated_code["airflow_dag"],
            file_name=f"{st.session_state.recommendations.get('project_info', {}).get('name', 'generated')}_dag.py",mime="text/python"
        )
    
    # SQL скрипты
    st.subheader("🗄️ SQL Скрипты")
    
    if generated_code.get("sql_scripts"):
        scripts = generated_code["sql_scripts"]
        
        # DDL скрипт
        if scripts.get("ddl"):
            st.markdown("**Создание таблицы (DDL):**")
            st.code(scripts["ddl"], language="sql")
        
        # Скрипты оптимизации
        if scripts.get("optimization"):
            st.markdown("**Оптимизация:**")
            st.code(scripts["optimization"], language="sql")
        
        # Кнопка скачивания всех скриптов
        all_scripts = "\n\n-- DDL Script\n" + scripts.get("ddl", "")
        all_scripts += "\n\n-- Optimization Script\n" + scripts.get("optimization", "")
        
        st.download_button(
            "📥 Скачать SQL скрипты",
            data=all_scripts,
            file_name="generated_scripts.sql",
            mime="text/sql"
        )
    
    # Информация о проекте
    if st.session_state.recommendations.get("project_info"):
        st.subheader("📋 Информация о проекте")
        project_info = st.session_state.recommendations["project_info"]
        
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Название проекта:** {project_info.get('name', 'Unknown')}")
        with col2:
            st.write(f"**Ожидаемое время выполнения:** {project_info.get('estimated_runtime', 'Unknown')}")
        
        # Инструкция по деплою
        st.markdown("### 🚀 Инструкция по развертыванию")
        
        storage_type = st.session_state.recommendations["recommendations"]["storage_recommendation"]["primary"]
        deploy_instructions = f"""
**Шаги для развертывания:**

1. **Настройка окружения:**

   # Установка зависимостей
   ```
   pip install apache-airflow pandas {'clickhouse-driver' if storage_type == 'clickhouse' else 'psycopg2-binary'}
   ```

2. **Настройка подключений в Airflow:**

   # Airflow Connections
   ```
   {'ClickHouse: clickhouse_default' if storage_type == 'clickhouse' else 'PostgreSQL: postgres_default'}
   ```
 
3. **Загрузка DAG файла:**
   
   # Скопировать в папку dags Airflow
   ```
   cp generated_dag.py $AIRFLOW_HOME/dags/
   ```
   

4. **Выполнение SQL скриптов:**
   
   -- Выполнить DDL скрипты для создания таблиц
   

5. **Активация DAG в Airflow UI**
        """
        
        st.markdown(deploy_instructions)

if __name__ == "__main__":
    main()
