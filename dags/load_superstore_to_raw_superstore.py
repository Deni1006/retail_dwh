"""
DAG: superstore_etl
Описание: 
    Инкрементальная загрузка данных из CSV в PostgreSQL (слой raw),
    затем запуск dbt-трансформаций и тестирование качества данных.
Теги: superstore, etl, bronze-silver-gold
"""

from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ============================================
# 1. КОНСТАНТЫ И НАСТРОЙКИ
# ============================================

CSV_PATH = "/opt/airflow/data/raw/csv/superstore.csv"

# Параметры подключения к PostgreSQL (целевая база — dwh_raw)
PG_HOST = "postgres"
PG_DB = "dwh_raw"
PG_USER = "airflow"
PG_PASSWORD = "airflow"
TABLE_NAME = "raw.superstore_raw"

# ============================================
# 2. ФУНКЦИЯ ЗАГРУЗКИ ДАННЫХ (BRONZE-СЛОЙ)
# ============================================

def load_superstore_to_raw():
    """
    Инкрементальная загрузка данных из CSV в таблицу raw.superstore_raw.
    - Создаёт схему raw, если не существует.
    - Создаёт таблицу, если не существует (все колонки типа text).
    - Загружает только новые записи на основе максимальной даты заказа.
    """
    
    # Чтение CSV-файла
    df = pd.read_csv(CSV_PATH, encoding="cp1251")
    
    if df.empty:
        print("⚠️  Файл superstore.csv пуст. Загрузка отменена.")
        return
    
    columns = list(df.columns)
    
    # Подключение к PostgreSQL
    conn = psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    # Создание схемы raw, если отсутствует
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    
    # Создание таблицы с динамическими колонками типа text
    columns_ddl = ",\n    ".join([f'"{col}" text' for col in columns])
    create_table_sql = f'''
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        {columns_ddl}
    );
    '''
    cur.execute(create_table_sql)
    
    # Определение последней загруженной даты
    cur.execute(f'SELECT MAX("Order_Date") FROM {TABLE_NAME}')
    last_date_result = cur.fetchone()[0]
    
    # Преобразование колонки Order_Date в datetime
    df['Order_Date'] = pd.to_datetime(df['Order_Date'], format='%d/%m/%Y')
    
    if last_date_result:
        last_date = pd.to_datetime(last_date_result)
        new_data = df[df['Order_Date'] > last_date]
        print(f"📅 Последняя загруженная дата: {last_date}")
        print(f"➕ Найдено новых записей: {len(new_data)}")
    else:
        new_data = df
        print("🆕 Первая загрузка — переносим все данные.")
    
    # Вставка только новых строк
    if not new_data.empty:
        rows = list(new_data.itertuples(index=False, name=None))
        insert_columns = ", ".join([f'"{col}"' for col in columns])
        insert_sql = f"INSERT INTO {TABLE_NAME} ({insert_columns}) VALUES %s"
        execute_values(cur, insert_sql, rows)
        print(f"✅ Успешно добавлено строк: {len(rows)}")
    else:
        print("⏸️  Нет новых данных для загрузки.")
    
    cur.close()
    conn.close()

# ============================================
# 3. ОПРЕДЕЛЕНИЕ DAG
# ============================================

with DAG(
    dag_id="superstore_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Только ручной запуск
    catchup=False,
    tags=["superstore", "etl", "dbt"],
    description="Загрузка Superstore → raw → dbt (silver/gold) → тесты",
    doc_md=__doc__,  # Подтягивает докстринг из начала файла
) as dag:

    # ============================================
    # 4. ЗАДАЧИ (TASKS)
    # ============================================
    
    # Задача 1: Загрузка данных в слой raw (инкрементально)
    load_to_bronze = PythonOperator(
        task_id="load_to_bronze",
        python_callable=load_superstore_to_raw,
        doc="Инкрементальная загрузка CSV в raw.superstore_raw",
    )
    
    # Задача 2: Запуск dbt-трансформаций (сборка silver и gold)
    dbt_run = BashOperator(
        task_id="run_dbt_transformations",
        bash_command="cd /opt/airflow/dbt && dbt run",
        doc="Запуск моделей dbt: серебро и золото",
    )
    
    # Задача 3: Запуск тестов качества данных (dbt test)
    dbt_test = BashOperator(
        task_id="test_data_quality",
        bash_command="cd /opt/airflow/dbt && dbt test",
        doc="Проверка целостности и качества данных через dbt-тесты",
    )
    
    # ============================================
    # 5. ПОРЯДОК ВЫПОЛНЕНИЯ (DEPENDENCIES)
    # ============================================
    
    load_to_bronze >> dbt_run >> dbt_test