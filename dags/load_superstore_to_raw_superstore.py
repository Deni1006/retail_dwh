from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator 

CSV_PATH = "/opt/airflow/data/raw/csv/superstore.csv"

# 🟢 ИСПРАВЛЕНО: Подключаемся к правильной базе dwh_raw
PG_HOST = "postgres"
PG_DB = "dwh_raw"                    # ← ИСПРАВЛЕНО С airflow НА dwh_raw
PG_USER = "airflow"
PG_PASSWORD = "airflow"
TABLE_NAME = "raw.superstore_raw"

def load_superstore_to_raw():
    #читаем csv
    df = pd.read_csv(CSV_PATH, encoding="cp1251")

    if df.empty:
        print("файл superstore.csv пустой, нечего загружать")
        return
    columns = list(df.columns)

     # 2. подключаемся к Postgres
    conn = psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    conn.autocommit = True
    cur = conn.cursor()

    # 🟢 ДОБАВЛЕНО: Создаем схему raw если не существует
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    
    # 3. создаем таблицу, если ее нет (все колонки как text для простоты)
    columns_ddl = ",\n    ".join([f'"{col}" text' for col in columns])
    create_table_sql = f'''
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        {columns_ddl}
    );
    '''
    cur.execute(create_table_sql)

    # 🟢 ИСПРАВЛЕНО: Инкрементальная загрузка вместо TRUNCATE
    # Получаем максимальную дату из существующих данных
    cur.execute(f'SELECT MAX("Order_Date") FROM {TABLE_NAME}')
    last_date_result = cur.fetchone()[0]
    
    # 🔴🔴🔴 ИСПРАВЛЕНО: ВЕРНУТЬ ПРАВИЛЬНЫЕ ОТСТУПЫ 🔴🔴🔴
    if last_date_result:
        # 🟢 ИСПРАВЛЕНО: Правильный парсинг даты DD/MM/YYYY
        df['Order_Date'] = pd.to_datetime(df['Order_Date'], format='%d/%m/%Y')
        last_date = pd.to_datetime(last_date_result)
        
        # Фильтруем только новые данные
        new_data = df[df['Order_Date'] > last_date]
        print(f"Найдено {len(new_data)} новых записей после {last_date}")
    else:
        new_data = df  # Первая загрузка
        print("Первая загрузка всех данных")

    if not new_data.empty:
        # 4. готовим данные к вставке
        rows = list(new_data.itertuples(index=False, name=None))

        insert_columns = ", ".join([f'"{col}"' for col in columns])
        insert_sql = f"INSERT INTO {TABLE_NAME} ({insert_columns}) VALUES %s"

        # 5. массовая вставка только новых данных
        execute_values(cur, insert_sql, rows)
        print(f"Добавлено новых строк: {len(rows)}")
    else:
        print("Нет новых данных для загрузки")

    cur.close()
    conn.close()
    # 🔴🔴🔴 КОНЕЦ ИСПРАВЛЕНИЯ ОТСТУПОВ 🔴🔴🔴

with DAG(
    dag_id="superstore_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["superstore", "etl", "bronze-silver-gold"],
) as dag:

    # Этап 1: Загрузка в Bronze (инкрементальная)
    load_to_bronze = PythonOperator(
        task_id="load_to_bronze",
        python_callable=load_superstore_to_raw,
    )

    # Этап 2: Запуск dbt трансформаций
    dbt_run = BashOperator(
        task_id="run_dbt_transformations",
        bash_command='cd /opt/airflow/dbt && dbt run',
    )

    # Этап 3: Тестирование данных
    dbt_test = BashOperator(
        task_id="test_data_quality",
        bash_command='cd /opt/airflow/dbt && dbt test',
    )

    # Определяем порядок выполнения
    load_to_bronze >> dbt_run >> dbt_test