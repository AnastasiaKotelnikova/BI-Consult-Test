from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests

# определение типа учебного заведения
def extract_university_type(name):
    name = name.lower()
    if "university" in name:
        return "university"
    elif "institute" in name:
        return "institute"
    elif "college" in name:
        return "college"
    return None

# получение данных
def fetch_universities():
    url = "http://universities.hipolabs.com/search"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# загрузка данных во временную таблицу
def load_universities_to_tmp():
    universities = fetch_universities()
    if not universities:
        return

    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # очистка временной таблицы
            cursor.execute("TRUNCATE TABLE universities_tmp;")
            for university in universities:
                university_type = extract_university_type(university.get("name", ""))
                insert_sql = """
                    INSERT INTO universities_tmp (name, alpha_two_code, country, state_province, type_of_university)
                    VALUES (%s, %s, %s, %s, %s);
                """
                cursor.execute(insert_sql, (
                    university.get("name"),
                    university.get("alpha_two_code"),
                    university.get("country"),
                    university.get("state-province"),
                    university_type
                ))
            conn.commit()

# DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 25),
    'retries': 1,
    'postgres_conn_id':'postgres_default'
}

dag = DAG(
    'load_universities_dag',
    default_args=default_args,
    description='DAG для загрузки данных об университетах',
    schedule='0 3 * * *',  # запуск каждый день в 3 ночи
)

# таски
create_tmp_table_task = PostgresOperator(
    task_id='create_tmp_table',
    sql="""
    CREATE TABLE IF NOT EXISTS universities_tmp (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        alpha_two_code VARCHAR(10),
        country VARCHAR(100),
        state_province VARCHAR(100),
        type_of_university VARCHAR(50)
    );
    """,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    sql="""
    CREATE TABLE IF NOT EXISTS universities (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        alpha_two_code VARCHAR(10),
        country VARCHAR(100),
        state_province VARCHAR(100),
        type_of_university VARCHAR(50),
        UNIQUE (name, alpha_two_code, country)
    );
    """,
    dag=dag,
)

load_universities_task = PythonOperator(
    task_id='load_universities_to_tmp',
    python_callable=load_universities_to_tmp,
    dag=dag,
)

insert_new_universities_task = PostgresOperator(
    task_id='insert_new_universities',
    sql="""
    INSERT INTO universities (name, alpha_two_code, country, state_province, type_of_university)
    SELECT DISTINCT name, alpha_two_code, country, state_province, type_of_university
    FROM universities_tmp
    ON CONFLICT (name, alpha_two_code, country) DO NOTHING;  -- Игнорировать дубли
    """,
    dag=dag,
)

# зависимости
create_tmp_table_task >> create_table_task >> load_universities_task >> insert_new_universities_task