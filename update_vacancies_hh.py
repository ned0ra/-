from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import requests

# Подключение к базе данных
def get_db_connection():
    return psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="11111",
        host="127.0.0.1",
        port="5432"
    )

# Функция для сбора данных с hh.ru API
def fetch_new_vacancies(**kwargs):
    roles = {
        "Веб-разработка": [96, 160, 104, 112, 113, 124, 84],
        "Программирование": [156, 10, 160, 150, 25, 165, 36, 96, 164, 157, 112, 113, 148, 114, 116, 124, 84],
        "Управление ИТ-проектами": [12, 36, 73, 155, 107],
        "Цифровой маркетинг": [163, 70, 68, 163, 55, 3],
        "Цифровой дизайн": [34, 3]
    }
    area_id = 22  # ID города (например, Владивосток)

    new_vacancies = []

    for category, role_ids in roles.items():
        for role_id in role_ids:
            url = f"https://api.hh.ru/vacancies"
            params = {
                "professional_role": role_id,
                "area": area_id,
                "per_page": 100,  # Максимальное количество вакансий на страницу
                "page": 0
            }
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                for item in data.get("items", []):
                    vacancy = {
                        "name": item.get("name"),
                        "area_id": area_id,
                        "experience_id": item.get("experience", {}).get("id"),
                        "professional_role_id": role_id,
                        "employment_id": item.get("employment", {}).get("id"),
                        "salary_from": item.get("salary", {}).get("from"),
                        "salary_to": item.get("salary", {}).get("to"),
                        "address_name": item.get("address", {}).get("street"),
                        "address_number": item.get("address", {}).get("building"),
                        "snippet_requirement": item.get("snippet", {}).get("requirement"),
                        "snippet_responsibility": item.get("snippet", {}).get("responsibility"),
                        "url": item.get("alternate_url"),
                        "published_at": item.get("published_at"),
                        "category": category
                    }
                    new_vacancies.append(vacancy)

    kwargs['ti'].xcom_push(key='new_vacancies', value=pd.DataFrame(new_vacancies).to_json())

# Функция для проверки дубликатов
def check_duplicates(**kwargs):
    ti = kwargs['ti']
    new_data_json = ti.xcom_pull(key='new_vacancies', task_ids='fetch_new_vacancies')
    new_data = pd.read_json(new_data_json)

    conn = get_db_connection()
    cursor = conn.cursor()

    # Проверяем дубликаты по URL
    existing_urls = set()
    cursor.execute("SELECT url FROM vacancies;")
    for row in cursor.fetchall():
        existing_urls.add(row[0])

    new_data = new_data[~new_data['url'].isin(existing_urls)]
    ti.xcom_push(key='filtered_vacancies', value=new_data.to_json())
    conn.close()

# Функция для обновления таблицы vacancies
def update_database(**kwargs):
    ti = kwargs['ti']
    filtered_data_json = ti.xcom_pull(key='filtered_vacancies', task_ids='check_duplicates')
    filtered_data = pd.read_json(filtered_data_json)

    if filtered_data.empty:
        print("Нет новых вакансий для добавления.")
        return

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        for _, row in filtered_data.iterrows():
            # Получаем ID из справочных таблиц
            cursor.execute("SELECT id FROM categories WHERE name = %s", (row["category"],))
            category_id = cursor.fetchone()[0]

            cursor.execute("SELECT id FROM area WHERE name_id = %s", (row["area_id"],))
            area_id = cursor.fetchone()[0]

            cursor.execute("SELECT id FROM experience WHERE name_id = %s", (row["experience_id"],))
            experience_id = cursor.fetchone()[0]

            cursor.execute("SELECT id FROM professional_role WHERE role_id = %s", (row["professional_role_id"],))
            professional_role_id = cursor.fetchone()[0]

            cursor.execute("SELECT id FROM employment WHERE name_id = %s", (row["employment_id"],))
            employment_id = cursor.fetchone()[0]

            # Вставка данных в таблицу "vacancies"
            cursor.execute("""
                INSERT INTO vacancies (
                    category_id, area_id, experience_id, professional_role_id, employment_id,
                    name, salary_from, salary_to, address_name, address_num,
                    requirement, responsibility, url, published_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                category_id, area_id, experience_id, professional_role_id, employment_id,
                row["name"], row["salary_from"], row["salary_to"], row["address_name"], row["address_number"],
                row["snippet_requirement"], row["snippet_responsibility"], row["url"], row["published_at"]
            ))

        conn.commit()
        print(f"Добавлено {len(filtered_data)} новых вакансий.")

    except Exception as e:
        conn.rollback()
        print(f"Ошибка при добавлении данных: {e}")

    finally:
        conn.close()

# Определение DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'update_vacancies_hh',
    default_args=default_args,
    description='Процесс сбора новых вакансий с hh.ru и обновления таблицы vacancies',
    schedule_interval=timedelta(hours=3),  # Выполняется каждые 3 часа
    catchup=False
)

# Определение задач
fetch_new_vacancies_task = PythonOperator(
    task_id='fetch_new_vacancies',
    python_callable=fetch_new_vacancies,
    provide_context=True,
    dag=dag
)

check_duplicates_task = PythonOperator(
    task_id='check_duplicates',
    python_callable=check_duplicates,
    provide_context=True,
    dag=dag
)

update_database_task = PythonOperator(
    task_id='update_database',
    python_callable=update_database,
    provide_context=True,
    dag=dag
)

# Порядок выполнения задач
fetch_new_vacancies_task >> check_duplicates_task >> update_database_task