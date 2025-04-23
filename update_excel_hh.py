from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests

# Путь к Excel-файлу
EXCEL_FILE_PATH = r"C:\Users\Пользователь\Desktop\ITHUB\3 курс\семинар по анализу данных\last kt\all_vac.xlsx"

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

    # Загружаем существующие данные из Excel
    try:
        existing_data = pd.read_excel(EXCEL_FILE_PATH)
    except FileNotFoundError:
        existing_data = pd.DataFrame()  # Если файл не существует, создаем пустой DataFrame

    # Проверяем дубликаты по URL
    if not existing_data.empty:
        existing_urls = set(existing_data['url'])
        new_data = new_data[~new_data['url'].isin(existing_urls)]

    ti.xcom_push(key='filtered_vacancies', value=new_data.to_json())

# Функция для обновления Excel-файла
def update_excel_file(**kwargs):
    ti = kwargs['ti']
    filtered_data_json = ti.xcom_pull(key='filtered_vacancies', task_ids='check_duplicates')
    filtered_data = pd.read_json(filtered_data_json)

    if filtered_data.empty:
        print("Нет новых вакансий для добавления.")
        return

    # Загружаем существующие данные из Excel
    try:
        existing_data = pd.read_excel(EXCEL_FILE_PATH)
    except FileNotFoundError:
        existing_data = pd.DataFrame()  # Если файл не существует, создаем пустой DataFrame

    # Объединяем старые и новые данные
    updated_data = pd.concat([existing_data, filtered_data], ignore_index=True)

    # Сохраняем обновленные данные в Excel
    updated_data.to_excel(EXCEL_FILE_PATH, index=False)
    print(f"Добавлено {len(filtered_data)} новых вакансий в Excel.")

# Определение DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'update_excel_hh',
    default_args=default_args,
    description='Процесс сбора новых вакансий с hh.ru и обновления Excel-файла',
    schedule_interval=timedelta(hours = 1),  # Выполняется каждую минуту
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

update_excel_file_task = PythonOperator(
    task_id='update_excel_file',
    python_callable=update_excel_file,
    provide_context=True,
    dag=dag
)

# Порядок выполнения задач
fetch_new_vacancies_task >> check_duplicates_task >> update_excel_file_task