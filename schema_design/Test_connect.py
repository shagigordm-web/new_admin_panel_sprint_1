import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

try:
    conn = psycopg2.connect(
        dbname=os.environ.get('DB_NAME'),
        user=os.environ.get('DB_USER'),
        password=os.environ.get('DB_PASSWORD'),
        host=os.environ.get('DB_HOST'),
        port=os.environ.get('DB_PORT')
    )
    print("Подключение успешно!")
    conn.close()
except Exception as e:
    print(f"Ошибка подключения: {e}")