# src/config/settings.py
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


class Settings:
    oracle = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'service_name': os.getenv('DB_SERVICE_NAME'),
        'username': os.getenv('DB_USERNAME'),
        'password': os.getenv('DB_PASSWORD')
    }

    doris = {
        'host': os.getenv('DORIS_HOST').strip(),
        'port': int(os.getenv('DORIS_PORT')),
        'username': os.getenv('DORIS_USERNAME'),
        'password': os.getenv('DORIS_PASSWORD'),
        'database': os.getenv('DORIS_DATABASE')
    }


settings = Settings()