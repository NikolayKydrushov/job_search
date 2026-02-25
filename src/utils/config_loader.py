import os
from typing import Dict, Optional

from dotenv import load_dotenv

load_dotenv()


class DatabaseConfig:
    """
    Класс для хранения конфигурации базы данных.

    Соблюдает принцип открытости/закрытости (OCP) - конфигурацию можно расширять.
    """

    def __init__(self):
        """Инициализация конфигурации с загрузкой из .env файла."""
        self.db_name = os.getenv("DB_NAME", "hh_parser")
        self.user = os.getenv("DB_USER", "postgres")
        self.password = os.getenv("DB_PASSWORD", "postgres")
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = os.getenv("DB_PORT", "5432")

        # Проверяем, что все необходимые переменные загружены
        self._validate_config()

    def _validate_config(self) -> None:
        """
        Проверяет наличие всех необходимых параметров конфигурации.

        Raises:
            ValueError: Если отсутствуют обязательные параметры
        """
        required_vars = {
            "DB_NAME": self.db_name,
            "DB_USER": self.user,
            "DB_PASSWORD": self.password,
            "DB_HOST": self.host,
            "DB_PORT": self.port,
        }

        missing_vars = [var for var, value in required_vars.items() if not value]

        if missing_vars:
            print(
                f"Предупреждение: Отсутствуют значения для: {', '.join(missing_vars)}"
            )

    def get_connection_params(self) -> Dict[str, str]:
        """
        Возвращает параметры подключения к БД.

        Returns:
            Словарь с параметрами подключения
        """
        return {
            "dbname": self.db_name,
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "port": self.port,
        }
