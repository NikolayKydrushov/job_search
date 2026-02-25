from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from src.api.hh_api import HeadHunterAPI
from src.utils.config_loader import DatabaseConfig


class DatabaseCreator:
    """
    Класс для создания базы данных и таблиц.

    Отвечает за инициализацию структуры БД и заполнение данными.
    Соблюдает принцип единственной ответственности (SRP).
    """

    def __init__(self, config: DatabaseConfig):
        """
        Инициализация создателя БД.

        Args:
            config: Конфигурация подключения к БД
        """
        self.config = config
        self.api = HeadHunterAPI()

    def create_database(self) -> None:
        """
        Создает базу данных, если она не существует.
        """
        # Подключаемся к стандартной БД postgres для создания новой БД
        conn = psycopg2.connect(
            dbname="postgres",
            user=self.config.user,
            password=self.config.password,
            host=self.config.host,
            port=self.config.port,
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Проверяем существование БД
        cur.execute(
            "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",
            (self.config.db_name,),
        )
        exists = cur.fetchone()

        if not exists:
            cur.execute(
                sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(self.config.db_name)
                )
            )
            print(f"База данных {self.config.db_name} создана")
        else:
            print(f"База данных {self.config.db_name} уже существует")

        cur.close()
        conn.close()

    def create_tables(self) -> None:
        """
        Создает таблицы companies и vacancies, если они не существуют.
        """
        conn = psycopg2.connect(**self.config.get_connection_params())
        cur = conn.cursor()

        # Создание таблицы companies
        cur.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                company_id INTEGER PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                description TEXT,
                site_url VARCHAR(255),
                hh_url VARCHAR(255)
            )
        """)

        # Создание таблицы vacancies с внешним ключом
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vacancies (
                vacancy_id INTEGER PRIMARY KEY,
                company_id INTEGER NOT NULL REFERENCES companies(company_id) ON DELETE CASCADE,
                title VARCHAR(255) NOT NULL,
                description TEXT,
                salary_from INTEGER,
                salary_to INTEGER,
                currency VARCHAR(10),
                url VARCHAR(255),
                published_at TIMESTAMP,
                CONSTRAINT fk_vacancies_company 
                    FOREIGN KEY (company_id) 
                    REFERENCES companies(company_id)
            )
        """)

        conn.commit()
        cur.close()
        conn.close()
        print("Таблицы созданы или уже существуют")

    def fill_companies(self, company_ids: List[int]) -> None:
        """
        Заполняет таблицу companies данными из API.

        Args:
            company_ids: Список ID компаний для загрузки
        """
        conn = psycopg2.connect(**self.config.get_connection_params())
        cur = conn.cursor()

        for company_id in company_ids:
            company_data = self.api.get_company(company_id)
            if company_data:
                try:
                    cur.execute(
                        """
                        INSERT INTO companies (company_id, name, description, site_url, hh_url)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (company_id) DO UPDATE SET
                            name = EXCLUDED.name,
                            description = EXCLUDED.description,
                            site_url = EXCLUDED.site_url,
                            hh_url = EXCLUDED.hh_url
                    """,
                        (
                            company_data["id"],
                            company_data["name"],
                            company_data.get("description", ""),
                            company_data.get("site_url", ""),
                            company_data.get("alternate_url", ""),
                        ),
                    )
                    print(f"Компания '{company_data['name']}' добавлена/обновлена")
                except Exception as e:
                    print(f"Ошибка при добавлении компании {company_id}: {e}")
                    conn.rollback()
                else:
                    conn.commit()

        cur.close()
        conn.close()

    def fill_vacancies(self, company_ids: List[int]) -> None:
        """
        Заполняет таблицу vacancies данными из API.

        Args:
            company_ids: Список ID компаний для загрузки вакансий
        """
        conn = psycopg2.connect(**self.config.get_connection_params())
        cur = conn.cursor()

        for company_id in company_ids:
            vacancies = self.api.get_company_vacancies(company_id)

            for vacancy in vacancies:
                try:
                    # Обработка зарплаты
                    salary = vacancy.get("salary")
                    salary_from = None
                    salary_to = None
                    currency = None

                    if salary:
                        salary_from = salary.get("from")
                        salary_to = salary.get("to")
                        currency = salary.get("currency")

                    cur.execute(
                        """
                        INSERT INTO vacancies (
                            vacancy_id, company_id, title, description, 
                            salary_from, salary_to, currency, url, published_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (vacancy_id) DO UPDATE SET
                            title = EXCLUDED.title,
                            description = EXCLUDED.description,
                            salary_from = EXCLUDED.salary_from,
                            salary_to = EXCLUDED.salary_to,
                            currency = EXCLUDED.currency,
                            url = EXCLUDED.url,
                            published_at = EXCLUDED.published_at
                    """,
                        (
                            vacancy["id"],
                            company_id,
                            vacancy["name"],
                            vacancy.get("snippet", {}).get("responsibility", "")
                            or vacancy.get("snippet", {}).get("requirement", ""),
                            salary_from,
                            salary_to,
                            currency,
                            vacancy.get("alternate_url", ""),
                            vacancy.get("published_at"),
                        ),
                    )
                except Exception as e:
                    print(f"Ошибка при добавлении вакансии {vacancy.get('id')}: {e}")
                    conn.rollback()
                else:
                    conn.commit()

            print(f"Вакансии для компании {company_id} загружены")

        cur.close()
        conn.close()

    def initialize(self, company_ids: List[int]) -> None:
        """
        Полная инициализация базы данных.

        Args:
            company_ids: Список ID компаний для загрузки
        """
        self.create_database()
        self.create_tables()
        self.fill_companies(company_ids)
        self.fill_vacancies(company_ids)
        self.api.close()
        print("Инициализация базы данных завершена")
