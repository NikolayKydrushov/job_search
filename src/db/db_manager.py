from typing import Any, List, Optional, Tuple

import psycopg2

from src.utils.config_loader import DatabaseConfig


class DBManager:
    """
    Класс для работы с данными в базе данных.

    Предоставляет методы для получения различной информации о компаниях и вакансиях.
    Соблюдает принцип единственной ответственности (SRP) - только работа с БД.
    Все методы имеют типизацию и документацию.
    """

    def __init__(self, config: DatabaseConfig):
        """
        Инициализация менеджера базы данных.

        Args:
            config: Конфигурация подключения к БД
        """
        self.config = config
        self.conn = None

    def _connect(self) -> None:
        """Устанавливает соединение с базой данных."""
        if not self.conn or self.conn.closed:
            self.conn = psycopg2.connect(**self.config.get_connection_params())

    def _close(self) -> None:
        """Закрывает соединение с базой данных."""
        if self.conn and not self.conn.closed:
            self.conn.close()

    def _execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """
        Выполняет SQL-запрос и возвращает результат.

        Args:
            query: SQL-запрос
            params: Параметры запроса

        Returns:
            Результат запроса
        """
        self._connect()
        cur = self.conn.cursor()

        try:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)

            result = cur.fetchall()
            cur.close()
            return result
        except Exception as e:
            print(f"Ошибка при выполнении запроса: {e}")
            cur.close()
            return []

    def get_companies_and_vacancies_count(self) -> List[Tuple[str, int]]:
        """
        Получает список всех компаний и количество вакансий у каждой компании.

        Использует JOIN для объединения таблиц.

        Returns:
            Список кортежей (название компании, количество вакансий)
        """
        query = """
            SELECT c.name, COUNT(v.vacancy_id) as vacancies_count
            FROM companies c
            LEFT JOIN vacancies v ON c.company_id = v.company_id
            GROUP BY c.company_id, c.name
            ORDER BY vacancies_count DESC
        """
        return self._execute_query(query)

    def get_all_vacancies(
        self,
    ) -> List[Tuple[str, str, Optional[int], Optional[int], str]]:
        """
        Получает список всех вакансий с указанием названия компании,
        названия вакансии, зарплаты и ссылки на вакансию.

        Использует JOIN для объединения таблиц.

        Returns:
            Список кортежей (компания, вакансия, зарплата_от, зарплата_до, ссылка)
        """
        query = """
            SELECT 
                c.name as company_name,
                v.title as vacancy_title,
                v.salary_from,
                v.salary_to,
                v.url
            FROM vacancies v
            JOIN companies c ON v.company_id = c.company_id
            ORDER BY c.name, v.title
        """
        return self._execute_query(query)

    def get_avg_salary(self) -> Optional[float]:
        """
        Получает среднюю зарплату по вакансиям.

        Использует AVG для подсчета среднего значения.
        Учитывает только вакансии с указанной зарплатой.

        Returns:
            Средняя зарплата или None, если нет данных
        """
        query = """
            SELECT AVG(
                CASE 
                    WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL 
                        THEN (salary_from + salary_to) / 2.0
                    WHEN salary_from IS NOT NULL 
                        THEN salary_from
                    WHEN salary_to IS NOT NULL 
                        THEN salary_to
                    ELSE NULL
                END
            ) as avg_salary
            FROM vacancies
            WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL
        """
        result = self._execute_query(query)
        return result[0][0] if result and result[0][0] else None

    def get_vacancies_with_higher_salary(self) -> List[Tuple]:
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.

        Использует WHERE для фильтрации.

        Returns:
            Список вакансий с зарплатой выше средней
        """
        # Сначала получаем среднюю зарплату
        avg_salary = self.get_avg_salary()

        if avg_salary is None:
            return []

        query = """
            SELECT 
                c.name as company_name,
                v.title as vacancy_title,
                v.salary_from,
                v.salary_to,
                v.currency,
                v.url
            FROM vacancies v
            JOIN companies c ON v.company_id = c.company_id
            WHERE 
                CASE 
                    WHEN v.salary_from IS NOT NULL AND v.salary_to IS NOT NULL 
                        THEN (v.salary_from + v.salary_to) / 2.0 > %s
                    WHEN v.salary_from IS NOT NULL 
                        THEN v.salary_from > %s
                    WHEN v.salary_to IS NOT NULL 
                        THEN v.salary_to > %s
                    ELSE FALSE
                END
            ORDER BY 
                CASE 
                    WHEN v.salary_from IS NOT NULL AND v.salary_to IS NOT NULL 
                        THEN (v.salary_from + v.salary_to) / 2.0
                    WHEN v.salary_from IS NOT NULL 
                        THEN v.salary_from
                    WHEN v.salary_to IS NOT NULL 
                        THEN v.salary_to
                END DESC
        """
        return self._execute_query(query, (avg_salary, avg_salary, avg_salary))

    def get_vacancies_with_keyword(self, keyword: str) -> List[Tuple]:
        """
        Получает список всех вакансий, в названии которых содержится ключевое слово.

        Использует LIKE для поиска по ключевому слову.

        Args:
            keyword: Ключевое слово для поиска

        Returns:
            Список вакансий, содержащих ключевое слово в названии
        """
        query = """
            SELECT 
                c.name as company_name,
                v.title as vacancy_title,
                v.salary_from,
                v.salary_to,
                v.currency,
                v.url
            FROM vacancies v
            JOIN companies c ON v.company_id = c.company_id
            WHERE LOWER(v.title) LIKE LOWER(%s)
            ORDER BY c.name, v.title
        """
        # Добавляем % для поиска по части слова
        search_pattern = f"%{keyword}%"
        return self._execute_query(query, (search_pattern,))

    def __enter__(self):
        """Контекстный менеджер для автоматического подключения."""
        self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Контекстный менеджер для автоматического закрытия соединения."""
        self._close()
