import time
from typing import Any, Dict, List, Optional

import requests


class HeadHunterAPI:
    """
    Класс для работы с API HeadHunter.

    Позволяет получать информацию о компаниях и их вакансиях.
    Соблюдает принцип единственной ответственности (SRP) - только работа с API.
    """

    BASE_URL = "https://api.hh.ru"

    def __init__(self):
        """Инициализация клиента API."""
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

    def get_company(self, company_id: int) -> Optional[Dict[str, Any]]:
        """
        Получает информацию о компании по ID.

        Args:
            company_id: ID компании на hh.ru

        Returns:
            Словарь с данными компании или None в случае ошибки
        """
        try:
            time.sleep(0.3)  # Задержка для соблюдения лимитов API

            response = self.session.get(f"{self.BASE_URL}/employers/{company_id}")

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                print(f"Компания с ID {company_id} не найдена")
                return None
            else:
                print(
                    f"Ошибка при получении компании {company_id}: HTTP {response.status_code}"
                )
                return None

        except requests.RequestException as e:
            print(f"Ошибка при получении компании {company_id}: {e}")
            return None

    def search_companies(
        self, query: str, per_page: int = 10, area: int = 113
    ) -> List[Dict[str, Any]]:
        """
        Ищет компании по названию.

        Args:
            query: Поисковый запрос (название компании)
            per_page: Количество результатов на странице (макс. 100)
            area: ID региона (113 - Россия, можно не указывать для всех стран)

        Returns:
            Список найденных компаний
        """
        try:
            time.sleep(0.3)

            params = {
                "text": query,
                "per_page": per_page,
                "sort": "by_vacancies_open",
                "area": area,
            }

            response = self.session.get(f"{self.BASE_URL}/employers", params=params)

            if response.status_code == 200:
                data = response.json()
                return data.get("items", [])
            else:
                print(
                    f"Ошибка при поиске компаний '{query}': HTTP {response.status_code}"
                )
                return []

        except requests.RequestException as e:
            print(f"Ошибка при поиске компаний '{query}': {e}")
            return []

    def get_company_vacancies(
        self, company_id: int, per_page: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Получает список вакансий компании.

        Args:
            company_id: ID компании на hh.ru
            per_page: Количество вакансий на странице (макс. 100)

        Returns:
            Список вакансий компании
        """
        vacancies = []
        page = 0

        try:
            while True:
                time.sleep(0.3)

                params = {
                    "employer_id": company_id,
                    "per_page": per_page,
                    "page": page,
                    "only_with_salary": False,
                    "area": 113,  # Россия
                }

                response = self.session.get(f"{self.BASE_URL}/vacancies", params=params)

                if response.status_code == 200:
                    data = response.json()
                    items = data.get("items", [])

                    if not items:
                        break

                    vacancies.extend(items)

                    if page >= data.get("pages", 0) - 1:
                        break

                    page += 1
                else:
                    print(
                        f"Ошибка при получении вакансий компании {company_id}: HTTP {response.status_code}"
                    )
                    break

        except requests.RequestException as e:
            print(f"Ошибка при получении вакансий компании {company_id}: {e}")

        return vacancies

    def get_popular_companies(
        self, per_page: int = 20, area: int = 113
    ) -> List[Dict[str, Any]]:
        """
        Получает список популярных компаний (с наибольшим количеством открытых вакансий).

        Args:
            per_page: Количество компаний
            area: ID региона (113 - Россия)

        Returns:
            Список популярных компаний
        """
        try:
            time.sleep(0.3)

            params = {"per_page": per_page, "sort": "by_vacancies_open", "area": area}

            response = self.session.get(f"{self.BASE_URL}/employers", params=params)

            if response.status_code == 200:
                data = response.json()
                return data.get("items", [])
            else:
                print(
                    f"Ошибка при получении популярных компаний: HTTP {response.status_code}"
                )
                return []

        except requests.RequestException as e:
            print(f"Ошибка при получении популярных компаний: {e}")
            return []

    def close(self):
        """Закрывает сессию."""
        self.session.close()
