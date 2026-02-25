from src.utils.config_loader import DatabaseConfig
from src.db.db_creator import DatabaseCreator
from src.db.db_manager import DBManager


def print_header(text: str) -> None:
    """Выводит заголовок секции."""
    print("\n" + "-" * 50)
    print(f" {text} \n")


def print_result(title: str, data: list, headers: tuple = None) -> None:
    """
    Выводит результат запроса в читаемом формате.

    Args:
        title: Заголовок секции
        data: Данные для вывода
        headers: Заголовки колонок (опционально)
    """
    print_header(title)

    if not data:
        print("Нет данных для отображения")
        return

    if headers:
        # Выводим заголовки
        header_line = " | ".join(str(h) for h in headers)
        print(header_line)
        print("-" * len(header_line))

    # Выводим данные
    for row in data:
        # Форматируем каждую строку для читаемости
        formatted_row = []
        for item in row:
            if item is None:
                formatted_row.append("Не указана")
            elif isinstance(item, (int, float)) and item > 1000:
                # Форматируем большие числа
                formatted_row.append(f"{item:,.0f}".replace(",", " "))
            else:
                formatted_row.append(str(item))

        print(" | ".join(formatted_row))

    print(f"\nВсего записей: {len(data)}")


def main():
    """Основная функция программы."""

    # Список на hh.ru
    # Проверенные рабочие ID
    COMPANY_IDS = [
        1740,  # Яндекс
        3529,  # СберТех (Сбер)
        78638,  # Тинькофф
        15478,  # VK
        2180,  # Ozon
        816144,  # ВкусВилл
        5744540,  # Онлайн-школа Фоксфорд
        4565267,  # Газпром
        63742,  # Лукойл
        3166753,  # KiberOne
    ]

    # Инициализация конфигурации
    config = DatabaseConfig()

    print_header("ПАРСЕР ВАКАНСИЙ С HH.RU")
    print("Добро пожаловать в программу для работы с вакансиями!")
    print(f"Будет загружено {len(COMPANY_IDS)} компаний")
    print(f"\nКонфигурация базы данных:")
    print(f"  База данных: {config.db_name}")
    print(f"  Пользователь: {config.user}")
    print(f"  Хост: {config.host}:{config.port}")

    # Создание и заполнение базы данных
    print_header("ЭТАП 1: ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ")

    creator = DatabaseCreator(config)

    print("1. Создание базы данных...")
    creator.create_database()

    print("2. Создание таблиц...")
    creator.create_tables()

    print("3. Загрузка информации о компаниях...")
    creator.fill_companies(COMPANY_IDS)

    print("4. Загрузка вакансий...")
    creator.fill_vacancies(COMPANY_IDS)

    print("\nБаза данных успешно инициализирована!")

    # Работа с данными через DBManager
    print_header("ЭТАП 2: АНАЛИЗ ДАННЫХ")

    with DBManager(config) as db:

        while True:
            print("\nВыберите действие:")
            print("1 - Список компаний и количество вакансий")
            print("2 - Список всех вакансий")
            print("3 - Средняя зарплата по вакансиям")
            print("4 - Вакансии с зарплатой выше средней")
            print("5 - Поиск вакансий по ключевому слову")
            print("0 - Выход")

            choice = input("\nВаш выбор: ").strip()

            if choice == "1":
                # Компании и количество вакансий
                data = db.get_companies_and_vacancies_count()
                print_result(
                    "КОМПАНИИ И КОЛИЧЕСТВО ВАКАНСИЙ",
                    data,
                    ("Компания", "Количество вакансий"),
                )

            elif choice == "2":
                # Все вакансии
                data = db.get_all_vacancies()
                print_result(
                    "ВСЕ ВАКАНСИИ",
                    data,
                    ("Компания", "Вакансия", "З/п от", "З/п до", "Ссылка"),
                )

            elif choice == "3":
                # Средняя зарплата
                avg_salary = db.get_avg_salary()
                print_header("СРЕДНЯЯ ЗАРПЛАТА")
                if avg_salary:
                    print(
                        f"Средняя зарплата по всем вакансиям: {avg_salary:,.0f} руб.".replace(
                            ",", " "
                        )
                    )
                else:
                    print("Нет данных для расчета средней зарплаты")

            elif choice == "4":
                # Вакансии с зарплатой выше средней
                data = db.get_vacancies_with_higher_salary()
                print_result(
                    "ВАКАНСИИ С ЗАРПЛАТОЙ ВЫШЕ СРЕДНЕЙ",
                    data,
                    ("Компания", "Вакансия", "З/п от", "З/п до", "Валюта", "Ссылка"),
                )

            elif choice == "5":
                # Поиск по ключевому слову
                keyword = input("Введите ключевое слово для поиска: ").strip()
                if keyword:
                    data = db.get_vacancies_with_keyword(keyword)
                    print_result(
                        f"ВАКАНСИИ, СОДЕРЖАЩИЕ '{keyword}'",
                        data,
                        (
                            "Компания",
                            "Вакансия",
                            "З/п от",
                            "З/п до",
                            "Валюта",
                            "Ссылка",
                        ),
                    )
                else:
                    print("Ключевое слово не может быть пустым")

            elif choice == "0":
                print("\nСпасибо за использование программы! До свидания!")
                break

            else:
                print("Неверный выбор. Пожалуйста, выберите пункт из меню.")

            input("\nНажмите Enter, чтобы продолжить...")


if __name__ == "__main__":
    main()
