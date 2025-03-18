import psycopg2
from config import host, user, password, db_name
from datetime import datetime, date
import random
import time


def benchmark(func):
    """Декоратор для измерения времени выполнения функции"""

    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        execution_time = time.time() - start
        print(f'[*] Время выполнения: {execution_time:.6f} секунд.')
        return result

    return wrapper


class Database:
    """Класс для работы с базой данных PostgreSQL"""

    def __init__(self):
        self.connection = None

    def connect(self):
        """Устанавливает соединение с базой данных"""
        try:
            self.connection = psycopg2.connect(
                host=host, user=user, password=password, database=db_name
            )
            print("[INFO] PostgreSQL подключение установлено")
            return True
        except Exception as ex:
            print(f"[ERROR] Ошибка подключения к PostgreSQL: {ex}")
            return False

    def close(self):
        """Закрывает соединение с базой данных"""
        if self.connection:
            self.connection.close()
            print("[INFO] PostgreSQL connection closed")
            self.connection = None

    def query_the_database(self, *, query, params=None, fetch=False, ex_many=False):
        """Выполняет SQL запрос к базе данных"""
        self.connect()
        view_rows = None

        try:
            with self.connection.cursor() as cursor:
                if params:
                    if ex_many:
                        cursor.executemany(query, params)
                    else:
                        cursor.execute(query, params)
                else:
                    cursor.execute(query)
                    if fetch:
                        view_rows = cursor.fetchall()

            print(f"Выполнен SQL запрос: {query}")
            self.connection.commit()

        except Exception as e:
            print(f"[ERROR] Не удалось выполнить SQL запрос: {e}")
            self.connection.rollback()

        finally:
            self.close()

        return view_rows

    def create_table(self):
        """Создает таблицу сотрудников если она не существует"""
        query = """
            CREATE TABLE IF NOT EXISTS employees (
                id SERIAL PRIMARY KEY,
                full_name VARCHAR(100) NOT NULL,
                birth_date DATE NOT NULL,
                gender VARCHAR(10) NOT NULL
            );
        """
        self.query_the_database(query=query)

    def clear_table(self):
        """Очищает таблицу сотрудников"""
        query = "DELETE FROM employees;"
        try:
            self.query_the_database(query=query)
            print("[INFO] Таблица успешно очищена")
        except Exception as e:
            print(f"[ERROR] Ошибка при очистке таблицы: {e}")

    def create_index(self):
        """Создает индекс для оптимизации поиска по полям gender и full_name"""
        query = """
        CREATE INDEX IF NOT EXISTS idx_gender_fullname ON employees (gender, full_name);
        """
        self.query_the_database(query=query)
        print("[INFO] Создан индекс idx_gender_fullname на полях gender, full_name")

    def drop_index(self):
        """Удаляет индекс для сравнения производительности"""
        query = "DROP INDEX IF EXISTS idx_gender_fullname;"
        self.query_the_database(query=query)
        print("[INFO] Удален индекс idx_gender_fullname")


class Employee:
    """Класс для работы с данными сотрудника"""

    def __init__(self, *, full_name, birth_date, gender):
        self.database = Database()  # Композиция с базой данных
        self.full_name = full_name
        self.birth_date = birth_date
        self.gender = gender

    @property
    def full_name(self):
        return self.__full_name

    @property
    def birth_date(self):
        return self.__birth_date

    @property
    def gender(self):
        return self.__gender

    @full_name.setter
    def full_name(self, full_name):
        """Валидирует и устанавливает полное имя"""
        valid_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

        if full_name is None:
            print("[ERROR] ФИО не заполнено! (None)")
            return

        words = full_name.split()
        if len(words) != 3:
            print("[ERROR] ФИО должно состоять ровно из трех слов (Фамилия Имя Отчество)")
            return

        for word in words:
            if not all(char in valid_chars for char in word):
                print("[ERROR] ФИО должно содержать только английские буквы")
                return

        self.__full_name = full_name.title()

    @birth_date.setter
    def birth_date(self, birth_date):
        """Валидирует и устанавливает дату рождения"""
        if birth_date is None:
            print("[ERROR] Дата рождения не заполнена! (None)")
            return

        try:
            self.__birth_date = datetime.strptime(birth_date, "%Y-%m-%d").date()
        except ValueError:
            print("[ERROR] Формат даты должен быть год-месяц-день (YYYY-MM-DD)")

    @gender.setter
    def gender(self, gender):
        """Валидирует и устанавливает пол"""
        if gender is None:
            print("[ERROR] Пол не заполнен! (None)")
            return

        gender = gender.title()
        if gender not in ["Male", "Female"]:
            print("[ERROR] Пол записывается только значениями 'Male' и 'Female'")
            return

        self.__gender = gender

    def add_employee(self):
        """Добавляет сотрудника в базу данных"""
        try:
            query = """
            INSERT INTO employees (full_name, birth_date, gender) 
            VALUES (%s, %s, %s)
            """
            self.database.query_the_database(
                query=query,
                params=(self.full_name, self.birth_date, self.gender)
            )
            print(f"[INFO] Сотрудник {self.full_name} успешно добавлен")
        except Exception:
            print(f"[ERROR] Не удалось добавить сотрудника, ошибка в введенных данных")

    @staticmethod
    def _generate_random_date():
        """Генерирует случайную дату рождения"""
        year = random.randint(1900, 2025)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        return date(year, month, day)

    @staticmethod
    def generate_batch_employees(*, batch_size, f_surnames=False):
        """Генерирует партию случайных сотрудников"""
        # Списки имен и фамилий
        surname = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez",
                   "Martinez"]
        name_male = ["James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph", "Thomas", "Charles"]
        name_female = ["Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan", "Jessica", "Sarah",
                       "Karen"]
        lastname = ["Edward", "Alexander", "Christopher", "Daniel", "Matthew", "Anthony", "Brian", "Kevin", "Eric",
                    "Ryan"]
        f_surname = ["Foster", "Fletcher", "Ferguson", "Fisher", "Finley", "Ford", "Franklin", "Fitzgerald"]

        batch_data = []

        # Вспомогательная функция для создания сотрудника
        def create_employee(surname_list, name_list, gender):
            full_name = f"{random.choice(surname_list)} {random.choice(name_list)} {random.choice(lastname)}"
            birth_date = Employee._generate_random_date()
            return (full_name, birth_date, gender)

        if f_surnames:
            # Генерируем мужчин с фамилией на F
            for _ in range(batch_size):
                batch_data.append(create_employee(f_surname, name_male, "Male"))
        else:
            # Генерируем обычных сотрудников (50/50 мужчин и женщин)
            for _ in range(batch_size // 2):
                # Мужчина
                batch_data.append(create_employee(surname, name_male, "Male"))
                # Женщина
                batch_data.append(create_employee(surname, name_female, "Female"))

        return batch_data

    def add_one_million_employees(self):
        """Добавляет миллион сотрудников в базу данных партиями"""
        total = 1000000
        batch_size = 100000  # Размер партии
        added = 0
        regular_employees = total - 100  # Обычные сотрудники (без F-фамилий)

        try:
            print(f"[INFO] Начинаем добавление {total} сотрудников партиями по {batch_size}...")

            # Добавляем обычных сотрудников
            while added < regular_employees:
                current_size = min(batch_size, regular_employees - added)

                print(f"[INFO] Генерация партии {added + 1}-{added + current_size}...")
                batch = self.generate_batch_employees(batch_size=current_size)

                query = "INSERT INTO employees (full_name, birth_date, gender) VALUES (%s, %s, %s)"
                self.database.query_the_database(query=query, params=batch, ex_many=True)

                added += current_size
                print(f"[INFO] Прогресс: {added}/{total} ({added / total * 100:.1f}%)")

            # Добавляем 100 мужчин с фамилией на F
            print("[INFO] Добавление 100 мужчин с фамилией на F...")
            f_batch = self.generate_batch_employees(batch_size=100, f_surnames=True)

            query = "INSERT INTO employees (full_name, birth_date, gender) VALUES (%s, %s, %s)"
            self.database.query_the_database(query=query, params=f_batch, ex_many=True)

            added += 100
            print(f"[INFO] Готово! Всего добавлено: {added}/{total} сотрудников.")

        except Exception as e:
            print(f"[ERROR] Не удалось добавить сотрудников: {e}")

    def get_age(self):
        """Возвращает возраст сотрудника в полных годах"""
        return self.calculate_age(birth_date=self.__birth_date)

    @staticmethod
    def calculate_age(*, birth_date):
        """Вычисляет возраст по дате рождения"""
        try:
            today = datetime.now().date()
            age = today.year - birth_date.year
            if (today.month, today.day) < (birth_date.month, birth_date.day):
                age -= 1
            return age
        except Exception as e:
            print(f"[ERROR] Не удалось посчитать возраст: {e}")
            return None


class EmployeeView:
    """Класс для отображения данных о сотрудниках"""

    def __init__(self):
        self.database = Database()

    def get_all_employees(self):
        """Получает список всех сотрудников из БД"""
        query = "SELECT * FROM employees ORDER BY full_name;"
        return self.database.query_the_database(query=query, fetch=True)

    def display_employees_with_age(self):
        """Отображает список сотрудников с их возрастом"""
        employees_data = self.get_all_employees()
        if not employees_data:
            print("[INFO] Список сотрудников пуст")
            return

        # Преобразуем в список списков и добавляем возраст
        employees_list = []
        for employee in employees_data:
            emp_id, full_name, birth_date, gender = employee
            age = Employee.calculate_age(birth_date=birth_date)
            employees_list.append([emp_id, full_name, birth_date, age, gender])

        # Выводим таблицу
        print("\n{:<5} {:<35} {:<15} {:<8} {:<8}".format(
            "ID", "ФИО", "Дата рождения", "Возраст", "Пол"))
        print("-" * 75)

        for emp in employees_list:
            print("{:<5} {:<35} {:<15} {:<8} {:<8}".format(
                emp[0], emp[1], emp[2].strftime("%Y-%m-%d"), emp[3], emp[4]))

    @benchmark
    def find_employee_F_without_index(self):
        """Находит мужчин с фамилией на 'F' без использования индекса"""
        query = """
        SELECT * FROM employees 
        WHERE gender = 'Male' AND full_name LIKE 'F%'
        ORDER BY full_name;
        """
        return self.database.query_the_database(query=query, fetch=True)

    @benchmark
    def find_employee_F_with_index(self):
        """Находит мужчин с фамилией на 'F' с использованием индекса"""
        query = """
        SELECT * FROM employees 
        WHERE gender = 'Male' AND full_name LIKE 'F%'
        ORDER BY full_name;
        """
        return self.database.query_the_database(query=query, fetch=True)


def compare_search_performance():
    """
    Сравнивает производительность поиска сотрудников с индексом и без индекса.
    Показывает разницу во времени выполнения.
    """
    print("\n=== СРАВНЕНИЕ ПРОИЗВОДИТЕЛЬНОСТИ ПОИСКА ===")
    db = Database()
    view = EmployeeView()

    # Выполнение поиска без индекса
    print("\n[INFO] Удаление индекса для чистоты эксперимента...")
    db.drop_index()

    print("[INFO] Выполнение поиска БЕЗ индекса:")
    start_time = time.time()
    result_without_index = view.find_employee_F_without_index()
    time_without_index = time.time() - start_time
    print(f"[INFO] Время без индекса: {time_without_index:.6f} секунд")

    # Выполнение поиска с индексом
    print("\n[INFO] Создание индекса для оптимизации...")
    db.create_index()

    print("[INFO] Выполнение поиска С индексом:")
    start_time = time.time()
    result_with_index = view.find_employee_F_with_index()
    time_with_index = time.time() - start_time
    print(f"[INFO] Время с индексом: {time_with_index:.6f} секунд")

    # Сравнение результатов
    print("\n=== РЕЗУЛЬТАТЫ ОПТИМИЗАЦИИ ===")
    print(f"Найдено сотрудников: {len(result_with_index)}")
    print(f"Время без индекса: {time_without_index:.6f} секунд")
    print(f"Время с индексом: {time_with_index:.6f} секунд")

    return result_with_index


def start_app():
    """Точка входа приложения при запуске через командную строку"""
    import sys

    # Определяем функции для каждого режима работы
    def create_table():
        """Режим 1: Создание таблицы"""
        print("[INFO] Создание таблицы сотрудников...")
        Database().create_table()
        print("[SUCCESS] Таблица успешно создана.")

    def add_employee(args):
        """Режим 2: Добавление сотрудника"""
        # Проверка наличия всех аргументов
        if len(args) < 3:
            print("[ERROR] Недостаточно аргументов.")
            print("Формат: python main.py 2 <Фамилия> <Имя> <Отчество> <ГГГГ-ММ-ДД> <Пол>")
            print("Пример: python main.py 2 Smith John Edward 1985-03-15 Male")
            return

        # Если ФИО передано отдельными аргументами
        if len(args) >= 5:
            full_name = f"{args[0]} {args[1]} {args[2]}"
            birth_date = args[3]
            gender = args[4]
        # Если ФИО передано в кавычках как одна строка
        else:
            full_name = args[0]
            birth_date = args[1]
            gender = args[2]

        # Создаем и добавляем сотрудника
        employee = Employee(full_name=full_name, birth_date=birth_date, gender=gender)
        employee.add_employee()

        # Выводим информацию о добавленном сотруднике
        age = employee.get_age()
        if age is not None:
            print(f"[SUCCESS] Сотрудник {full_name} успешно добавлен. Возраст: {age} лет.")

    def display_employees():
        """Режим 3: Просмотр всех сотрудников"""
        EmployeeView().display_employees_with_age()

    def add_million_employees():
        """Режим 4: Массовое добавление данных"""
        print("[INFO] Начинаем добавление 1,000,000 сотрудников...")
        employee = Employee(full_name="Smith John Edward", birth_date="2000-01-01", gender="Male")
        employee.add_one_million_employees()

    def test_search_speed():
        """Режим 5: Тестирование скорости поиска"""
        print("[INFO] Поиск сотрудников мужского пола с фамилией на 'F'...")
        db = Database()
        db.drop_index()  # Удаляем индекс для чистоты эксперимента

        view = EmployeeView()
        result = view.find_employee_F_without_index()

        print(f"[SUCCESS] Найдено {len(result)} сотрудников.")

        # Показываем результаты
        if result:
            print("\nПервые 10 результатов:")
            print("{:<5} {:<35} {:<15} {:<10}".format("ID", "ФИО", "Дата рождения", "Пол"))
            print("-" * 65)
            for i, emp in enumerate(result[:10], 1):
                print("{:<5} {:<35} {:<15} {:<10}".format(
                    emp[0], emp[1], emp[2].strftime("%Y-%m-%d"), emp[3]))
            if len(result) > 10:
                print(f"... и еще {len(result) - 10} записей")

    def optimize_search():
        """Режим 6: Оптимизация поиска"""
        compare_search_performance()

    # Словарь с доступными режимами и соответствующими функциями
    modes = {
        "1": {"func": create_table, "desc": "Создание таблицы"},
        "2": {"func": add_employee, "desc": "Добавление сотрудника"},
        "3": {"func": display_employees, "desc": "Просмотр всех сотрудников"},
        "4": {"func": add_million_employees, "desc": "Массовое добавление данных"},
        "5": {"func": test_search_speed, "desc": "Тестирование скорости поиска"},
        "6": {"func": optimize_search, "desc": "Оптимизация поиска"},
    }

    # Показываем справку, если не указан режим
    if len(sys.argv) < 2:
        print("\n=== СПРАВОЧНИК СОТРУДНИКОВ ===")
        print("\nДоступные режимы:")
        for key, value in modes.items():
            print(f"  {key} - {value['desc']}")

        print("\nПримеры использования:")
        print("  python main.py 1                           # Создание таблицы")
        print("  python main.py 2 Smith John Edward 1985-03-15 Male  # Добавление сотрудника")
        print("  python main.py 3                           # Просмотр всех сотрудников")
        return

    # Получаем выбранный режим
    mode = sys.argv[1]

    # Проверяем, существует ли выбранный режим
    if mode not in modes:
        print(f"[ERROR] Неизвестный режим: {mode}")
        print("[INFO] Доступные режимы: " + ", ".join(modes.keys()))
        return

    # Запускаем соответствующую функцию
    if mode == "2":  # Для добавления сотрудника нужны дополнительные аргументы
        modes[mode]["func"](sys.argv[2:])
    else:
        modes[mode]["func"]()

if __name__ == "__main__":
    start_app()