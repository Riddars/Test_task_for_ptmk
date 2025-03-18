import psycopg2
from config import host, user, password, db_name
from datetime import datetime, date
import random
import time

def benchmark(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print('[*] Время выполнения: {} секунд.'.format(end - start))
        return result
    return wrapper

class Database:
    def __init__(self):
        self.connection = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(host=host, user=user, password=password, database=db_name)
            print("[INFO] PostgreSQL подключение установлено")
            return True
        except Exception as _ex:
            print("[INFO] Error while working PostgreSQL", _ex)
            return False

    def close(self):
        if self.connection:
            self.connection.close()
            print("[INFO] PostgreSQL connection closed")
            self.connection = None

    def query_the_database(self, *, query: str, params = None, fetch = False, ex_many = False):
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
            print(f"Не удалось выполнить SQL запрос: {query}. Ошибка: {e}")
            self.connection.rollback()
        self.close()
        return view_rows

    def create_table(self):
        query = """
            CREATE TABLE IF NOT EXISTS employees (
                id SERIAL PRIMARY KEY,
                full_name VARCHAR(100) NOT NULL,
                birth_date DATE NOT NULL,
                gender VARCHAR(10) NOT NULL
            );
        """
        self.query_the_database(query = query, params = None)

    def clear_table(self):
        try:
            query = """
            DELETE FROM employees;
            """
            self.query_the_database(query = query)
            print("Таблица очищена")
        except:
            print("При попытке очистить таблицу произошла ошибка")




class Employee:
    def __init__(self, *, full_name, birth_date, gender):
        self.database = Database() # Создаём композицию чтобы не наследовать все атрибуты и методы Database
        # приватные переменные инициализируются в setter
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
        valid_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        if full_name is None:
            print("ФИО не заполнено! (None)")
            return

        words = full_name.split()

        if len(words) != 3:
            print("ФИО должно состоять ровно из трех слов (Фамилия Имя Отчество) на английском языке")
            return

        for word in words:
            for char in word:
                if char not in valid_chars:
                    print("ФИО должно содержать только английские буквы")
                    return

        full_name = full_name.title()
        self.__full_name = full_name

    @birth_date.setter
    def birth_date(self, birth_date):
        if birth_date is None:
            print("Дата рождения не заполнена! (None)")
            return
        try:
            self.__birth_date = datetime.strptime(birth_date, "%Y-%m-%d").date()
        except ValueError:
            print("Формат даты должен быть год-месяц-день")

    @gender.setter
    def gender(self, gender):
        if gender is None:
            print("Пол не заполнен! (None)")
            return
        gender = gender.title()
        if gender != "Male" and gender != "Female":
            print("Пол записывается только значениями 'Male' и 'Female'")
            return
        self.__gender = gender

    def add_employee(self):
        try:
            query = """
            INSERT INTO employees (full_name, birth_date, gender) VALUES (%s, %s, %s)
            """
            self.database.query_the_database(query = query, params = (self.full_name, self.birth_date, self.gender))
        except:
            print(f"Не удалось добавить сотрудника в таблицу, ошибка в ведённых данных")

    @staticmethod
    def generate_one_million_employees():
        surname = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]

        name_male = ["James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph", "Thomas", "Charles"]
        name_female = ["Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan", "Jessica", "Sarah", "Karen"]

        lastname = ["Edward", "Alexander", "Christopher", "Daniel", "Matthew", "Anthony", "Brian", "Kevin", "Eric", "Ryan"]

        f_surname = ["Foster", "Fletcher", "Ferguson", "Fisher", "Finley", "Ford", "Franklin", "Fitzgerald"]

        result_full_name = []

        for i in range(499950):
            result_full_name.append(random.choice(surname) + " " + random.choice(name_male) + " " + random.choice(lastname))
            result_full_name.append(random.choice(surname) + " " + random.choice(name_female) + " " + random.choice(lastname))

        for i in range(100):
            result_full_name.append(random.choice(f_surname) + " " + random.choice(name_male) + " " + random.choice(lastname))

        #-------------

        result_birth_date = []
        for i in range(1000000):
            year = random.randint(1900, 2025)
            month = random.randint(1, 12)
            day = random.randint(1, 28)
            result_birth_date.append(date(year, month, day))

        #------------
        result_gender = []
        for i in range(499950):
            result_gender.append("Male")
            result_gender.append("Female")

        for i in range(100):
            result_gender.append("Male")

        result_billion_employees = []
        for i in range(len(result_full_name)):
            result_billion_employees.append((result_full_name[i], result_birth_date[i], result_gender[i]))

        return result_billion_employees

    def add_one_million_employees(self):
        try:
            query = """
            INSERT INTO employees (full_name, birth_date, gender) VALUES (%s, %s, %s)
            """
            self.database.query_the_database(query = query, params = self.generate_one_billion_employees(), ex_many = True)
        except:
            print(f"Не удалось добавить миллион сотрудников в таблицу, ошибка в ведённых данных")

    def get_age(self):
        return self.calculate_age(birth_date = self.__birth_date)

    @staticmethod
    def calculate_age(*, birth_date):
        # Сделал статичным потому что в целом self тут не нужен, а рассчёт лет нужен в методе display_employees_with_age
        # вообщем чтобы не дублировать код
        try:
            today = datetime.now().date()
            age = today.year - birth_date.year
            if (today.month, today.day) < (birth_date.month, birth_date.day):
                age -= 1
            return age
        except Exception as e:
            print(f"Не удалось посчитать возраст: {e}")
            return None



class EmployeeView:
    def __init__(self):
        self.database = Database() # создаём композицию

    def get_all_employees(self):
        query = """
        SELECT * FROM employees ORDER BY full_name;
        """
        list_with_employees = self.database.query_the_database(query=query, fetch = True)
        return list_with_employees

    def display_employees_with_age(self):
        # Unpacking списка с кортежами с информацией о сотрудниках
        employees_list = []
        for i in self.get_all_employees():  # [(id, ФИО, год рождения, пол), (id, ФИО, год рождения, пол) ...]
            employees_list.append(list(i))  # [[id, ФИО, год рождения, пол], [id, ФИО, год рождения, пол] ...]

        for employee in employees_list:  #
            full_name, birth_date, gender = employee[1], employee[2], employee[3]
            age = Employee.calculate_age(birth_date = birth_date)
            employee.insert(3, age)

        print("\n{:<5} {:<35} {:<15} {:<8} {:<6}".format("ID", "ФИО", "Дата рождения", "Возраст", "Пол"))
        print("-" * 75)
        for e in employees_list:
            print("{:<5} {:<35} {:<15} {:<8} {:<6}".format(e[0], e[1], e[2].strftime("%Y-%m-%d"), e[3], e[4]))

    @benchmark
    def find_employee_F(self):
        query = """
        SELECT * FROM employees 
        WHERE gender = 'Male' AND full_name LIKE 'F%'
        ORDER BY full_name;
        """
        return self.database.query_the_database(query=query, fetch = True)






# def test_application():
#     print("=== Тестирование приложения ===")
#
#     # Тест 1: Создание таблицы
#     print("\n--- Тест 1: Создание таблицы ---")
#     db = Database()
#     db.clear_table()
#     db.create_table()
#     # Тест 2: Добавление сотрудников
#     print("\n--- Тест 2: Добавление сотрудников ---")
#     employee1 = Employee(full_name='Serdov Petr Sergeevich', birth_date='2001-06-20', gender='Male')
#     employee1.add_employee()
#
#     employee2 = Employee(full_name='Ivanov Ivan Ivanovich', birth_date='1990-01-15', gender='Male')
#     employee2.add_employee()
#
#     employee3 = Employee(full_name='Petrova Anna Mikhailovna', birth_date='1985-12-03', gender='Female')
#     employee3.add_employee()
#
#     # Тест валидации данных
#     print("\nПроверка валидации данных:")
#     invalid_employee = Employee(full_name='Invalid Name123', birth_date='not-a-date', gender='Unknown')
#     invalid_employee.add_employee()
#
#     # Тест 3: Расчет возраста
#     print("\n--- Тест 3: Расчет возраста ---")
#     employee1.get_age()
#     employee2.get_age()
#     employee3.get_age()
#
#     # Тест 4: Отображение всех сотрудников
#     print("\n--- Тест 4: Отображение всех сотрудников ---")
#     view = EmployeeView()
#     view.display_employees_with_age()
#
#
#     print(view.find_employee_F())

def start_app():
    db = Database()

    while True:
        print("\nСИСТЕМА УПРАВЛЕНИЯ СОТРУДНИКАМИ")
        print("1. Создание таблицы")
        print("2. Добавление одного сотрудника")
        print("3. Просмотр всех сотрудников")
        print("4. Массовое добавление данных")
        print("5. Тестирование скорости поиска")
        print("0. Выход")

        choice = input("\nВыберите режим: ")

        if choice == "0":
            print("Программа завершена.")
            break

        elif choice == "1":
            # Создание таблицы
            print("\nСоздание таблицы employees...")
            db.create_table()
            print("Таблица успешно создана!")

        elif choice == "2":
            # Добавление одного сотрудника
            print("\nДобавление нового сотрудника")
            print("Введите данные сотрудника:")
            full_name = input("ФИО (например, Smith John Edward): ")
            birth_date = input("Дата рождения (ГГГГ-ММ-ДД): ")
            gender = input("Пол (Male/Female): ")

            employee = Employee(full_name=full_name, birth_date=birth_date, gender=gender)
            employee.add_employee()

            age = employee.get_age()
            if age:
                print(f"Сотрудник добавлен. Возраст: {age} лет")

        elif choice == "3":
            # Просмотр всех сотрудников
            print("\nСписок всех сотрудников:")
            view = EmployeeView()
            view.display_employees_with_age()

        elif choice == "4":
            # Массовое добавление данных
            print("\nГенерация и добавление 1,000,000 сотрудников...")
            print("Это может занять некоторое время...")

            # Создаем экземпляр с корректным ФИО
            employee = Employee(full_name="Smith John Edward", birth_date="2000-01-01", gender="Male")

            try:
                # Получаем данные и вставляем их в базу
                employees_data = employee.generate_one_million_employees()
                if employees_data:
                    query = """
                    INSERT INTO employees (full_name, birth_date, gender) VALUES (%s, %s, %s)
                    """
                    db.query_the_database(query=query, params=employees_data, ex_many=True)
                    print("Миллион сотрудников успешно добавлен!")
                else:
                    print("Ошибка: не удалось сгенерировать данные.")
            except Exception as e:
                print(f"Ошибка при добавлении сотрудников: {e}")

        elif choice == "5":
            # Тестирование скорости поиска
            print("\nПоиск всех мужчин с фамилией на букву 'F'...")
            view = EmployeeView()
            result = view.find_employee_F()
            print(f"Поиск завершен. Найдено {len(result)} сотрудников.")

            if result:
                print("\nРЕЗУЛЬТАТЫ ПОИСКА:")
                for i, emp in enumerate(result[:10], 1):
                    print(f"{i}. ID: {emp[0]}, ФИО: {emp[1]}, Дата: {emp[2].strftime('%Y-%m-%d')}, Пол: {emp[3]}")
                if len(result) > 10:
                    print(f"... и ещё {len(result) - 10} записей")

        else:
            print("Неверный выбор. Пожалуйста, выберите пункт из меню.")


if __name__ == "__main__":
    start_app()