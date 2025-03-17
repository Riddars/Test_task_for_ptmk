from idlelib import query

import psycopg2
from config import host, user, password, db_name
from datetime import datetime, date


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

    def query_the_database(self, *, query: str, params = None, Fetch = False):
        self.connect()
        view_rows = None
        try:
            with self.connection.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                    if Fetch:
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

    def get_age(self):
        print(self.calculate_age(self.__birth_date))
        return self.calculate_age(self.__birth_date)

    def calculate_age(self, birth_date):
        """Вычисляет возраст по дате рождения (может использоваться без создания объекта)"""
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
        self.employee = Employee(full_name = None, birth_date = None, gender = None)

    def get_all_employees(self):
        query = """
        SELECT * FROM employees ORDER BY full_name;
        """
        list_with_employees = self.database.query_the_database(query=query, params=None, Fetch = True)
        return list_with_employees

    def display_employees_with_age(self):
        today = datetime.now().date()
        # Unpacking списка с кортежами с информацией о сотрудниках
        employees_list = []
        for i in self.get_all_employees():  # [(id, ФИО, год рождения, пол), (id, ФИО, год рождения, пол) ...]
            employees_list.append(list(i))  # [[id, ФИО, год рождения, пол], [id, ФИО, год рождения, пол] ...]

        for employee in employees_list:  #
            full_name, birth_date, gender = employee[1], employee[2], employee[3]
            age = self.employee.calculate_age(birth_date = birth_date)
            employee.insert(3, age)
        #     age = today.year - employee[2].year
        #     if (today.month, today.day) < (employee[2].month, employee[2].day):
        #         age -= 1
        #     employee.insert(3, age)  # вставляем возраст после даты рождения
        #
        print("\n{:<5} {:<35} {:<15} {:<8} {:<6}".format("ID", "ФИО", "Дата рождения", "Возраст", "Пол"))
        print("-" * 75)
        for e in employees_list:
            print("{:<5} {:<35} {:<15} {:<8} {:<6}".format(e[0], e[1], e[2].strftime("%Y-%m-%d"), e[3], e[4]))


task1 = Database()
task1.create_table()

employee1 = Employee(full_name='Serddddov Petr Sergeevich', birth_date='2001-06-20', gender='male')
employee1.get_age()

employee2 = EmployeeView()
employee2.display_employees_with_age()

