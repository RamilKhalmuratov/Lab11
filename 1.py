import psycopg2
import csv

def connect_db():
    return psycopg2.connect(
        dbname="phonebook_db",
        user="postgres",
        password="22041983re", 
        host="localhost"
    )

def create_table():
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS phonebook (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(50) NOT NULL,
                phone VARCHAR(20) NOT NULL UNIQUE
            );
        """)
        conn.commit()

def create_csv_file():
    try:
        with open('phones.csv', 'x', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['first_name', 'phone'])
    except FileExistsError:
        pass

def insert_from_csv():
    with connect_db() as conn, conn.cursor() as cur:
        try:
            with open('phones.csv', 'r') as f:
                reader = csv.reader(f)
                next(reader)
                for row in reader:
                    cur.execute(
                        "INSERT INTO phonebook (first_name, phone) VALUES (%s, %s) ON CONFLICT (phone) DO NOTHING",
                        row
                    )
            conn.commit()
            print("✅ Данные из CSV добавлены!")
        except FileNotFoundError:
            print("❌ Файл 'phones.csv' не найден!")

def insert_from_console():
    name = input("Введите имя: ")
    phone = input("Введите телефон: ")
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("CALL insert_or_update_user(%s, %s);", (name, phone))
        conn.commit()
        print(f"✅ Контакт '{name}' добавлен/обновлён!")

def bulk_insert():
    users = []
    while True:
        name = input("Имя (или '0' для выхода): ")
        if name == "0": break
        phone = input("Телефон: ")
        users.append([name, phone])
    array_for_sql = [[str(i+1), u[0], u[1]] for i, u in enumerate(users)]
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("CALL bulk_insert_users(%s);", (array_for_sql,))
        conn.commit()
        print("✅ Массовая вставка завершена.")

def update_data():
    user_id = input("Введите ID контакта: ")
    new_name = input("Новое имя (оставьте пустым): ")
    new_phone = input("Новый телефон (оставьте пустым): ")
    with connect_db() as conn, conn.cursor() as cur:
        if new_name:
            cur.execute("UPDATE phonebook SET first_name = %s WHERE id = %s", (new_name, user_id))
        if new_phone:
            cur.execute("UPDATE phonebook SET phone = %s WHERE id = %s", (new_phone, user_id))
        conn.commit()
        print("✅ Данные обновлены!")

def delete_data():
    target = input("Удалить по имени (1) или телефону (2)? ")
    with connect_db() as conn, conn.cursor() as cur:
        if target == "1":
            name = input("Введите имя: ")
            cur.execute("DELETE FROM phonebook WHERE first_name = %s", (name,))
        else:
            phone = input("Введите телефон: ")
            cur.execute("DELETE FROM phonebook WHERE phone = %s", (phone,))
        conn.commit()
        print("✅ Контакт удалён.")

def query_data():
    search = input("Введите имя или телефон для поиска: ")
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM phonebook WHERE first_name LIKE %s OR phone LIKE %s", (f"%{search}%", f"%{search}%"))
        results = cur.fetchall()
        for row in results:
            print(f"ID: {row[0]}, Имя: {row[1]}, Телефон: {row[2]}")
        if not results:
            print("❌ Ничего не найдено.")

def show_all_contacts():
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM phonebook ORDER BY id")
        results = cur.fetchall()
        for row in results:
            print(f"ID: {row[0]}, Имя: {row[1]}, Телефон: {row[2]}")

def paginated_view():
    limit = int(input("Сколько записей выводить? "))
    offset = int(input("С какого номера начать? "))
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM get_contacts_paginated(%s, %s)", (limit, offset))
        results = cur.fetchall()
        for row in results:
            print(f"ID: {row[0]}, Имя: {row[1]}, Телефон: {row[2]}")

def main():
    create_table()
    create_csv_file()
    while True:
        print("\n=== PhoneBook Manager ===")
        print("1. Добавить из CSV\n2. Добавить вручную\n3. Массовая вставка\n4. Обновить\n5. Удалить\n6. Поиск\n7. Показать все\n8. Пагинация\n9. Выход")
        choice = input("Выберите действие: ").strip()
        if choice == "1":
            insert_from_csv()
        elif choice == "2":
            insert_from_console()
        elif choice == "3":
            bulk_insert()
        elif choice == "4":
            update_data()
        elif choice == "5":
            delete_data()
        elif choice == "6":
            query_data()
        elif choice == "7":
            show_all_contacts()
        elif choice == "8":
            paginated_view()
        elif choice == "9":
            print("👋 До свидания!")
            break
        else:
            print("❌ Неверный выбор. Повторите.")

if __name__ == "__main__":
    main()
