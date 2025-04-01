from sqlalchemy import Table, Column, Float, Integer, Date, String, MetaData, text, select, update, delete
from sqlalchemy import create_engine
from datetime import datetime
import csv

def create_tables():
    engine = create_engine('sqlite:///database.db')
    meta = MetaData()

    # Definiujemy strukturę tabel:
    stations = Table(
        "stations",
        meta,
        Column("station", String, primary_key = True), 
        Column("latitude", Float),
        Column("longitude", Float),
        Column("elevation", Float),
        Column("name", String),
        Column("country", String),
        Column("state", String), 
    )

    measure = Table(
        "measure",
        meta,
        Column("station", String),
        Column("date", Date),
        Column("precip", Float),
        Column("tobs", Integer),
    )

    meta.create_all(engine)
    print(f"Wygenerowano tabele: {engine.table_names()}")

    return engine, engine.connect(), stations, measure



def import_data_from_csv(csv_db_stations, csv_db_measure, conn, stations, measure):

    with open(csv_db_stations, newline='', encoding='utf-8') as csv_stations:
        reader = csv.DictReader(csv_stations)
        csv_db_stations_to_insert = []

        for row in reader:
            csv_db_stations_to_insert.append({
                "station": row["station"],
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
                "elevation": float(row["elevation"]),
                "name": row["name"],
                "country": row["country"],
                "state": row["state"]
            })



    with open(csv_db_measure, newline='', encoding='utf-8') as csv_measure:
        reader = csv.DictReader(csv_measure)
        csv_db_measure_to_insert = []

        for row in reader:
            csv_db_measure_to_insert.append({
                "station": row["station"],
                "date": datetime.strptime(row["date"], "%Y-%m-%d").date(),
                "precip": float(row["precip"]),
                "tobs": int(row["tobs"])
            })

    
    ins = stations.insert()
    conn.execute(ins, csv_db_stations_to_insert)
    print(f"Dodano prawidłowo dane z pliku {csv_db_stations} do tabeli: stations!")

    ins = measure.insert()
    conn.execute(ins, csv_db_measure_to_insert)
    print(f"Dodano prawidłowo dane z pliku {csv_db_measure} do tabeli: measure!")


if __name__ == "__main__":
    answer_create_tables = input("Czy chcesz stworzyć tabele o nazwie stations i measure? (T/N): ")
    if answer_create_tables == "T":
        engine, conn, stations, measure = create_tables()
    if answer_create_tables == "N":
        print("Zamykam program.")
        exit


    csv_db_stations = 'clean_stations.csv'
    csv_db_measure = 'clean_measure.csv'

    answer_import_data = input("Czy chcesz zaimportować dane z plików 'clean_stations.csv' i 'clean_measure.csv'? (T/N): ")
    if answer_import_data == "T":
        import_data_from_csv(csv_db_stations, csv_db_measure, conn, stations, measure)
    if answer_import_data == "N":
        print("Zamykam program.")
        exit


    while True:
        type_sql = int(input("Podaj rodzaj zapytanie sql wpisując cyfrę: 1 - Select_all, 2 - Select_limit, 3 - Update, 4 - Delete, 5 - Własna treść zapytania, 0 - Zakończ program: "))
        description = ("Select_all" if type_sql == 1 else 
            "funkcję: Select_limit" if type_sql == 2 else 
            "funkcję: Delete"if type_sql == 3 else 
            "funkcję: Update" if type_sql == 4 else 
            "własna treść zapytania" if type_sql == 5 else
            "zakończenie programu." if type_sql == 0 else
            "złą odpowiedź! ;D - prawidłowa to od 0 do 5")

        print(f"Wybrano {description} ")

        if type_sql == 0:
            print("Zamykam program!")
            break

        if type_sql == 1:
            table_name = input(f"Podaj nazwę tabeli którem na dotyczyć zapytanie {description}, tabele: {engine.table_names()} :")
            if table_name == "stations":
                sql = stations.select()
            elif table_name == "measure":
                sql = measure.select()
            else:
                print("Niepoprawna nazwa tabeli.")
                exit()
            result = conn.execute(sql)
            for row in result:
                print(row)

        if type_sql == 2:
            table_name = input(f"Podaj nazwę tabeli którem na dotyczyć zapytanie {description}, tabele: {engine.table_names()} :")
            limit = int(input(f"Do ilu wyników ograniczyć zapytanie dla tabeli {table_name}? :"))
            if table_name == "stations":
                sql = stations.select().limit(limit)
            elif table_name == "measure":
                sql = measure.select().limit(limit)
            else:
                print("Niepoprawna nazwa tabeli.")
                exit()
            result = conn.execute(sql).fetchall()
            for row in result:
                print(row)

        if type_sql == 3:
            table_name = input(f"Podaj nazwę tabeli którem na dotyczyć zapytanie {description}, tabele: {engine.table_names()} :")
            chosen_column = input(f"Podaj nazwę kolumny której będzie dotyczyła funkcja UPDATE: ")
            old_data = input(f"Dla jakiej wartości dokonać zmiany: ")
            new_date = input(f"Podaj nową wartość: ")
            if table_name == "stations":
                table = stations
            elif table_name == "measure":
                table = measure
            else:
                print("Niepoprawna nazwa tabeli.")
                exit()
   
            if chosen_column in ("latitude", "longitude", "elevation", "precip"):
                old_data = float(old_data)
                new_date = float(new_date)
            if chosen_column == "tobs":
                old_data = int(old_data)
                new_date = int(new_date)
            if chosen_column == "date":
                old_data = datetime.strptime(old_data, "%Y-%m-%d").date()
                new_data = datetime.strptime(new_data, "%Y-%m-%d").date()
   
            sql = (update(table)
                .where(table.c[chosen_column] == old_data)
                .values({chosen_column: new_date})
            )
            conn.execute(sql)
            print(f"Zmian dokonano w tabeli {table_name} w kolumnie {chosen_column}, zmienono wartość {old_data} na wartość {new_date}")
   
        if type_sql == 4:
            table_name = input(f"Podaj nazwę tabeli którem na dotyczyć zapytanie {description}, tabele: {engine.table_names()} :")
            chosen_column = input(f"Podaj nazwę kolumny której będzie dotyczyła funkcja DELETE: ")
            delete_data = input(f"Dla jakiej wartości dokonać DELETE? : ")
            if table_name == "stations":
                table = stations
            elif table_name == "measure":
                table = measure
            else:
                print("Niepoprawna nazwa tabeli.")
                exit()
   
            if chosen_column == "latitude" or "longitude" or "elevation" or "precip":
                delete_data = float(delete_data)
            if chosen_column == "tobs":
                delete_data = int(delete_data)
            if chosen_column == "date":
                delete_data = datetime.strptime(delete_data, "%Y-%m-%d").date()
   
            sql = (delete(table)
                .where(table.c[chosen_column] == delete_data))
   
            conn.execute(sql)
            print(f"Funkcji DELETE dokonano w tabeli {table_name} w kolumnie {chosen_column}, dla wartość {delete_data}")

        if type_sql == 5:
            tresc_sql = input("Podaj pełną treść zapytania SQL:")
            result = conn.execute(text(tresc_sql)).fetchall()

            print(f'Wynik zapytania"{tresc_sql}":')
            for row in result:
                print(row)
        
        input("\nWciśnij Enter, aby kontynuować...")