import pandas as pd
import psycopg2
import os
import sqlalchemy

conn = psycopg2.connect(database="postgres", user='postgres', password=os.environ.get("pg_psw"), host='localhost', port= '5432')

conn.autocommit = True

cursor = conn.cursor()

sql = '''CREATE database vélos_nantesmetropole''';

cursor.execute(sql)
print("Base de données créée avec succès !")

conn.close()

conn = psycopg2.connect(database="vélos_nantesmetropole", user='postgres', password=os.environ.get("pg_psw"), host='localhost', port= '5432')

conn.autocommit = True

cursor = conn.cursor()

create_tables_sql = '''CREATE TABLE IF NOT EXISTS Station (
    id INTEGER PRIMARY KEY,
    nom VARCHAR,
    longitude FLOAT,
    latitude FLOAT,
    organisme VARCHAR);

    CREATE TABLE IF NOT EXISTS Meteo (
    id SERIAL PRIMARY KEY,
    date VARCHAR,
    t_min FLOAT,
    t_max FLOAT,
    t_moy FLOAT);
    
    CREATE TABLE IF NOT EXISTS Type (
    id INTEGER PRIMARY KEY,
    libellé VARCHAR);
    
    CREATE TABLE IF NOT EXISTS Mesure (
    id SERIAL PRIMARY KEY,
    date VARCHAR,
    id_station INTEGER,
    valeur FLOAT,
    heure TIME,
    type INTEGER,
    jour VARCHAR,
    FOREIGN KEY (id_station) REFERENCES Station(id),
    FOREIGN KEY (type) REFERENCES Type(id));''';

cursor.execute(create_tables_sql)
print("Tables créées avec succès !")

sql_inserer_stations = """
    COPY Station (id, Nom, Longitude, Latitude, Organisme)
    FROM 'C:/Users/Public/Data/Stations_velos.csv'
    WITH csv header ENCODING 'UTF-8' DELIMITER ',' QUOTE '"';
"""

cursor.execute(sql_inserer_stations)
print("Insertion effectuée avec succès !")

sql_inserer_types = """
    COPY Type (id, libellé)
    FROM 'C:/Users/Public/Data/Types.csv'
    WITH csv header ENCODING 'UTF-8' DELIMITER ',' QUOTE '"';
"""

cursor.execute(sql_inserer_types)
print("Insertion effectuée avec succès !")

sql_inserer_meteo = """
    COPY Meteo (date, t_min, t_max, t_moy)
    FROM 'C:/Users/Public/Data/Meteo.csv'
    WITH csv header ENCODING 'UTF-8' DELIMITER ',' QUOTE '"';
"""

cursor.execute(sql_inserer_meteo)
print("Insertion effectuée avec succès !")

sql_inserer_mesures = """
    COPY Mesure (id_station, date, valeur, heure, type, jour)
    FROM 'C:/Users/Public/Data/Mesures_velos.csv'
    WITH csv header ENCODING 'UTF-8' DELIMITER ',' QUOTE '"';
"""

cursor.execute(sql_inserer_mesures)
print("Insertion effectuée avec succès !")

sql_creation_mesures_jours = '''CREATE VIEW Mesures_quotidiennes (
        Id_station,
        Nom_station,
        Date,
        Valeur)
    AS
		SELECT id_station,
            nom,
			date,
            sum(valeur)
		FROM Mesure
        INNER JOIN Station ON Mesure.id_station = Station.id
        GROUP BY id_station, nom, date'''

cursor.execute(sql_creation_mesures_jours)
print("Vue créée avec succès !")