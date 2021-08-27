import pandas as pd
import csv
import sqlalchemy
import sqlite3
import os
import psycopg2
import matplotlib.pyplot as plt
import numpy as np
import dash
import dash_core_components as dcc
import dash_html_components as html
from collections import Counter
from sqlalchemy import create_engine
import plotly.express as px

conn = sqlite3.connect('vélos_nantesmetropole.db')

ma_base_donnees = "vélos_nantesmetropole"
utilisateur = "postgres"
mot_passe = os.environ.get("pg_psw")

datas = pd.read_csv("C:/Users/levog/Simplon/projet-chef-d-oeuvre/Data/output/Mesures_quotidiennes.csv")
datas["date"] = pd.to_datetime(datas["date"], format="%Y-%m-%d")
datas.sort_values("date", inplace=True)

def ouvrir_connection(nom_bdd, utilisateur, mot_passe, host='localhost', port=5432):
    
    try:
        conn = psycopg2.connect(dbname=nom_bdd, user=utilisateur, password=mot_passe, host=host, port=5432)
    except psycopg2.Error as e:
        print("Erreur lors de la connection à la base de données")
        print(e)
        return None
    conn.set_session(autocommit=True)
    
    return conn

conn = ouvrir_connection(ma_base_donnees, utilisateur, mot_passe)

#Graphique n°1
evolution = '''SELECT date, avg(valeur), count(*) AS count
FROM Mesures_quotidiennes
WHERE date >= '2014-01-01'
GROUP BY date
ORDER BY date
'''

evolution = pd.read_sql(evolution, conn)

fig1 = px.line(evolution, x='date', y='avg', labels={"avg": "Mesures", "date": "Date"}, title='Evolution globale de la tendance (moyenne quotidienne par station)')
fig1.update_xaxes(
    rangeslider_visible=True,
    rangeselector=dict(
        buttons=list([
            dict(count=1, label="1m", step="month", stepmode="backward"),
            dict(count=6, label="6m", step="month", stepmode="backward"),
            dict(count=1, label="1a", step="year", stepmode="backward"),
            dict(step="all")
        ])
    )
)

#Graphique n°2
ordre_stations = '''SELECT nom, CAST(ROUND(AVG(CAST(valeur AS DECIMAL))) AS INTEGER) AS moyenne, id
FROM station
INNER JOIN mesures_quotidiennes ON station.id=mesures_quotidiennes.id_station 
GROUP BY station.id
ORDER BY moyenne desc
LIMIT 10'''

ordre_stations = pd.read_sql(ordre_stations, conn)
ordre_stations

fig2 = px.bar(ordre_stations, x="nom", y="moyenne", color="moyenne", text='moyenne', labels={"moyenne": "Mesures", "nom": "Station"}, title="Les 10 stations les plus fréquentées (mesures quotidiennes moyennes par station)")

#Graphique n°3
jours_semaine = '''
SELECT mesure.jour, CAST(ROUND(AVG(CAST(mesures_quotidiennes.valeur AS DECIMAL))) AS INTEGER) AS moyenne
FROM mesures_quotidiennes
INNER JOIN mesure ON mesures_quotidiennes.date = mesure.date
INNER JOIN (
           SELECT 1 AS num_jour, 'Monday' AS jour
           UNION
           SELECT 2 AS num_jour, 'Tuesday' AS jour
           UNION
           SELECT 3 AS num_jour, 'Wednesday' AS jour
           UNION
           SELECT 4 AS num_jour, 'Thursday' AS jour
           UNION
           SELECT 5 AS num_jour, 'Friday' AS jour
           UNION
           SELECT 6 AS num_jour, 'Saturday' AS jour
           UNION
           SELECT 7 AS num_jour, 'Sunday' AS jour
            ) J ON mesure.jour = J.jour
GROUP BY mesure.jour, j.num_jour
ORDER BY j.num_jour'''

jours_semaine = pd.read_sql(jours_semaine, conn)
jours_semaine

fig3 = px.bar(jours_semaine, x="jour", y="moyenne", text='moyenne', labels={"moyenne": "Mesures", "jour": "Jour de la semaine"}, title="Fréquentation par jour de semaine (mesures quotidiennes moyennes par jour)")

#Graphique n°4
type_velo_14 = '''
SELECT date, type, libellé, avg(valeur)
FROM Mesure
INNER JOIN Type ON Mesure.type = Type.id
WHERE heure = '14:00:00'
GROUP BY date, type, libellé, heure
ORDER BY date
'''

type_velo_14 = pd.read_sql(type_velo_14, conn)
type_velo_14

fig4 = px.area(type_velo_14, x="date", y="avg", labels={"avg": "Mesures moyennes par station", "date": "Date"}, color="libellé", title = 'Evolution des types de vélos (mesures prises à 14h)')

#Graphique n°5
type_velo_17 = '''
SELECT date, type, libellé, avg(valeur)
FROM Mesure
INNER JOIN Type ON Mesure.type = Type.id
WHERE heure = '17:30:00'
GROUP BY date, type, libellé, heure
ORDER BY date
'''

type_velo_17 = pd.read_sql(type_velo_17, conn)
type_velo_17

fig5 = px.area(type_velo_17, x="date", y="avg", labels={"avg": "Mesures moyennes par station", "date": "Date"}, color="libellé", title = 'Evolution des types de vélos (mesures prises à 17h30)')

#Graphique n°6
data_785 = '''SELECT date, valeur
FROM Mesures_quotidiennes
INNER JOIN Station ON Mesures_quotidiennes.id_station = Station.id
WHERE id_station = 785
AND date BETWEEN '2020-01-01' AND '2020-12-31'
ORDER BY date
'''

data_785 = pd.read_sql(data_785, conn)

fig6 = px.line(data_785, x='date', y='valeur', labels={"valeur": "Mesures", "date": "Date"}, title='Evolution de la tendance, station 785 (50 Otages vers Sud - Jean Bart)')

fig6.update_xaxes(
    rangeslider_visible=True,
    rangeselector=dict(
        buttons=list([
            dict(count=1, label="1m", step="month", stepmode="backward"),
            dict(count=6, label="6m", step="month", stepmode="backward"),
            dict(count=1, label="1a", step="year", stepmode="backward"),
            dict(step="all")
        ])
    )
)

#Graphique n°7
moy_19 = '''
SELECT date AS date19, valeur AS valeur19
FROM mesures_quotidiennes
INNER JOIN station ON mesures_quotidiennes.id_station = station.id
WHERE station.id = 881
AND date BETWEEN '2019-01-01' AND '2019-12-31'
ORDER BY date'''

moy_19 = pd.read_sql(moy_19, conn)

moy_20 = '''
SELECT date AS date20, valeur AS valeur20
FROM mesures_quotidiennes
INNER JOIN station ON mesures_quotidiennes.id_station = station.id
WHERE station.id = 881
AND date BETWEEN '2020-01-01' AND '2020-12-31'
AND date != '2020-02-29'
ORDER BY date'''

moy_20 = pd.read_sql(moy_20, conn)

moy = pd.concat([moy_19,moy_20], axis=1)

moy['evolution'] = moy['valeur20']-moy['valeur19']

#Graphique n°8
horaires_velo = '''select heure, CAST(ROUND(AVG(CAST(valeur AS DECIMAL))) AS INTEGER) AS valeur
from mesure
where heure != '17:30:00'
group by heure'''

horaires_velo = pd.read_sql(horaires_velo, conn)

fig8 = px.bar(horaires_velo, x="heure", y="valeur", text='valeur', labels={"valeur": "Mesures", "heure": "Heure de la journée"}, title="Fréquentation par heure de la journée (mesures horaires moyennes)")

#Graphique n°9
lien_meteo = '''SELECT mesures_quotidiennes.date, avg(t_moy) AS Température, avg(valeur) AS Valeur
FROM meteo
INNER JOIN mesures_quotidiennes ON meteo.date=mesures_quotidiennes.date
WHERE meteo.date BETWEEN '2016-01-01' AND '2019-12-31'
GROUP BY mesures_quotidiennes.date
ORDER BY date
'''

lien_meteo = pd.read_sql_query(lien_meteo, conn)

fig9 = px.scatter(x=lien_meteo['température'], y=lien_meteo['valeur'], labels={"x": "Températures (en °C)", "y": "Nombre de passages moyens"})

#Station

datas = pd.read_csv("C:/Users/levog/Simplon/projet-chef-d-oeuvre/Data/output/Mesures_quotidiennes.csv")
datas["date"] = pd.to_datetime(datas["date"], format="%Y-%m-%d")
datas.sort_values("date", inplace=True)

def update_charts(id_station, start_date, end_date):
    mask = (
        (datas.id_station == id_station)
        & (datas.date >= start_date)
        & (datas.date <= end_date)
    )
    filtered_data = datas.loc[mask, :]
    price_chart_figure = {
        "data": [
            {
                "x": filtered_data["date"],
                "y": filtered_data["valeur"],
                "type": "lines",
                "hovertemplate": "%{y} vélos<extra></extra>",
            },
        ],
        "layout": {
            "title": {
                "text": "Average Price of Avocados",
                "x": 0.05,
                "xanchor": "left",
            },
            "xaxis": {"fixedrange": True},
            "yaxis": {"tickprefix": "", "fixedrange": True},
            "colorway": ["#17B897"],
        },
    }

    return price_chart_figure

