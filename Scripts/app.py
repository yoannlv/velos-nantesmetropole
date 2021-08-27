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
import sqlite3
from collections import Counter
from sqlalchemy import create_engine
import plotly.express as px
from data import *
from dash.dependencies import Output, Input

conn = sqlite3.connect('vélos_nantesmetropole.db')

ma_base_donnees = "vélos_nantesmetropole"
utilisateur = "postgres"
mot_passe = os.environ.get("pg_psw")

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

app = dash.Dash(__name__)
app.title = "Évolution du vélo à Nantes Métropole"

app.layout = html.Div(
    children=[
            html.Div(children=[html.Img(src='assets/velo.png')],
            style={"text-align":"center"}
            ),
        html.H1(
            children="🚲Analyse de la tendance du cyclisme à Nantes Métropole🚲",
            style={"fontSize": "44px", "fontFace": "Helvetica", "background-color" : "#8C8F91", "text-align":"center"}
            ),
        html.P(
            children="Ce tableau de bord décrit les principaux enseignements tirés de l'exploitation de la base de données 'vélos_nantesmetropole.db', qui analyse l'évolution du cyclisme dans la métropole de Nantes\
                à partir des données des stations de comptage placées à différents lieux de la ville. Ce travail a été fourni par Yoann Le Voguer dans le cadre du projet chef d'oeuvre pour la certification Développeur\
                Data à l'organisme Simplon.co Grand Ouest. Nous nous contenterons de commencer dans un premier temps de façon brute les données sous chaque graphique, puis d'en tirer des conclusions à la fin.",
            style={"fontSize": "24px", "text-align":"left"}
        ),
        html.Iframe(id = 'map', srcDoc = open('C:/Users/levog/Simplon/projet-chef-d-oeuvre/Data/Liste_stations.html', 'r').read(), width ='100%', height='500'),
        html.H2("I. Evolution globale du cyclisme", style={"fontSize": "24px", "background-color" : "#CEC1C1"}),
        dcc.Graph(
        id='id1',
        figure=fig1
    ), 
        html.H4("Comme nous pouvons le constater, la fréquence d'utilisation du vélo possède des cycles chaque année, avec une légère augmentation continue entre janvier et juin, avant une diminution en été\
            (juillet-août) avant de repartir à la hausse jusqu'en octobre puis de rediminuer. Globalement on peut donc constater que l'utilisation du vélo est la plus élevée hors vacances scolaires et lors des journées\
            chaudes. De 2014 à 2019, les mesures avaient tendance a augmenté de façon régulière chaque année.", style={"fontSize": "18px"}),
        html.H2("II. Fréquence par station", style={"fontSize": "24px", "background-color" : "#CEC1C1"}),
        html.Div(
            children=[
                html.Div(
                    children=[
                        html.Div(
                            children=[
                                html.Div(children="Station", className="menu-title"),
                                dcc.Dropdown(
                                    id="region-filter",
                                    options=[
                                        {"label": nom_station, "value": nom_station}
                                        for nom_station in np.sort(datas.nom_station.unique())
                                    ],
                                    value="Sélectionnez une station",
                                    clearable=False,
                                    className="dropdown",
                                ),
                            ]
                        ),
                        html.Div(
                            children=[
                                html.Div(
                                    children="Date",
                                    className="menu-title"
                                    ),
                                dcc.DatePickerRange(
                                    id="date-range",
                                    min_date_allowed=datas.date.min().date(),
                                    max_date_allowed=datas.date.max().date(),
                                    start_date=datas.date.min().date(),
                                    end_date=datas.date.max().date(),
                                ),
                            ]
                        ),
                    ],
                    className="menu",
                ),
                html.Div(
                    children=[
                        html.Div(
                            children=dcc.Graph(
                                id="price-chart", config={"displayModeBar": False},
                            ),
                            className="card",
                        ),
                    ],
                    className="wrapper",
                ),
            ]
        ),
        html.H4("Ce graphique permet de connaître la fréquentation par station sur une période donnée.", style={"fontSize": "18px"}),
        html.H2("III. Stations les plus fréquentées", style={"fontSize": "24px", "background-color" : "#CEC1C1"}),
        dcc.Graph(
        id='id2',
        figure=fig2
    ), 
        html.H4("Ce diagramme en barres nous indique le nombre moyen de vélos comptabilisés par stations. Sans grande surprise, nous nous apercevons que les pistes les plus fréquentées se situent au centre de la ville, notamment 50 Otages,\
            et sont principalement situés sur des grands axes de circulation.", style={"fontSize": "18px"}),
        html.H2("III. Fréquence par jour de la semaine", style={"fontSize": "24px", "background-color" : "#CEC1C1"}),
        dcc.Graph(
        id='id3',
        figure=fig3
    ), 
        html.H4("Comme nous pouvons le constater, la fréquence d'utilisation du vélo possède des cycles chaque année, avec une légère augmentation continue entre janvier et juin, avant une diminution en été\
            (juillet-août) avant de repartir à la hausse jusqu'en octobre puis de rediminuer. Globalement on peut donc constater que l'utilisation du vélo est la élevée hors vacances scolaires et lors des journées\
            chaudes. De 2014 à 2019, les mesures avaient tendance a augmenté de façon régulière chaque année.", style={"fontSize": "18px"}),
        html.H2("IV. Fréquence par type de vélo", style={"fontSize": "24px", "background-color" : "#CEC1C1"}),
        dcc.Graph(
        id='id4',
        figure=fig4
    ), 
        dcc.Graph(
        id='id5',
        figure=fig5
    ), 
        html.H4("Nous possédons grâce à ces graphiques 2 types d'informations différentes. Tout d'abord, le nombre de vélos comptés sur des sessions d'une heure par jour est systématiquement\
            plus élevé à 17h30 qu'à 14h, du double environ. Ensuite, nous constatons une importante augmentation du nombre de vélos entre 2010 et 2021, quelque soit le type de vélo. Les vélos\
                classiques de particuliers constituent la grande majorité des mesures observés, mais si l'on observe dans un même temps la légère augmentation du nombre de Bicloo (vélos libre-service),\
                le nombre de VAE (vélo à assistance électrique) a explosé en une décennie.", style={"fontSize": "18px"}),
        html.H2("V. Effet des confinements", style={"fontSize": "24px", "background-color" : "#CEC1C1"}),
        dcc.Graph(
        id='id6',
        figure=fig6
    ), 
        html.H4("La station 785 a été choisie car elle est celle qui est en moyenne la plus fréquentée. Nous pouvons constater une brusque diminution du nombre de vélos à partir de la mi-mars, puis à une reprise soudaine à partir du milieu du mois de mai,\
            ces dates correspondant très exactement à celle du 1er confinement. On peut alors attribuer cette tendance à la situation sanitaire, du fait qu'aucune baisse\
                de ce type n'avait été observé les années précédentes à ces dates. Les dates du 2ème confinement laissent aussi voir une diminution du nombre de vélos\
                moins importante cette fois-ci du fait des restrictions plus souples de la part du gouvernement, et qui peut également être imputé aux baisses de température\
                liées à l'entrée dans l'hiver, que l'on retrouve également les autres années.", style={"fontSize": "18px"}),
        dcc.Graph(
        id='fig7',
        figure={
            'data': [
                {'x': moy["date20"], 'y': moy["evolution"], 'type': 'bar', 'name': 'Valeur'},
            ],
            'layout': {
                'title': 'Variation des comptages de vélo, station 881 : Madeleine vers Sud (par rapport au même jour de l\'année n-1)'
            }
        }
    ),
        html.H4("Pour effectuer la comparaison 2019-2020, et véritablement juger de la comparaison d'une année sur l'autre et pouvoir mettre en valeur une différence entre 2 dates similaires,\
            la station 881 a été retenue car c'est celle qui avait le plus de données disponibles et fiables lors de ces deux années. Sur cette station en particulier, on remarque une baisse sur toute l'année,\
            qui peut être expliquée par des raisons que l'on ne peut pas connaître simplement à partir de chiffres. Toutefois, cette différence est plus marquée et plus stable à partir de la mi-mars,\
            et ce jusqu'à la fin-mai. Il faut tenir en compte que les variations sont réalisées sur des dates similaires, donc ne sont pas comparées avec les mêmes jours de la semaine", style={"fontSize": "18px"}
    ),
        html.H2("VI. Fréquence par heure de la journée", style={"fontSize": "24px", "background-color" : "#CEC1C1"}),
        dcc.Graph(
        id='id8',
        figure=fig8
    ),
        html.H4("Ce graphique montre l’évolution du nombre de vélos au fur et à mesure de la journée grâce à une moyenne horaire. On constate un premier pic entre 8h et 9h,\
         ainsi qu’un second entre 17h et 19h. Un léger regain est observé à l’heure de midi. L’utilisation correspondant aux horaires de travail/étude classiques, nous pouvons\
         donc supposer que le vélo est principalement utilisé pour des raisons professionnelles.", style={"fontSize": "18px"}
    ),
        html.H2("VII. Test de corrélation avec la météo", style={"fontSize": "24px", "background-color" : "#CEC1C1"}),
        dcc.Graph(
        id='id9',
        figure=fig9
    ),
        html.H4("Nous avons voulu tenter de vérifier une potentielle corrélation entre l'utilisation du vélo avec, en axe des abscisses les températures et en axe des ordonnées le nombre moyen\
            de passages de vélo par date, toutes stations confondues. Chaque point correspond à une journée. Seule les dates entre le 1er janvier 2016 et le 31 décembre 2019 ont été prise en compte,\
            l'année 2020 ayant été mise de côté car cause de stations supplémentaires et de contexte différent, ce qui rendait la comparaison difficile, car n'étant pas toute chose égale par ailleurs.\
            Aucune corrélation n'en ressort vraiment, l'utilisation du vélos étant davantage influencée par la saison, les périodes de vacances, les jours de la semaine...", style={"fontSize": "18px"}
    ),
        html.H2("Conclusion", style={"fontSize": "24px", "background-color" : "#CEC1C1"}),
        html.H4("L’exploitation de la base de données a permis de retirer des enseignements sur l’utilisation du vélo à Nantes. Cette tendance est globalement à la hausse sur la dernière\
            décennie, malgré une diminution durant l’année 2020, très marquée durant les confinements, et qui est imputable à la situation sanitaire, donc qui ne laisse pas présager d’une\
            baisse du vélo dans le temps. Nous en savons également davantage sur les habitudes, et par conséquent les motivations des cyclistes. Deux des analyses que nous avons retirées\
            sont la fréquentation dans l’année et par heure. Cette première information nous apprend que le vélo est plus fréquemment utilisé au printemps et à l’automne et connaît une diminution\
            en été, la seconde nous montre que les pistes cyclables sont principalement fréquentées aux alentours de 8h à 9h, puis de 16h à 18h, avec un léger regain à l’heure de midi. En croisant\
            ces analyses, on peut supposer que le vélo est davantage utilisé pour réaliser le trajet domicile-travail que pour les loisirs. La diminution en hiver peut elle être expliquée par les\
            conditions météorologiques plus froides et moins adaptées à la pratique du vélo.", style={"fontSize": "18px"}
    ),
        html.Div(children=[html.Img(src='assets/banniere.png')],
            style={"text-align":"center"}
            ),       
    ]
)

@app.callback(
    Output("price-chart", "figure"),
    [
        Input("region-filter", "value"),
        Input("date-range", "start_date"),
        Input("date-range", "end_date"),
    ],
)
def update_charts(nom_station, start_date, end_date):
    mask = (
        (datas.nom_station == nom_station)
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
                "text": "Nombre quotidien de vélos par station",
                "x": 0.05,
                "xanchor": "left",
            },
            "xaxis": {"fixedrange": True},
            "yaxis": {"tickprefix": "", "fixedrange": True},
            "colorway": ["#17B897"],
        },
    }

    return price_chart_figure

if __name__ == "__main__":
    app.run_server(debug=True)