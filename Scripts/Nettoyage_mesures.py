import csv
import pandas as pd
from math import isnan

#Chargement des données
mesures1 = 'C:/Users/levog/Simplon/projet-chef-d-oeuvre/Data/Comptages_nantes_metropole_2014-2019.csv'

data_mesures1 = []
with open(mesures1, newline="",encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';')
    for row in reader:
        data_mesures1.append(row)

mesures2 = "C:/Users/levog/Simplon/projet-chef-d-oeuvre/Data/Comptages_nantes_metropole_2020_2021.csv"

data_mesures2 = []
with open(mesures2, newline="",encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';')
    for row in reader:
        data_mesures2.append(row)

mesures3 = 'C:/Users/levog/Simplon/projet-chef-d-oeuvre/Data/Comptages_place_au_velo_1998_2020.csv'

data_mesures3 = []
with open(mesures3, newline="",encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';')
    for row in reader:
        data_mesures3.append(row)

mesures4 = 'C:/Users/levog/Simplon/projet-chef-d-oeuvre/Data/Comptages_place_au_velo_2010_2020.csv'

data_mesures4 = []
with open(mesures4, newline="",encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';')
    for row in reader:
        data_mesures4.append(row)

#Elimination des données non-fiables
Data_mesures1 = []
for e in data_mesures1:
    if e['Valeur modélisée'] != 'NA' or ['Valeur modélisée'] != '':
        Data_mesures1.append(e)

Data_mesures2 = []
for e in data_mesures2:
    if e["Probabilité de présence d'anomalies"] == '':
        Data_mesures2.append(e)

#Conversion des fichiers en dataframes
df_data_mesures1 = pd.DataFrame(Data_mesures1)
df_data_mesures2 = pd.DataFrame(Data_mesures2)
df_data_mesures3 = pd.DataFrame(data_mesures3)
df_data_mesures4 = pd.DataFrame(data_mesures4)

#Attribution d'un identifiant pour les stations qui n'en possédaient pas
df_data_mesures1['Identifiant du compteur'].where(df_data_mesures1['Nom du compteur']!='Saint Léger les Vignes','100',inplace=True)
df_data_mesures1['Identifiant du compteur'].where(df_data_mesures1['Nom du compteur']!='La Chapelle sur Erdre','101',inplace=True)

#Suppression valeurs nulles
df_data_mesures1.drop(df_data_mesures1.loc[df_data_mesures1['Comptage relevé']=='NA'].index, inplace=True)

#Suppression colonnes
df_data_mesures1 = df_data_mesures1.drop(columns=['Nom du compteur', 'Valeur modélisée', 'Anomalie'])
df_data_mesures2 = df_data_mesures2.drop(columns=["Probabilité de présence d'anomalies", "Jour de la semaine", "Libellé"])
df_data_mesures3 = df_data_mesures3.drop(columns=["Années de comptage", "Nom du lieu de comptage", "Date complète", "Jour de la semaine"])
df_data_mesures4 = df_data_mesures4.drop(columns=["Années de comptage", "Nom du lieu de comptage", "Date complète", "Jour de la semaine", "Total"])

#Pivot dataframes
df_data_mesures2 = df_data_mesures2.melt(id_vars = ['Numéro de boucle', 'Jour'], var_name = 'heure', value_name = 'valeur')
df_data_mesures4 = df_data_mesures4.melt(id_vars = ['Identifiant', 'Date', 'Heure'], var_name = 'type', value_name = 'total')

#Remplacement du type par un _id_
df_data_mesures4 = df_data_mesures4.replace(['VAE', 'bicloo', 'Autres vélos'], ['1', '2', '3'])

#Renommage colonnes
df_data_mesures1 = df_data_mesures1.rename(columns = {'Identifiant du compteur': 'Id_station', 'Jour': 'Date', 'Comptage relevé': 'Valeur'})
df_data_mesures2 = df_data_mesures2.rename(columns = {'Numéro de boucle': 'Id_station', 'Jour': 'Date', 'heure': 'Heure', 'valeur': 'Valeur'})
df_data_mesures3 = df_data_mesures3.rename(columns = {'Identifiant du lieu de comptage': 'Id_station','Comptage relevé': 'Valeur'})
df_data_mesures4 = df_data_mesures4.rename(columns = {'Identifiant': 'Id_station', 'type': 'Type', 'total': 'Valeur'})

#Changement colonne Heure
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['00'],'00:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['01'],'01:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['02'],'02:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['03'],'03:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['04'],'04:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['05'],'05:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['06'],'06:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['07'],'07:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['08'],'08:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['09'],'09:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['10'],'10:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['11'],'11:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['12'],'12:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['13'],'13:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['14'],'14:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['15'],'15:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['16'],'16:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['17'],'17:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['18'],'18:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['19'],'19:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['20'],'20:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['21'],'21:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['22'],'22:00:00')
df_data_mesures2['Heure'] = df_data_mesures2['Heure'].replace(['23'],'23:00:00')

#Concaténation
df_data_mesures = pd.concat([df_data_mesures1, df_data_mesures2, df_data_mesures3, df_data_mesures4])

#Changement format Date et Id_station
df_data_mesures['Date'] = pd.to_datetime(df_data_mesures['Date'])
df_data_mesures['Id_station'] = pd.to_numeric(df_data_mesures['Id_station'])

#Ajout d'une colonne pour le jour de la semaine
df_data_mesures['Jour'] = df_data_mesures['Date'].dt.day_name()

#Téléchargement des données Stations pour ne conserver que les mesures attribuées à une station qui est identifiée
liste_stations = pd.read_csv("C:/Users/Public/Data/Stations_velos.csv", delimiter=',')
liste = liste_stations['Id']
liste_stations = list(liste)
df_data_mesures = df_data_mesures[df_data_mesures.Id_station.isin(liste_stations)]

#Export des données
df_data_mesures.to_csv('C:/Users/levog/Simplon/projet-chef-d-oeuvre/Data/output/Mesures_velos.csv', index=False)
df_data_mesures.to_csv('C:/Users/Public/Data/Mesures_velos.csv', index=False)