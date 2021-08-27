import csv
import pandas as pd
import json
import requests
import psycopg2
import os

##Données des stations Nantes Métropole
#Chargement des données
lieux1 = pd.read_csv("C:/Users/levog/Simplon/projet-chef-d-oeuvre/Data/Lieux_comptages_velo_nantes_metropole.csv", delimiter=';')

#Division de la cellule Géolocalisation
lieux1[['Longitude','Latitude']] = lieux1.Géolocalisation.str.split(",",expand=True)

#Suppression colonnes
lieux1 = lieux1.drop(columns=['Observations', 'Géolocalisation'])

#Attribution d'un identifiant pour les stations qui n'en possédaient pas
lieux1['Numéro'].where(lieux1['Libellé']!='Saint Léger les Vignes','100',inplace=True)
lieux1['Numéro'].where(lieux1['Libellé']!='La Chapelle sur Erdre','101',inplace=True)

#Conversion des identifiants en integer
lieux1['Numéro'] = lieux1['Numéro'].astype(int)

#Déclaration de l'organisme possédant les compteurs
lieux1['Organisme'] = 'Nantes Métropole'

#Renommage des colonnes
lieux1 = lieux1.rename(columns = {'Numéro': 'Id', 'Libellé': 'Nom'})

##Données des stations Place au vélos
#Chargement des données
lieux2 = pd.read_csv("C:/Users/levog/Simplon/projet-chef-d-oeuvre/Data/Lieux_comptages_place_au_velo.csv", delimiter=';')

#Division de la cellule Géolocalisation
lieux2[['Longitude','Latitude']] = lieux2.geo_point_2d.str.split(",",expand=True)

#Suppression colonnes
lieux2 = lieux2.drop(columns=['Géométrie', 'geo_point_2d'])

#Déclaration de l'organisme possédant les compteurs
lieux2['Organisme'] = 'Place au vélo'

#Renommage des colonnes
lieux2 = lieux2.rename(columns = {'identifiant': 'Id', 'Lieu de comptage': 'Nom'})

#Concaténation des dataframes
df_data_stations = pd.concat([lieux1, lieux2])

#Export données traitées
df_data_stations.to_csv('C:/Users/levog/Simplon/projet-chef-d-oeuvre/Data/output/Stations_velos.csv', index=False)
df_data_stations.to_csv('C:/Users/Public/Data/Stations_velos.csv', index=False)