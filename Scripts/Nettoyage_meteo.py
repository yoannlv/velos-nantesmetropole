import csv
import pandas as pd

meteo = pd.read_csv("C:/Users/levog/Simplon/projet-chef-d-oeuvre/Data/Temperature-quotidienne-regionale-2016-2017.csv", delimiter=';')

meteo = meteo.drop(columns=['Code INSEE région', 'Région'])

meteo.to_csv('C:/Users/levog/Simplon/projet-chef-d-oeuvre/Data/output/Meteo.csv', index=False)
meteo.to_csv('C:/Users/Public/Data/Meteo.csv', index=False)