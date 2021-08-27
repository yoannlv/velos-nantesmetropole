import csv
import pandas as pd
import folium

stations = pd.read_csv('C:/Users/Public/Data/Stations_velos.csv', delimiter=',')

STATIONS = stations['Nom']
LATS = stations['Latitude']
LONGS = stations['Longitude']

coords = (47.216801845, -1.54981276633)
map = folium.Map(location=coords, tiles='OpenStreetMap', zoom_start=13)

for i in range(len(STATIONS)):
    folium.CircleMarker(
        location = (LONGS[i], LATS[i]),
        color = 'crimson',
        fill = True,
        fill_color = 'crimson',
        fill_opacity=0.1,
        line_opacity=0.55
    ).add_to(map)

map.save('C:/Users/levog/Simplon/projet-chef-d-oeuvre/Data/Liste_stations.html')
print("Exportation réussie !")