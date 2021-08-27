import pymongo
import subprocess
import json
from pymongo import MongoClient

def test():
    
    liste = ['Comptages_nantes_metropole_2014-2019.csv', 'Comptages_nantes_metropole_2020_2021.csv', 'Comptages_place_au_velo_1998_2020.csv',
             'Comptages_place_au_velo_2010_2020.csv', 'Lieux_comptages_place_au_velo.csv', 'Lieux_comptages_velo_nantes_metropole.csv', 'Temperature-quotidienne-regionale-2016-2017.csv']  
    
    for e in liste:
        k = '/'+str(e)    
        input_file = 'C:/Users/levog/Simplon/projet-chef-d-oeuvre/Data'+k
        exe = "C:/Users/levog/Downloads/exiftool_12-25_fr_202900/exiftool.exe"
        process = subprocess.Popen([exe, input_file], stdout=subprocess.PIPE, stderr= subprocess.STDOUT, universal_newlines=True)
        metadatabrut = {}
        for output in process.stdout:
            line = (output.strip().split(":",1))
            metadatabrut[line[0].strip()] = line[1].strip()
        
        client = MongoClient()

        db = client.metadonnees

        collection = db.metadonnees_repertoire

        result = collection.insert_one(metadatabrut)

        db.list_collection_names()

if __name__ == "__main__":
    test() 