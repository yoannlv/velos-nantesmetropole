import pandas as pd

types = pd.DataFrame({'id': ['1', '2', '3'],
                   'type': ['VAE', 'Bicloo', 'Autres vélos']})

types.to_csv('C:/Users/levog/Simplon/projet-chef-d-oeuvre/Data/output/Types.csv', index=False)
types.to_csv('C:/Users/Public/Data/Types.csv', index=False)