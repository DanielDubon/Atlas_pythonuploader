import pandas as pd
from pymongo import MongoClient


def connect_to_mongo():
    client = MongoClient(
        'mongodb+srv://biancalderon0115:15BMnmaq1iP2nypk@proyecto2.lmxv3ix.mongodb.net/'
        '?retryWrites=true&w=majority&appName=Proyecto2&tlsInsecure=true'
    )
    db = client['Proyecto2']
    return db

# Funcion para cargar el CSV a MongoDB en la colección indicada
def upload_csv_to_mongo(csv_file, collection):
    # Cargar los datos del archivo CSV en un DataFrame
    df = pd.read_csv(csv_file)
    # Convertir el DataFrame a una lista de diccionarios
    data_dict = df.to_dict(orient='records')
    # Insertar los documentos en la colección
    collection.insert_many(data_dict)
    print(f"{len(data_dict)} documentos insertados en {collection.name}.")


csv_files = {
    'Pedidos': 'pedidos.csv',
    'Productos': 'productos.csv',
    'Reportes': 'reportes.csv',
    'Reseñas': 'reseñas.csv',
    'Restaurante': 'restaurante (1).csv', 
    'Usuario': 'usuario.csv',
   
}

# Conectar a la base de datos
db = connect_to_mongo()

# Subir los archivos a sus respectivas colecciones
for collection_name, csv_file in csv_files.items():
    collection = db[collection_name]
    upload_csv_to_mongo(csv_file, collection)

print("Todos los datos han sido cargados correctamente.")
