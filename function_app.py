import azure.functions as func
import datetime
import json
import logging
import requests
from dotenv import load_dotenv
import os
import pandas as pd 
from azure.storage.blob import BlobServiceClient
import io
import random

app = func.FunctionApp()

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DOMAIN = os.getenv("DOMAIN")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")

@app.queue_trigger(arg_name="azqueue", queue_name="requests",
                               connection="QueueAzureWebJobsStorage") 

def QueueTriggerPokeReport(azqueue: func.QueueMessage):
    body = azqueue.get_body().decode('utf-8')
    record = json.loads(body)
    id = record[0]["id"]

    update_request(id, "inprogress")

    request_info = get_request(id)
    pokemons = get_pokemons(request_info[0]["type"], request_info[0]["qty"])
    pokemon_bytes = generate_csv_to_blob(pokemons)
    blob_name = f"poke_report_{id}.csv"
    upload_csv_to_blob(blob_name=blob_name, csv_data=pokemon_bytes)
    logger.info(f"CSV file {blob_name} uploaded to blob storage.")

    url_completa = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{blob_name}"
    update_request(id, "completed", url_completa)
    
def update_request(id: int, status: str, ulr: str = None  ) -> dict:
    payload = {
        "status": status,
        "id": id
    }
    if ulr:
        payload["url"] = ulr
    response = requests.put(f"{DOMAIN}/api/request", json=payload)
    return response.json()

def get_request(id : int) -> dict:
    response = requests.get(f"{DOMAIN}/api/request/{id}")
    return response.json()

def get_pokemons(type: str, pokemonQty: int) -> list:
    pokeapi_url = f"https://pokeapi.co/api/v2/type/{type}"
    response = requests.get(pokeapi_url, timeout=300)
    data = response.json()
    pokemon_entries = data.get("pokemon", [])

    pokemon_entries = data.get("pokemon", [])
    cantidad = len(pokemon_entries)

    # Limitar pokemonQty a la cantidad máxima de entradas disponibles
    if pokemonQty > cantidad:
        pokemonQty = cantidad

    # Asegurarse de que pokemonQty no sea 0
    if pokemonQty == 0:
        pokemonQty = 1

    pokemon_entries = random.sample(pokemon_entries, pokemonQty)
    
    # Lista de detalles completos de los Pokémon
    pokemons_details = []
    
    for entry in pokemon_entries:
        pokemon_url = entry["pokemon"]["url"]
        pokemon_name = entry["pokemon"]["name"]
        
        # Llamada adicional para obtener detalles completos del Pokémon
        try:
            pokemon_details = requests.get(pokemon_url).json()
            base_stats = {stat["stat"]["name"]: stat["base_stat"] for stat in pokemon_details["stats"]}
            abilities = [ability["ability"]["name"] for ability in pokemon_details["abilities"]]
            
            # Agregar el Pokémon a la lista con las estadísticas y habilidades
            pokemons_details.append({
                "name": pokemon_name,
                "HP": base_stats.get("hp", 0),
                "Attack": base_stats.get("attack", 0),
                "Defense": base_stats.get("defense", 0),
                "Special Attack": base_stats.get("special-attack", 0),
                "Special Defense": base_stats.get("special-defense", 0),
                "Speed": base_stats.get("speed", 0),
                "abilities": ", ".join(abilities)
            })
        except Exception as e:
            logger.error(f"Error al obtener detalles para el Pokémon {pokemon_name}: {e}")
    
    return pokemons_details

def generate_csv_to_blob(pokemon_list: list) -> bytes:
    # Crear un DataFrame con la lista de Pokémon y sus estadísticas
    df = pd.DataFrame(pokemon_list)

    # Asegurarse de que las columnas existan
    columns = ['name', 'HP', 'Attack', 'Defense', 'Special Attack', 'Special Defense', 'Speed', 'abilities']
    df = df[columns]

    # Convertir el DataFrame a CSV en formato de bytes
    output = io.StringIO()
    df.to_csv(output, index=False, encoding='utf-8')
    csv_bytes = output.getvalue().encode('utf-8')
    output.close()
    
    return csv_bytes

def upload_csv_to_blob(blob_name: str, csv_data: bytes):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_name)
        blob_client.upload_blob(csv_data, overwrite=True)
        
    except Exception as e:
        logger.error(f"Error uploading CSV to blob: {e}")
        raise