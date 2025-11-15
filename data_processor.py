import requests
import json
import re
from pathlib import Path

# URL de la fuente de datos
API_URL = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
OUTPUT_FILE = "gasolineras.geojson"

# Mapeo de nombres de campos del JSON original a nombres simplificados y normalizados
# Usaremos estos nombres simplificados en el front-end
PRECIOS_MAP = {
    "Precio Gasolina 95 E5": "Precio_95E5",
    "Precio Gasolina 98 E5": "Precio_98E5",
    "Precio Gasoleo A": "Precio_GasoilA",
    "Precio Gasoleo B": "Precio_GasoilB",
    "Precio Gasoleo Premium": "Precio_GasoilPremium",
    "Precio Gasolina 95 E10": "Precio_95E10",
    "Precio Gasolina 98 E10": "Precio_98E10",
}

def clean_price(price_str):
    """Limpia una cadena de precio y la convierte a float. Devuelve None si no es válido."""
    if not price_str or price_str.strip() == "":
        return None
    # Reemplaza la coma decimal por un punto y convierte a float
    try:
        return float(price_str.replace(',', '.'))
    except ValueError:
        return None

def clean_coord(coord_str, is_lon=False):
    """Limpia y convierte las coordenadas (Latitud/Longitud) a float."""
    if not coord_str:
        return None
        
    # La API del Ministerio usa "Longitud (WGS84)" con un formato DD,DDDDD o similar
    try:
        # Reemplaza la coma decimal por un punto
        return float(coord_str.replace(',', '.').strip())
    except ValueError:
        # Intenta un parseo más complejo si es necesario (ej. si usa grados/minutos/segundos)
        # Por simplicidad, asumimos que es el formato decimal directo.
        return None

def process_data():
    """Descarga los datos de la API y genera el archivo GeoJSON."""
    print("1. Descargando datos de la API del Ministerio...")
    try:
        response = requests.get(API_URL)
        response.raise_for_status() # Lanza un error para códigos de estado HTTP fallidos
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error al descargar los datos: {e}")
        return

    estaciones = data.get("ListaEESSPrecio", [])
    features = []
    
    print(f"2. Procesando {len(estaciones)} estaciones de servicio...")
    
    for estacion in estaciones:
        lat = clean_coord(estacion.get("Latitud"))
        lon = clean_coord(estacion.get("Longitud (WGS84)"))
        
        # Debe tener coordenadas válidas para ser añadido al mapa
        if lat is None or lon is None:
            continue
            
        properties = {
            "Rotulo": estacion.get("Rótulo", "S/N"),
            "Direccion": estacion.get("Dirección", ""),
        }
        
        # Limpiar y mapear los precios
        has_valid_price = False
        for original_key, new_key in PRECIOS_MAP.items():
            price = clean_price(estacion.get(original_key))
            properties[new_key] = price
            if price is not None:
                has_valid_price = True
        
        # Solo añade la estación si tiene al menos un precio válido
        if has_valid_price:
            feature = {
                "type": "Feature",
                "geometry": {
                    # GeoJSON usa [longitud, latitud]
                    "coordinates": [lon, lat],
                    "type": "Point"
                },
                "properties": properties
            }
            features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }

    # Guardar el GeoJSON
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
        
    print(f"3. Proceso finalizado. {len(features)} estaciones guardadas en '{OUTPUT_FILE}'.")

if __name__ == "__main__":
    process_data()