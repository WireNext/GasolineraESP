import requests
import json
import time 
from pathlib import Path

# URL de la fuente de datos
API_URL = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
OUTPUT_FILE = "gasolineras.geojson"

# Configuración de reintentos
MAX_RETRIES = 5
RETRY_DELAY_SECONDS = 10 

# Mapeo de nombres de campos del JSON original a nombres simplificados y normalizados
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
    try:
        # Reemplaza la coma por el punto para la conversión a float
        return float(price_str.replace(',', '.'))
    except ValueError:
        return None

def clean_coord(coord_str):
    """Limpia y convierte las coordenadas a float."""
    if not coord_str:
        return None
    try:
        return float(coord_str.replace(',', '.').strip())
    except ValueError:
        return None

def process_data():
    """Descarga los datos de la API, limpia y genera el GeoJSON con reintentos."""
    print("1. Descargando datos de la API del Ministerio...")
    
    # --- CABECERA USER-AGENT para simular un navegador y evitar bloqueos de IP ---
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    data = None
    # Lógica de Reintentos
    for attempt in range(MAX_RETRIES):
        try:
            print(f"Intento {attempt + 1} de {MAX_RETRIES}...")
            # Realizar la petición, pasando la cabecera y el timeout
            response = requests.get(API_URL, timeout=45, headers=HEADERS) 
            response.raise_for_status()
            data = response.json()
            print("Descarga exitosa.")
            break # Salir del bucle si la descarga es exitosa
        except requests.exceptions.RequestException as e:
            print(f"Error en la descarga (Tipo: {e.__class__.__name__}): {e}")
        
        # Esperar y reintentar si no es el último intento
        if attempt < MAX_RETRIES - 1:
            print(f"Esperando {RETRY_DELAY_SECONDS} segundos antes de reintentar...")
            time.sleep(RETRY_DELAY_SECONDS)

    if data is None:
        print("❌ Falló la descarga después de todos los reintentos. No se generará el GeoJSON.")
        return

    # ----------------------------------------------------
    # Procesamiento de datos si la descarga fue exitosa
    # ----------------------------------------------------
    estaciones = data.get("ListaEESSPrecio", [])
    features = []
    
    print(f"2. Procesando {len(estaciones)} estaciones de servicio...")
    
    for estacion in estaciones:
        # La API usa 'Longitud (WGS84)' para la longitud y 'Latitud' para la latitud
        lat = clean_coord(estacion.get("Latitud"))
        lon = clean_coord(estacion.get("Longitud (WGS84)"))
        
        # Debe tener coordenadas válidas
        if lat is None or lon is None:
            continue
            
        properties = {
            "Rotulo": estacion.get("Rótulo", "S/N"),
            "Direccion": estacion.get("Dirección", ""),
        }
        
        has_valid_price = False
        for original_key, new_key in PRECIOS_MAP.items():
            price = clean_price(estacion.get(original_key))
            properties[new_key] = price
            if price is not None:
                has_valid_price = True
        
        # Solo añade la estación si tiene al menos un precio válido (para reducir el tamaño del archivo)
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
        # Usar indent=2 para hacer el GeoJSON legible, pero puedes quitarlo si quieres el archivo más pequeño
        json.dump(geojson, f, ensure_ascii=False, indent=2)
        
    print(f"3. Proceso finalizado. {len(features)} estaciones guardadas en '{OUTPUT_FILE}'.")

if __name__ == "__main__":
    process_data()
