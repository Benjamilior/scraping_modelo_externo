import http.client
import json
import pandas as pd
import gspread
import time
from datetime import datetime

# Constantes
HEADERS = {
    'sec-ch-ua': "'Google Chrome';v='107', 'Chromium';v='107', 'Not=A?Brand';v='24'",
    'sec-ch-ua-mobile': "?0",
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
    'Content-Type': "application/json",
    'accept': "application/json",
    'Referer': "https://www.jumbo.cl/",
    'x-api-key': "IuimuMneIKJd3tapno2Ag1c1WcAES97j",
    'sec-ch-ua-platform': 'Windows'
}
API_URL = "apijumboweb.smdigital.cl"

def connect_to_google_sheets():
    gc = gspread.service_account(filename='creds.json')
    return gc.open('scrapetosheets').worksheet('Jumbo')

def get_current_datetime():
    now = datetime.now()
    return now.strftime("%m/%d/%Y, %H:%M:%S")

def fetch_jumbo_data(category, page_num):
    conn = http.client.HTTPSConnection(API_URL)
    endpoint = f'/catalog/api/v2/products/{category}?page={page_num}&sc=11'
    conn.request("GET", endpoint, "", HEADERS)
    res = conn.getresponse()
    return json.loads(res.read())

def process_jumbo_response(data):
    rwdatajumbo = data['products']
    jsonjumbo, disponibilidad, unidad, preciobase, preciooferta, categoriaj = [], [], [], [], [], []
    
    for product in rwdatajumbo:
        item = product['items'][0]
        seller = item['sellers'][0]
        offer = seller['commertialOffer']
        
        disponibilidad.append(offer['AvailableQuantity'])
        unidad.append(item['measurementUnit'])
        preciobase.append(offer['Price'])
        preciooferta.append(offer['PriceWithoutDiscount'])
        categoriaj.append(product['categories'][0])
        jsonjumbo.append(product)
        
    jumbo_df = pd.json_normalize(jsonjumbo)[['productName', 'productId', 'brand']]
    jumbo_df['Disponibilidad'] = disponibilidad
    jumbo_df['Unidad'] = unidad
    jumbo_df['Precio Base'] = preciobase
    jumbo_df['Precio Oferta'] = preciooferta
    jumbo_df['Categoria'] = categoriaj
    jumbo_df['Web'] = 'Jumbo'
    jumbo_df['Fecha'] = get_current_datetime()
    
    return jumbo_df

def main():
    Jumbo = connect_to_google_sheets()
    Jumbo.clear()
    Jumbo.append_row(['Nombre Producto', 'ID', 'Marca', 'Disponibilidad', 'Unidad', 'Precio Base', 'Precio Oferta', 'Categoria', 'Web', 'Fecha'])
    
    categories = ['frutas-y-verduras','despensa','lacteos','limpieza','carniceria','bebidas-aguas-y-jugos','congelados','desayuno-y-dulces','belleza-y-cuidado-personal','vinos-cervezas-y-licores','quesos-y-fiambres','panaderia-y-pasteleria','pescaderia','comidas-preparadas','mundo-bio-natura','mascotas','farmacia', ]
    #categories = ['lacteos']
    
    for category in categories:
        page_num = 1
        last_product = None   
        all_data = []  # Usaremos esta lista para almacenar todos los datos antes de subirlos a Google Sheets

        while True:  # Eliminamos la condición de 200 páginas
            try:
                data = fetch_jumbo_data(category, page_num)

                # Verificar si hay productos en la respuesta
                if not data.get('products'):
                    break

                jumbo_df = process_jumbo_response(data)

                # Si el último producto de la página anterior es el mismo que el primer producto de la página actual, rompemos el ciclo
                if not jumbo_df.empty and last_product == jumbo_df.iloc[0].to_dict():
                    break

                # Almacenar el último producto de esta página para la siguiente iteración
                if not jumbo_df.empty:
                    last_product = jumbo_df.iloc[-1].to_dict()

                all_data.extend(jumbo_df.values.tolist())  # Acumulamos los datos
                print(f"categoria {category} pagina {page_num}")
                page_num += 1
                time.sleep(1)  # Agregamos un retraso de 2 segundos entre solicitudes

            except Exception as e:
                print(f"Error processing category {category} on page {page_num}: {e}")
                break

        # Ahora, subimos todos los datos acumulados a Google Sheets
        Jumbo.append_rows(all_data)

if __name__ == "__main__":
    main()