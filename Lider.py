import http.client
import json
import pandas as pd
import gspread
from datetime import datetime
import time
import unicodedata


HEADERS = {
        'cookie': "TS018b674b=01538efd7cc5a58efbcfa0c2c82b5e2059cd014a298bf8c5bed3673bd5f378d936e3a861be4aaff8758129d553d2317ef09598108a; zy_did=8E57C48A-1E9A-B892-D63E-EB265BB97E0B",
        'authority': "apps.lider.cl",
        'accept': "application/json, text/plain, */*",
        'accept-language': "es-ES,es;q=0.9",
        'content-type': "application/json",
        'origin': "https://www.lider.cl",
        'referer': "https://www.lider.cl/",
        'sec-ch-ua': "'Chromium';v='106', 'Google Chrome';v='106', 'Not;A=Brand';v='99'",
        'sec-ch-ua-mobile': "?0",
        'sec-ch-ua-platform': "'Windows'",
        'sec-fetch-dest': "empty",
        'sec-fetch-mode': "cors",
        'sec-fetch-site': "same-site",
        'tenant': "supermercado",
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
        'x-channel': "SOD",
        'x-flowid': "1f0f7446-bd27-4d71-a2f4-cc5549014f32",
        'x-sessionid': "42881928-619d-43d3-b39d-d0455e1f1aac"
}
HEADERS2 = {
        'Set-Cookie': "TS018b674b=011797f54187fd4ca23a2a9bde8cb37ddc796126ad91750b56686d8bfc84c9504a93bbf6f7dc6cc9fa732a17d8f249113724e3ce85; Path=/",
        'authority': "apps.lider.cl",
        'accept': "application/json, text/plain, */*",
        'accept-language': "es-ES,es;q=0.9",
        'content-type': "application/json",
        'origin': "https://www.lider.cl",
        'referer': "https://www.lider.cl/",
        'sec-ch-ua': "'Chromium';v='106', 'Google Chrome';v='106', 'Not;A=Brand';v='99'",
        'sec-ch-ua-mobile': "?0",
        'sec-ch-ua-platform': "'Windows'",
        'sec-fetch-dest': "empty",
        'sec-fetch-mode': "cors",
        'sec-fetch-site': "same-site",
        'tenant': "supermercado",
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
        'x-channel': "SOD",
        'x-flowid': "1f0f7446-bd27-4d71-a2f4-cc5549014f32",
        'x-sessionid': "42881928-619d-43d3-b39d-d0455e1f1aac"
}

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    only_ascii = nfkd_form.encode('ASCII', 'ignore')
    return only_ascii.decode('ASCII')

def fetch_data2():
    payload2 = b''
    conn2 = http.client.HTTPSConnection("apps.lider.cl")
    conn2.request("GET", "/supermercado/bff/categories",payload2, HEADERS2)
    res = conn2.getresponse().read()
    conn2.close()
    #print(res.decode('utf-8'))
    return json.loads(res)['categories']


def extract_categories(data):
    CATEGORIES = []

    for category2 in data:
        label1 = category2['label'].replace(' ', '_')
        
        for subcategory_level2 in category2.get('categoriesLevel2', []):
            label2 = subcategory_level2['label'].replace(' ', '_')
            combined_label = f"{label1}/{label2}"
            CATEGORIES.append(combined_label)
    return CATEGORIES

def fetch_data(category, max_retries=3):
    all_products = []
    page_num = 1
   
    while True:
        payload = json.dumps({
            "categories": category,
            "page": page_num,
            "facets": [],
            "sortBy": "",
            "hitsPerPage": 100
        })

        data_for_this_page = None
        for retry in range(max_retries):
            try:
                conn = http.client.HTTPSConnection("apps.lider.cl")
                conn.request("POST", "/supermercado/bff/category", payload, HEADERS)
                res = conn.getresponse().read()
                conn.close()
                data_for_this_page = json.loads(res)['products']

                # Si data_for_this_page es vacío, significa que hemos terminado de raspar todos los productos para esta categoría
                if not data_for_this_page:
                    return all_products

                all_products.extend(data_for_this_page)
                break

            except json.JSONDecodeError:
                print(f"Failed to decode JSON for category {category} on page {page_num} attempt {retry + 1}.")
                print(res)
                if retry < max_retries - 1:  
                    time.sleep(2)
                    continue
                else:
                    print(f"Max retries reached for category {category} on page {page_num}. Breaking out of loop...")
                    return all_products

            except Exception as e:
                print(f"Error fetching data for category {category} on page {page_num} attempt {retry + 1}: {e}")
                if retry < max_retries - 1:
                    time.sleep(2)
                    continue
                else:
                    return all_products

        page_num += 1

def main():

    data = fetch_data2()
    CATEGORIES = extract_categories(data)
    CATEGORIES = [remove_accents(category) for category in CATEGORIES]
    CATEGORY_NAMES = [cat.split('/')[-1] for cat in CATEGORIES]

    gc = gspread.service_account(filename='creds.json')
    Lider = gc.open('scrapetosheets').worksheet('Lider')

    now = datetime.now()
    date_time = now.strftime("%m/%d/%Y, %H:%M:%S")

    all_data = []
    for cat, cat_name in zip(CATEGORIES, CATEGORY_NAMES):
            products = fetch_data(cat)
            df = pd.json_normalize(products)
            
            desired_columns = ['displayName', 'gtin13', 'brand', 'available', 'defaultQuantity', 'price.BasePriceReference', 'price.BasePriceSales','images.largeImage']
            available_columns = [col for col in desired_columns if col in df.columns]
            missing_columns = set(desired_columns) - set(available_columns)

            # Agregamos las columnas faltantes con valores predeterminados
            for missing_col in missing_columns:
                df[missing_col] = None  # Aquí puedes cambiar 'None' a cualquier valor predeterminado que desees.
            
            if missing_columns:
                print(f"Warning: Missing columns {missing_columns} for category {cat_name}")

            df = df[desired_columns]
            df['Categoria'] = cat_name
            df['Web'] = 'Lider'
            df['Fecha'] = date_time
            all_data.append(df)

    final_df = pd.concat(all_data)

    Lider.clear()
    Lider.append_row(['Nombre Producto', 'EAN','Marca','Disponibilidad','Unidad','Precio Base','Precio Oferta','Imagen','Categoria','Web','Fecha'])
    Lider.append_rows(final_df.values.tolist())

if __name__ == '__main__':
    main()