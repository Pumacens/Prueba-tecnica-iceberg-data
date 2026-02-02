import pandas as pd
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import os

# ================= CONFIGURACIÓN =================
INPUT_FILE = "goofish_urls.csv"
OUTPUT_FILE = "goofish_data_completed.csv"
API_URL = "http://127.0.0.1:8080/scrapePDP"
MAX_WORKERS = 10  
SAVE_EVERY = 5 
# =================================================

def get_data_from_api(url):
    """
    Llama a tu microservicio local.
    """
    try:
        response = requests.get(API_URL, params={"url": url}, timeout=60)
        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, list) and len(data) > 0:
                return data[0] 
    except Exception as e:
        pass
    return None


def main():
    if os.path.exists(OUTPUT_FILE):
        print(f"Retomando trabajo desde {OUTPUT_FILE}...")
        df = pd.read_csv(OUTPUT_FILE, dtype=str)
    else:
        print(f"Cargando archivo original {INPUT_FILE}...")
        df = pd.read_csv(INPUT_FILE, dtype=str)

    df = df.head(10000)

    columns_to_write = [
        'ITEM_ID', 'CATEGORY_ID', 'TITLE', 'IMAGES', 
        'SOLD_PRICE', 'BROWSE_COUNT', 'WANT_COUNT', 
        'COLLECT_COUNT', 'QUANTITY', 'GMT_CREATE', 'SELLER_ID'
    ]
    
    for col in columns_to_write:
        if col in df.columns:
            df[col] = df[col].astype('object')
    # -------------------------

    pending_mask = df['TITLE'].isna() | (df['TITLE'] == "") | (df['TITLE'].astype(str).str.lower() == "nan")
    pending_indices = df[pending_mask].index.tolist()

    print(f"Total filas: {len(df)}")
    print(f"Pendientes por procesar: {len(pending_indices)}")

    if len(pending_indices) == 0:
        print("✅ Todo completado. No hay nada que procesar.")
        return

    processed_count = 0
    print("Iniciando procesamiento...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_index = {
            executor.submit(get_data_from_api, df.loc[idx, 'URL']): idx 
            for idx in pending_indices
        }

        pbar = tqdm(total=len(pending_indices), unit="item")

        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            try:
                result = future.result()
                
                if result:
                    if "ERROR" in result:
                        df.at[idx, 'TITLE'] = "ERROR_API"
                    else:
                        # Convertimos todo a string antes de asignar para seguridad
                        df.at[idx, 'ITEM_ID'] = str(result.get('ITEM_ID', ''))
                        df.at[idx, 'CATEGORY_ID'] = str(result.get('CATEGORY_ID', ''))
                        df.at[idx, 'TITLE'] = str(result.get('TITLE', '')).replace("\n", " ")
                        
                        imgs = result.get('IMAGES')
                        if isinstance(imgs, list):
                            df.at[idx, 'IMAGES'] = "|".join(imgs)
                        else:
                            df.at[idx, 'IMAGES'] = str(imgs)

                        df.at[idx, 'SOLD_PRICE'] = str(result.get('SOLD_PRICE', ''))
                        df.at[idx, 'BROWSE_COUNT'] = str(result.get('BROWSE_COUNT', ''))
                        df.at[idx, 'WANT_COUNT'] = str(result.get('WANT_COUNT', ''))
                        df.at[idx, 'COLLECT_COUNT'] = str(result.get('COLLECT_COUNT', ''))
                        df.at[idx, 'QUANTITY'] = str(result.get('QUANTITY', ''))
                        df.at[idx, 'GMT_CREATE'] = str(result.get('GMT_CREATE', ''))
                        df.at[idx, 'SELLER_ID'] = str(result.get('SELLER_ID', ''))
                else:
                    df.at[idx, 'TITLE'] = "ERROR_CONNECTION"

            except Exception as e:
                df.at[idx, 'TITLE'] = "ERROR_SCRIPT"
            
            processed_count += 1
            pbar.update(1)

            if processed_count % SAVE_EVERY == 0:
                df.to_csv(OUTPUT_FILE, index=False)
                pbar.set_description(f"Guardado parc. ({processed_count})")

        pbar.close()

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n¡Proceso finalizado! Datos guardados en {OUTPUT_FILE}")



if __name__ == "__main__":
    main()