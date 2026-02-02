import time
import json
import hashlib
import random
import re
from urllib.parse import urlparse, parse_qs
from curl_cffi import requests as cffi_requests
from playwright.sync_api import sync_playwright

PROXY_CONF = {
    "host": "gw.netnut.net",
    "port": "5959",
    "user": "", 
    "pass": ""
}

class GoofishScraper:
    def __init__(self, proxy_config):
        self.cookies_dict = {}
        self.token = None
        self.app_key = "34839810"
        self.user_agent_str = None 
        self.proxy_config = proxy_config
        self.session = cffi_requests.Session(impersonate="chrome124")
        
        self.cache = {}

    def get_proxy_url(self):
        session_id = random.randint(1000000, 9999999)
        username_constructed = f"{self.proxy_config['user']}-sid-{session_id}"
        proxy_url = f"http://{username_constructed}:{self.proxy_config['pass']}@{self.proxy_config['host']}:{self.proxy_config['port']}"
        
        return {
            "server": f"http://{self.proxy_config['host']}:{self.proxy_config['port']}",
            "username": username_constructed,
            "password": self.proxy_config['pass'],
            "full_url": proxy_url
        }

    def init_playwright_session(self):
        print("--- Iniciando Navegador (Fase Auth) ---")
        proxy_data = self.get_proxy_url()
        self.session.proxies = {"http": proxy_data["full_url"], "https": proxy_data["full_url"]}

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True, 
                args=['--disable-blink-features=AutomationControlled']
            )
            
            context = browser.new_context(
                proxy={"server": proxy_data["server"], "username": proxy_data["username"], "password": proxy_data["password"]},
                viewport={'width': 1280, 'height': 800},
                locale='es-ES',
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
            
            context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            page = context.new_page()
            
            try:
                print("Navegando a Goofish para obtener Token...")
                page.goto("https://www.goofish.com/item?id=995598771021", timeout=60000, wait_until="domcontentloaded")
                
                self.user_agent_str = page.evaluate("navigator.userAgent")
                
                page.mouse.wheel(0, 500)
                time.sleep(2)

                print("Esperando cookies de autenticación...")
                for i in range(10):
                    cookies = context.cookies()
                    cookie_map = {c['name']: c['value'] for c in cookies}
                    h5_tk = cookie_map.get('_m_h5_tk')
                    
                    if h5_tk:
                        self.token = h5_tk.split('_')[0]
                        self.cookies_dict = cookie_map
                        
                        self.session.cookies.clear()
                        self.session.cookies.update(self.cookies_dict)
                        
                        print(f"Token obtenido: {self.token}")
                        browser.close()
                        return True
                    
                    time.sleep(1.5)
                
                print("No se encontró la cookie _m_h5_tk")
                browser.close()
                return False

            except Exception as e:
                print(f"Error en Playwright: {e}")
                browser.close()
                return False

    def get_product_details(self, item_id):
        if item_id in self.cache:
            print(f"✅ Recuperado de caché: {item_id}")
            return self.cache[item_id]

        if not self.token:
            if not self.init_playwright_session(): 
                return {"error": "Authentication Failed"}

        t = str(int(time.time() * 1000))
        app_key = self.app_key
        
        data_obj = {"itemId": str(item_id)}
        data_json = json.dumps(data_obj, separators=(',', ':'))
        
        str_to_sign = f"{self.token}&{t}&{app_key}&{data_json}"
        sign = hashlib.md5(str_to_sign.encode('utf-8')).hexdigest()

        params = {
            "jsv": "2.7.2", "appKey": app_key, "t": t, "sign": sign,
            "v": "1.0", "type": "originaljson", "api": "mtop.taobao.idle.pc.detail",
            "sessionOption": "AutoLoginOnly", "spm_cnt": "a21ybx.item.0.0"
        }

        headers = {
            'authority': 'h5api.m.goofish.com',
            'accept': 'application/json',
            'content-type': 'application/x-www-form-urlencoded',
            'origin': 'https://www.goofish.com',
            'referer': 'https://www.goofish.com/',
            'user-agent': self.user_agent_str,
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
        }

        try:
            url = "https://h5api.m.goofish.com/h5/mtop.taobao.idle.pc.detail/1.0/"
            post_data = {'data': data_json} 
            
            print(f"Solicitando API para ID: {item_id}...")
            response = self.session.post(url, params=params, data=post_data, headers=headers, timeout=10)
            
            data = response.json()
            ret_code = data.get('ret', [''])[0]
            
            if "SUCCESS" in ret_code:
                result = self.parse_data(data, item_id)
                self.cache[item_id] = result
                return result
            
            elif "RGV587" in ret_code:
                print(f"BLOQUEO Detectado. Renovando token...")
                self.token = None 
                return self.get_product_details(item_id)
            else:
                return {"ITEM_ID": item_id, "ERROR": f"API Error: {ret_code}"}

        except Exception as e:
            print(f"Error de conexión: {e}")
            return {"ITEM_ID": item_id, "ERROR": str(e)}
        
    def parse_data(self, json_response, original_id):
        try:
            data = json_response.get("data", {})
            track_params = data.get("trackParams", {})
            item_do = data.get("itemDO", {})

            extracted_info = {
                "ITEM_ID": str(track_params.get("itemId", original_id)),
                "CATEGORY_ID": str(track_params.get("categoryId", "")),
                "TITLE": item_do.get("desc", ""),
                "IMAGES": track_params.get("mainPic", []),
                "SOLD_PRICE": str(item_do.get("soldPrice", "0")),
                "BROWSE_COUNT": int(item_do.get("browseCnt", 0)),
                "WANT_COUNT": int(item_do.get("wantCnt", 0)),
                "COLLECT_COUNT": int(item_do.get("collectCnt", 0)),
                "QUANTITY": int(item_do.get("quantity", 0)),
                "GMT_CREATE": str(item_do.get("gmtCreate", "")),
                "SELLER_ID": str(track_params.get("sellerId", ""))
            }
            return extracted_info
        except Exception as e:
            return {"ITEM_ID": original_id, "ERROR": f"Parse Error: {e}"}

scraper_instance = GoofishScraper(PROXY_CONF)

def scrape_pdp(url: str):
    """
    Función llamada por el endpoint de FastAPI.
    1. Parsea la URL para sacar el ID.
    2. Llama al scraper global.
    3. Retorna una lista como pide el endpoint.
    """
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    
    item_id = query_params.get('id', [None])[0]
    
    if not item_id:
        match = re.search(r'id=(\d+)', url)
        if match:
            item_id = match.group(1)
    
    if not item_id:
        return [{"ERROR": "Invalid URL provided, could not extract ID"}]

    result = scraper_instance.get_product_details(item_id)
    
    return [result]
