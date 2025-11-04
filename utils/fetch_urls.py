import serpapi
import os 
import json 
import time
from config.paths import DATA_DIR
from dotenv import load_dotenv

load_dotenv()

serpapi_key = os.getenv('SERPAPI_KEY')
client = serpapi.Client(api_key=serpapi_key)

json_path = DATA_DIR / 'hotels.json'

with open(json_path, "r", encoding="utf-8") as f:
    hotels = json.load(f)

for hotel in hotels:
    name = hotel["name"]
    location = hotel["location"]
    query = f"site:tripadvisor.com {name} {location}"

    try:
        result = client.search(
            q = query,
            engine = "google"
        )
        tripadvisor_url = None
        for item in result["organic_results"]:
            link = item["link"]
            if "tripadvisor.com/Hotel_Review" in link:
                tripadvisor_url = link 
                break
        
        hotel["url"] = tripadvisor_url or "NOT_FOUND"

    except Exception as e:
        hotel["url"] = "ERROR"
        print(f"Error with hotel {name}: {e}")
    
    time.sleep(1.5)

with open(json_path, "w", encoding="utf-8") as f:
    json.dump(hotels, f, indent=2)








