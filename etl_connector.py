import os
import time
from datetime import datetime
import requests
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from datetime import timezone

# -------------------
# Load env variables
# -------------------
load_dotenv()
SHODAN_API_KEY = os.getenv("SHODAN_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "ssn_etl")
CONNECTOR_NAME = os.getenv("CONNECTOR_NAME", "shodan_connector")

# -------------------
# MongoDB setup
# -------------------
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[f"{CONNECTOR_NAME}_raw"]

# -------------------
# List of IPs to fetch
# -------------------
IP_LIST = [
    '''
    "8.8.8.8",       # Google DNS
    "1.1.1.1",       # Cloudflare DNS
    "9.9.9.9",       # Quad9 DNS
    "208.67.222.222", # OpenDNS
    "151.101.1.69",   #Fastly CDN
    "104.26.2.33",     #Cloudflare site
    "13.107.42.12", #Microsoft'''
    "172.217.164.110", #google web endpoint
    "192.0.66.2", #wordpress.com
    "151.101.129.69", #stack overflow via Fastly
    "23.35.112.101", #Akamai edge mode
    "91.189.91.39", #ubuntu archive server
    "129.250.35.250", #NIT Comm
    "185.199.108.153", #github pages
]

# -------------------
# Extract function
# -------------------
def extract_shodan_data(ip: str):
    """Fetch raw Shodan data for a given IP"""
    url = f"https://api.shodan.io/shodan/host/{ip}?key={SHODAN_API_KEY}"
    '''response = requests.get(url, timeout=60)
    if response.status_code != 200:
        raise Exception(f"Error {response.status_code} for IP {ip}: {response.text}")
    return response.json()'''
    for attempt in range(3):
        try:
            response = requests.get(url, timeout=90)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.ReadTimeout:
            print(f"Timeout for {ip}, retrying ({attempt+1}/3)...")
            time.sleep(5)
    raise Exception(f"Failed after retries for {ip}")
    return response.json()

# -------------------
# Transform function
# -------------------
def transform_shodan_data(raw: dict):
    """Flatten and clean Shodan JSON for MongoDB"""
    transformed = {
        "ip": raw.get("ip_str"),
        "organization": raw.get("org"),
        "asn": raw.get("asn"),
        "city": raw.get("city"),
        "country_name": raw.get("country_name"),
        "latitude": raw.get("latitude"),
        "longitude": raw.get("longitude"),
        "open_ports_count": len(raw.get("data", [])),
        "ports": sorted([service.get("port") for service in raw.get("data", []) if service.get("port")]),
        "services": [],
        "ingested_at": datetime.now(timezone.utc),
        "source": "shodan_host_api"
    }

    # Flatten service-level data
    for service in raw.get("data", []):
        service_doc = {
            "port": service.get("port"),
            "transport": service.get("transport"),
            "product": service.get("product"),
            "asn": service.get("asn"),
            "org": service.get("org"),
            "service_timestamp": None,
            "ssl_versions": service.get("ssl", {}).get("versions", []) if service.get("ssl") else []
        }
        # Convert timestamp to datetime
        ts = service.get("timestamp")
        if ts:
            try:
                service_doc["service_timestamp"] = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except Exception:
                service_doc["service_timestamp"] = ts
        transformed["services"].append(service_doc)

    return transformed

# -------------------
# Load function
# -------------------
def load_to_mongo(doc: dict):
    """Upsert transformed doc into MongoDB"""
    collection.update_one(
        {"ip": doc["ip"]},
        {"$set": doc},
        upsert=True
    )

# -------------------
# ETL runner
# -------------------
def run_etl():
    for ip in IP_LIST:
        print(f"Processing IP: {ip}")
        try:
            raw_data = extract_shodan_data(ip)
            transformed_data = transform_shodan_data(raw_data)
            load_to_mongo(transformed_data)
            print(f"Inserted/Updated: {ip}")
        except Exception as e:
            print(f"Failed for {ip}: {e}")
            time.sleep(3)  # Respect Shodan free-tier rate limit

if __name__ == "__main__":
    run_etl()
