import os
import time
from datetime import datetime
import requests
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

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
    "8.8.8.8",       # Google DNS
    "1.1.1.1",       # Cloudflare DNS
    "9.9.9.9",       # Quad9 DNS
    "208.67.222.222" # OpenDNS
]

# -------------------
# Extract function
# -------------------
def extract_shodan_data(ip: str):
    """Fetch raw Shodan data for a given IP"""
    url = f"https://api.shodan.io/shodan/host/{ip}?key={SHODAN_API_KEY}"
    response = requests.get(url, timeout=15)
    if response.status_code != 200:
        raise Exception(f"Error {response.status_code} for IP {ip}: {response.text}")
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
        "ingested_at": datetime.utcnow(),
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
        time.sleep(1)  # Respect Shodan free-tier rate limit

if __name__ == "__main__":
    run_etl()
