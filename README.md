# Shodan ETL Connector

**Author:** Neha Shanmitha (Roll No: 3122225001080)

---

## 📌 Overview
This ETL connector fetches data from the **Shodan Host API** for a list of IP addresses, transforms the nested JSON into a MongoDB-friendly format, and stores it in a dedicated MongoDB collection. It is designed for secure API key management, robust error handling, and repeatable ingestion without duplicates.

---

## 🌐 API Details
- **Provider:** Shodan
- **Base URL:** `https://api.shodan.io`
- **Endpoint:** `/shodan/host/{ip}`
- **Method:** `GET`
- **Authentication:** API key passed as query parameter (`?key=YOUR_API_KEY`)
- **Documentation:** [https://developer.shodan.io/api](https://developer.shodan.io/api)
- **Example Request:** https://api.shodan.io/shodan/host/8.8.8.8?key=API_KEY


---

## 🔑 Authentication & Security
1. Signing up at [https://account.shodan.io/register](https://account.shodan.io/register) to get API key.
2. Storing it in a local `.env` file (not committed to Git).
3. Example `.env`:
    ```
    SHODAN_API_KEY=api_key
    MONGO_URI=mongodb://localhost:27017
    MONGO_DB=ssn_etl
    CONNECTOR_NAME=shodan_connector
    ```

---

## MongoDB Storage Strategy
Collection name: shodan_connector_raw
Upsert strategy based on IP address to avoid duplicates.

### Fields:
- **ingested_at → First ingestion timestamp.**
- **last_updated_at → Last update timestamp.**
- **IP metadata (city, country, org, asn).**
- **Service data (per open port).**

---

## Transformations Applied
- **Flattened top-level metadata for easier querying.**
- **Counted open ports.**
- **Created sorted list of open ports.**
- **Converted timestamps to timezone-aware datetimes.**
- **Preserved service-level details.**
- **Added ingestion and update metadata.**

--- 

## Testing & Validation
- **Invalid API key/IP → Skips with error message.**
- **Empty payloads → Skips insert.**
- **Rate limit (HTTP 429) → Waits 60 seconds, retries.**
- **Connectivity errors → Retries up to 3 times.**
- **Consistent inserts → Uses update_one(..., upsert=True).**

## Input JSON:
{
    "region_code": "CA",
    "tags": [],
    "ip": 134744072,
    "area_code": null,
    "domains": [
        "dns.google"
    ],
    "hostnames": [
        "dns.google"
    ],
    "country_code": "US",
    "org": "Google LLC",
    "data": [{...},... ],
    "asn": "AS15169",
    "city": "Mountain View",
    "latitude": 38.00881,
    "isp": "Google LLC",
    "longitude": -122.11746,
    "last_update": "2025-08-10T19:54:30.864225",
    "country_name": "United States",
    "ip_str": "8.8.8.8",
    "os": null,
    "ports": [
        443,
        53
    ]
}

## Output JSON:
{
  "_id": {
    "$oid": "6899af23d34128dcedf7053e"
  },
  "ip": "8.8.8.8",
  "asn": "AS15169",
  "city": "Mountain View",
  "country_name": "United States",
  "ingested_at": {
    "$date": "2025-08-11T17:59:37.759Z"
  },
  "latitude": 38.00881,
  "longitude": -122.11746,
  "open_ports_count": 3,
  "organization": "Google LLC",
  "ports": [
    53,
    53,
    443
  ],
  "services": [
    {
      "port": 53,
      "transport": "tcp",
      "product": null,
      "asn": "AS15169",
      "org": "Google LLC",
      "service_timestamp": {
        "$date": "2025-08-11T10:46:21.366Z"
      },
      "ssl_versions": []
    },
    {
      "port": 53,
      "transport": "udp",
      "product": null,
      "asn": "AS15169",
      "org": "Google LLC",
      "service_timestamp": {
        "$date": "2025-08-11T11:26:53.781Z"
      },
      "ssl_versions": []
    },
    {
      "port": 443,
      "transport": "tcp",
      "product": null,
      "asn": "AS15169",
      "org": "Google LLC",
      "service_timestamp": {
        "$date": "2025-08-11T10:46:21.420Z"
      },
      "ssl_versions": [
        "-TLSv1",
        "-SSLv2",
        "-SSLv3",
        "-TLSv1.1",
        "TLSv1.2",
        "TLSv1.3"
      ]
    }
  ],
  "source": "shodan_host_api",
  "last_updated_at": {
    "$date": "2025-08-11T17:59:37.759Z"
  }
}
