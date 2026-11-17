
import os
import json
import time
import hashlib
import requests

from pathlib import Path
from dotenv import load_dotenv
from kafka import KafkaProducer

# --------------------------------------------------
# 1. Load project configuration
# --------------------------------------------------

# Find the KrishiSignal project folder
PROJECT_DIR = Path(__file__).resolve().parent.parent

# Load API key and settings from the project's .env
load_dotenv(PROJECT_DIR / ".env")

API_KEY = os.getenv("DATA_GOV_API_KEY")

if not API_KEY:
    raise ValueError("DATA_GOV_API_KEY is missing in .env")

API_URL = (
    "https://api.data.gov.in/resource/"
    "9ef84268-d588-465a-a308-a864a43d0070"
)

KAFKA_SERVER = os.getenv(
    "KAFKA_BOOTSTRAP", "localhost:9092"
)

KAFKA_TOPIC = "mandi-prices"

# Fetch new data every 15 minutes
POLL_SECONDS = 900

# File used to remember previously published records
STATE_FILE = PROJECT_DIR / "data" / "published_mandi_fingerprints.json"
STATE_FILE.parent.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------
# 2. Connect to Kafka
# --------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda data: json.dumps(data).encode("utf-8")#Python to Json
)


# --------------------------------------------------
# 3. Load already-published record IDs
# --------------------------------------------------

def load_published():
    if not STATE_FILE.exists():
        return set()

    return set(json.loads(STATE_FILE.read_text()))


def save_published(record_ids):
    # Save to a temporary file before replacing the old file
    temp_file = STATE_FILE.with_suffix(".tmp")
    temp_file.write_text(json.dumps(list(record_ids)))
    temp_file.replace(STATE_FILE)


# --------------------------------------------------
# 4. Fetch mandi data from the API
# --------------------------------------------------

def fetch_mandi_data():
    all_records = []
    offset = 0
    page_size = 100

    while True:
        params = {
            "api-key": API_KEY,
            "format": "json",
            "filters[state]": "Gujarat",
            "limit": page_size,
            "offset": offset
        }

        # Send API request
        response = requests.get(
            API_URL,
            params=params,
            timeout=30
        )

        # Stop if the API returns an HTTP error
        response.raise_for_status()

        # Convert the response to Python data
        records = response.json().get("records", [])

        if not records:
            break

        all_records.extend(records)

        # Stop when the final page has fewer records
        if len(records)< page_size:
  break

  offset +=len(records)

  return all_records


  # --------------------------------------------------
  # 5. Clean and validate each record
  # --------------------------------------------------

  def clean_record(record):
  try:
  # Read fields from the API response
  date=record.get("arrival_date" ) or record.get("date")
  district=record.get("district" )
  market=record.get("market" )
  crop=record.get("commodity" )
  price=record.get("modal_price" )

  # Required fields must be present
  if not all([date, district, market, crop, price]):
  return None

  # Convert price to a number
  price=float(str(price).replace("," ,"" ))

  if price<= 0:
   return None

   # Return data in a consistent format
   return {
"date" : date,
"district" : district,
"market" : market,
"crop" : crop,
"modal_price" : price,
"state" : record.get("state","Gujarat" ),
"source" :"data.gov.in"
   }

   except (ValueError, TypeError, AttributeError):
   # Ignore invalid records
   return None


   # --------------------------------------------------
   # 6. Continuously fetch and publish new records
   # --------------------------------------------------

   try:
   while True:
   try:
   print("Fetching Gujarat mandi prices...")

   # Load previously published fingerprints
   published_ids=load_published()

   # Fetch records from the API
   records=fetch_mandi_data()

   new_count=0

   for record in records:
   # Clean the API record
   clean_data=clean_record(record)

   if clean_data is None:
   continue

   # Generate a stable ID from the record's content
   record_text=json.dumps(
   clean_data, sort_keys=True
   )
   record_id=hashlib.sha256(
   record_text.encode("utf-8")
   ).hexdigest()

   # Skip records already published
   if record_id in published_ids:
   continue

   # Send the record to Kafka
   future=producer.send(
   KAFKA_TOPIC,
   key=record_id.encode("utf-8" ),
   value=clean_data
   )

   # Wait for Kafka acknowledgement
   future.get(timeout=60)

   # Save the ID only after successful acknowledgement
   published_ids.add(record_id)
   save_published(published_ids)

   new_count +=1
   print("Published:", clean_data)

   print("New records published:", new_count)

   except (
   requests.RequestException,
   ValueError,
   OSError
   ) as error:
   print("Error fetching or processing data:", error)

   # Wait before the next API request cycle
   print(f"Waiting {POLL_SECONDS} seconds...")
   time.sleep(POLL_SECONDS)

   except KeyboardInterrupt:
   print("Stopping KrishiSignal producer...")

   finally:
   # Close Kafka resources properly
   producer.flush()
   producer.close()
