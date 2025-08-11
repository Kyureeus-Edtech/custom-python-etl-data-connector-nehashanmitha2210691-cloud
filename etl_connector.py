from dotenv import load_dotenv
import os

# Load variables from .env
load_dotenv()

# Access them
SHODAN_API_KEY = os.getenv("SHODAN_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
CONNECTOR_NAME = os.getenv("CONNECTOR_NAME")
