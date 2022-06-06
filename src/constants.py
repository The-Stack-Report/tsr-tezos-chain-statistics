from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

env_cache_path = os.getenv("CACHE_PATH")
if env_cache_path == None:
    raise Exception("Missing env variable: CACHE_PATH")

cache_path = Path(env_cache_path)