from pathlib import Path
import os
import yaml
from dotenv import load_dotenv

load_dotenv()

class ConfigLoader:

    def __init__(self):
        self.project_root = Path(__file__).resolve().parent.parent
        self.config_dir = self.project_root / 'config'
        self._cache = {}

    def _load_config(self, filename : str) -> dict:
        if filename in self._cache:
            return self._cache[filename]
        
        file_path = self.config_dir / f"{filename}.yaml"

        if not file_path.exists():
            raise FileNotFoundError(f"config file not found {file_path}")
        
        with open(file_path, "r") as file:
            config = yaml.safe_load(file)
        
        if filename == "mysql":
            config["mysql"]["password"] = os.getenv("MYSQL_PASSWORD")
        elif filename == "postgres":
            config["postgres"]["password"] = os.getenv("POSTGRES_PASSWORD")

        self._cache[filename] = config

        return config
    
    def get_mysql(self):
        return self._load_config("mysql")
    
    def get_kafka(self):
        return self._load_config("kafka")
    
    def get_spark(self):
        return self._load_config("spark")
    
    def get_postgres(self):
        return self._load_config("postgres")
    
    def get_logging(self):
        return self._load_config("logging")
    
    def get_project(self):
        return self._load_config("config")
    
_config_loader = ConfigLoader()

def get_config():
    return _config_loader

