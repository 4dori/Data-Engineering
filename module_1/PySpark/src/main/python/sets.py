from pydantic import BaseSettings
class Settings(BaseSettings): 
    SPARK_MASTER: str = "local[*]" 
    AZURE_ACC_1: str = "" 
    AZURE_CLIENT_ID_1: str = "" 
    AZURE_CLIENT_SECRET_1: str = "" 
    AZURE_TENANT_ID_1: str = "" 
    AZURE_ACC_2: str = "" 
    AZURE_ACC_KEY_2 = ""
    GEOCODE_API: str = "" 
    GEOCODE_API_KEY: str = "" 
    PATH_HOTELS: str = "" 
    PATH_WEATHER: str = "" 
    PATH_RESULT: str = "" 
    
    class Config: 
        env_file = ".env" 
        case_sensitive = True
    
settings = Settings()