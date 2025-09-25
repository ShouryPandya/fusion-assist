import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
    AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
    ORACLE_FUSION_URL = os.getenv("ORACLE_FUSION_URL")
    ORACLE_FUSION_USER = os.getenv("ORACLE_FUSION_USER")
    ORACLE_FUSION_PASS = os.getenv("ORACLE_FUSION_PASS")
    ORACLE_BIP_ENDPOINT: str = ORACLE_FUSION_URL + "/xmlpserver/services/ExternalReportWSSService?wsdl"
    ORACLE_FUSION_ENDPOINT= ORACLE_FUSION_URL
    BASE_URL = "http://10.1.0.124:8080"
    ORACLE_DB_USERNAME = "AI_POC"
    ORACLE_DB_PASSWORD = "Mastek@123456"  
    ORACLE_DB_DSN = "aipocatp_high" 
    ORACLE_DB_CONFIG_DIR = r"/Wallet"
    ORACLE_DB_WALLET_LOCATION = r"/Wallet"
    ORACLE_DB_WALLET_PASSWORD = "Mastek@123456" # Replace with your wallet password