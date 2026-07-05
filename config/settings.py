# config/settings.py : Configuration centralisée du pipeline
#
# Toutes les constantes, paramètres et variables d'environnement sont
# rassemblés ici. Le reste du code importe depuis ce fichier.
#
# SÉCURITÉ :
#   Ne jamais écrire de clé API ou mot de passe en dur dans le code.
#   Utiliser un fichier .env (copier .env.example → .env et le remplir).
#   Le fichier .env ne doit JAMAIS être commité dans Git (.gitignore).


import os
from dotenv import load_dotenv

# Charge les variables depuis le fichier .env situé à la racine du projet
# Si une variable est déjà dans l'environnement système, elle n'est pas écrasée
load_dotenv()



# APIs FINANCIÈRES


# Alpha Vantage
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "")
ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"

# Twelve Data
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "")
TWELVE_DATA_BASE_URL = "https://api.twelvedata.com"

# Liste des tickers à traiter
# Format : liste de symboles boursiers (marchés US par défaut)
TICKERS = [
    "AAPL",   # Apple
    "MSFT",   # Microsoft
    "TSLA",   # Tesla
    "AMZN",   # Amazon
    "GOOGL",  # Alphabet (Google)
    "META",   # Meta (Facebook)
    "NVDA",   # NVIDIA
    "NFLX",   # Netflix
    "JPM",    # JPMorgan Chase
    "V",      # Visa
]

# Source principale et source de secours
PRIMARY_SOURCE   = "alphavantage"   # Utilisée en premier
FALLBACK_SOURCE  = "twelvedata"     # Utilisée si la source principale échoue

# Paramètres d'appel API
API_TIMEOUT_SECONDS     = 15    # Timeout par requête
API_MAX_RETRIES         = 3     # Nb de tentatives en cas d'échec
API_RETRY_DELAY_SECONDS = 5     # Attente entre deux tentatives (backoff)
API_RATE_LIMIT_DELAY    = 12    # Secondes entre deux appels (Alpha Vantage free: 5/min)



# AZURE BLOB STORAGE


AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
AZURE_STORAGE_ACCOUNT   = os.getenv("AZURE_STORAGE_ACCOUNT", "")

# Noms des containers Azure Blob (= "dossiers racine")
AZURE_CONTAINER_BRONZE = "bronze"   # Données brutes JSON
AZURE_CONTAINER_SILVER = "silver"   # Données nettoyées Parquet
AZURE_CONTAINER_GOLD   = "gold"     # Données enrichies (indicateurs)
AZURE_CONTAINER_LOGS   = "logs"     # Logs du pipeline

# Structure des chemins dans Azure Blob
# bronze/{ticker}/{YYYY-MM-DD}.json
# silver/{ticker}/{YYYY-MM}.parquet
# gold/stock_analytics_{YYYY-MM}.parquet
AZURE_BRONZE_PATH_TEMPLATE = "{ticker}/{date}.json"
AZURE_SILVER_PATH_TEMPLATE = "{ticker}/{year_month}.parquet"
AZURE_GOLD_PATH_TEMPLATE   = "stock_analytics_{year_month}.parquet"



# BASE DE DONNÉES MySQL


MYSQL_HOST     = os.getenv("MYSQL_HOST", "localhost")   # localhost depuis Windows
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "stockdb")
MYSQL_USER     = os.getenv("MYSQL_USER", "hduser")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")

# URL de connexion SQLAlchemy
# pymysql = driver Python pur (pip install pymysql), pas besoin de client MySQL natif
MYSQL_URL = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    f"?charset=utf8mb4"
)

# Paramètres d'insertion pandas vers MySQL
MYSQL_CHUNK_SIZE  = 500    # Lignes insérées par batch (évite les timeouts)
MYSQL_IF_EXISTS   = "append"  # Ne pas écraser la table existante



# INDICATEURS TECHNIQUES


SMA_WINDOWS  = [20, 50]    # Périodes des moyennes mobiles simples
EMA_WINDOWS  = [20, 50]    # Périodes des moyennes mobiles exponentielles
RSI_PERIOD   = 14          # Période RSI (standard Wilder)
BBANDS_PERIOD = 20         # Période Bollinger Bands
BBANDS_STD    = 2          # Nombre d'écarts-types pour les bandes

# Seuils RSI pour les alertes Prometheus
RSI_OVERBOUGHT  = 70       # Au-dessus = zone de surachat
RSI_OVERSOLD    = 30       # En dessous = zone de survente



# MONITORING PROMETHEUS


PROMETHEUS_PORT = 8000     # Port exposé sur Windows (scrape par Prometheus Docker)
                            # Accessible depuis Docker via host.docker.internal:8000



# PIPELINE — PARAMÈTRES GÉNÉRAUX


# Dossier de logs local (sur Windows)
LOG_DIR   = os.path.join(os.path.dirname(__file__), "..", "logs")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")   # DEBUG, INFO, WARNING, ERROR

# Nombre de jours d'historique à récupérer au premier run
HISTORY_DAYS = 365   # 1 an d'historique

# Taille minimale d'un DataFrame Silver acceptable (si moins de lignes = erreur données)
MIN_ROWS_THRESHOLD = 10



# VALIDATION AU DÉMARRAGE
# Vérifie que les variables critiques sont bien configurées.
# Appelée dans main.py avant le démarrage du pipeline.


def validate_config() -> list[str]:
    """
    Vérifie que toutes les variables d'environnement obligatoires sont définies.
    Retourne une liste d'erreurs (vide si tout est OK).
    """
    errors = []

    if not ALPHA_VANTAGE_API_KEY:
        errors.append("ALPHA_VANTAGE_API_KEY manquante dans .env")

    if not TWELVE_DATA_API_KEY:
        errors.append("TWELVE_DATA_API_KEY manquante dans .env")

    if not AZURE_CONNECTION_STRING:
        errors.append("AZURE_STORAGE_CONNECTION_STRING manquante dans .env")

    if not MYSQL_PASSWORD or MYSQL_PASSWORD == "stock_pass":
        errors.append("MYSQL_PASSWORD non sécurisé - changer dans .env")

    return errors
