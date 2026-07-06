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

# APIs OMDb
OMDB_API_KEY = os.getenv("OMDB_API_KEY", "")
MONGODB_URI = os.getenv("MONGODB_URI", "")

# Paramètres d'appel API
API_TIMEOUT_SECONDS     = 15    # Timeout par requête
API_MAX_RETRIES         = 3     # Nb de tentatives en cas d'échec
API_RETRY_DELAY_SECONDS = 5     # Attente entre deux tentatives (backoff)
API_RATE_LIMIT_DELAY    = 12    # Secondes entre deux appels (Alpha Vantage free: 5/min)


SMA_WINDOWS  = [20, 50]    # Périodes des moyennes mobiles simples
EMA_WINDOWS  = [20, 50]    # Périodes des moyennes mobiles exponentielles
RSI_PERIOD   = 14          # Période RSI (standard Wilder)
BBANDS_PERIOD = 20         # Période Bollinger Bands
BBANDS_STD    = 2          # Nombre d'écarts-types pour les bandes

# Seuils RSI pour les alertes Prometheus
RSI_OVERBOUGHT  = 70       # Au-dessus = zone de surachat
RSI_OVERSOLD    = 30       # En dessous = zone de survente

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

    if not OMDB_API_KEY:
        errors.append("OMDB_API_KEY manquante dans .env")

    return errors
