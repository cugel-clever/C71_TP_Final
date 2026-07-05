# src/ingestion.py — Récupération des données depuis les APIs financières
#
# Ce module gère :
#   1. Les appels à l'API Alpha Vantage (source principale)
#   2. Les appels à l'API Twelve Data (source de secours / fallback)
#   3. La logique de retry avec backoff exponentiel
#   4. La gestion du rate limiting (Alpha Vantage : 5 req/min en tier gratuit)
#   5. L'enregistrement des métriques Prometheus pour chaque appel
#
# RÉSULTAT : données brutes JSON pour chaque ticker, prêtes à être
#            uploadées sur Azure Blob (couche Bronze).


import time
import json
import logging
from datetime import datetime, timedelta
from typing import Optional

import requests

from config import settings
from src.monitoring import metrics

logger = logging.getLogger(__name__)



# UTILITAIRES


def _call_with_retry(url: str, params: dict, source: str) -> Optional[dict]:
    """
    Effectue un appel HTTP GET avec retry automatique et backoff exponentiel.

    Backoff exponentiel : si l'appel échoue, on attend de plus en plus longtemps
    entre chaque tentative (5s, 10s, 20s) pour ne pas saturer l'API.

    Args:
        url    : URL de l'endpoint API
        params : paramètres GET (ticker, apikey, function, etc.)
        source : nom de la source pour les métriques ("alphavantage" ou "twelvedata")

    Returns:
        dict contenant la réponse JSON, ou None si toutes les tentatives échouent.
    """
    for attempt in range(1, settings.API_MAX_RETRIES + 1):
        start_time = time.time()

        try:
            logger.debug(f"[{source}] Tentative {attempt}/{settings.API_MAX_RETRIES} — {url}")

            response = requests.get(
                url,
                params=params,
                timeout=settings.API_TIMEOUT_SECONDS,
            )

            duration = time.time() - start_time

            # Vérifier le code HTTP -200 = OK, autre = erreur serveur
            response.raise_for_status()

            data = response.json()

            # Alpha Vantage retourne un message d'erreur DANS le JSON
            # même si le code HTTP est 200 (particularité de leur API)
            if "Error Message" in data:
                raise ValueError(f"Erreur API: {data['Error Message']}")

            if "Note" in data:
                # "Note" = message de rate limit Alpha Vantage
                raise ValueError(f"Rate limit atteint: {data['Note']}")

            # Succès — enregistrer la durée
            metrics.record_api_call(source=source, duration=duration, success=True)
            return data

        except requests.exceptions.Timeout:
            duration = time.time() - start_time
            logger.warning(f"[{source}] Timeout (tentative {attempt}) après {duration:.1f}s")
            metrics.record_api_call(source=source, duration=duration,
                                    success=False, error_type="timeout")

        except requests.exceptions.HTTPError as e:
            duration = time.time() - start_time
            logger.warning(f"[{source}] Erreur HTTP {e.response.status_code} (tentative {attempt})")
            metrics.record_api_call(source=source, duration=duration,
                                    success=False, error_type=f"http_{e.response.status_code}")

        except ValueError as e:
            # Erreur métier dans le JSON (rate limit, ticker invalide, etc.)
            duration = time.time() - start_time
            logger.warning(f"[{source}] Erreur JSON (tentative {attempt}): {e}")
            metrics.record_api_call(source=source, duration=duration,
                                    success=False, error_type="api_error")

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"[{source}] Erreur inattendue (tentative {attempt}): {e}")
            metrics.record_api_call(source=source, duration=duration,
                                    success=False, error_type="unknown")

        # Backoff exponentiel avant la prochaine tentative
        if attempt < settings.API_MAX_RETRIES:
            wait = settings.API_RETRY_DELAY_SECONDS * (2 ** (attempt - 1))
            logger.info(f"[{source}] Attente {wait}s avant nouvelle tentative...")
            time.sleep(wait)

    # Toutes les tentatives ont échoué
    logger.error(f"[{source}] Échec après {settings.API_MAX_RETRIES} tentatives")
    return None


# 
# ALPHA VANTAGE: Source principale
#

class AlphaVantageClient:
    """
    Client pour l'API Alpha Vantage.

    Documentation : https://www.alphavantage.co/documentation/
    Tier gratuit : 25 requêtes/jour, 5 requêtes/minute
    """

    def __init__(self):
        self.api_key = settings.ALPHA_VANTAGE_API_KEY
        self.base_url = settings.ALPHA_VANTAGE_BASE_URL

    def fetch_daily_ohlcv(self, ticker: str) -> Optional[dict]:
        """
        Récupère les données OHLCV journalières ajustées pour un ticker.

        'outputsize=full' retourne 20 ans d'historique (vs 100 jours pour 'compact').
        On utilise 'full' au premier run, puis 'compact' pour les runs suivants.
        À adapter selon votre logique de run incrémental.

        Args:
            ticker : symbole boursier ("AAPL", "MSFT", etc.)

        Returns:
            Dictionnaire JSON brut d'Alpha Vantage, ou None si échec.

        Structure de réponse Alpha Vantage :
        {
            "Meta Data": {
                "1. Information": "Daily Adjusted Time Series",
                "2. Symbol": "AAPL",
                "3. Last Refreshed": "2026-06-17",
                ...
            },
            "Time Series (Daily)": {
                "2026-06-17": {
                    "1. open": "185.0000",
                    "2. high": "186.5000",
                    "3. low": "184.2000",
                    "4. close": "185.9200",
                    "5. adjusted close": "185.9200",
                    "6. volume": "52000000",
                    "7. dividend amount": "0.0000",
                    "8. split coefficient": "1.0"
                },
                ...
            }
        }
        """
        logger.info(f"[AlphaVantage] récupération OHLCV pour {ticker}")

        params = {
            "function":   "TIME_SERIES_DAILY_ADJUSTED",
            "symbol":     ticker,
            "outputsize": "full",       # Historique complet (20 ans)
            "datatype":   "json",
            "apikey":     self.api_key,
        }

        data = _call_with_retry(self.base_url, params, source="alphavantage")

        if data is None:
            return None

        # Vérifier que la clé principale est présente dans la réponse
        if "Time Series (Daily)" not in data:
            logger.error(f"[AlphaVantage] Clé 'Time Series (Daily)' absente pour {ticker}")
            return None

        # Nombre de lignes récupérées pour les métriques
        n_records = len(data["Time Series (Daily)"])
        metrics.record_ingestion(ticker=ticker, source="alphavantage", n_records=n_records)
        logger.info(f"[AlphaVantage] {ticker} — {n_records} jours récupérés")

        # Respect du rate limit (5 req/min = 12s minimum entre appels)
        time.sleep(settings.API_RATE_LIMIT_DELAY)

        return data


# 
# TWELVE DATA — Source de secours (fallback)
# 

class TwelveDataClient:
    """
    Client pour l'API Twelve Data.

    Documentation : https://twelvedata.com/docs
    Tier gratuit : 800 appels/jour, 8 appels/minute
    """

    def __init__(self):
        self.api_key = settings.TWELVE_DATA_API_KEY
        self.base_url = settings.TWELVE_DATA_BASE_URL

    def fetch_daily_ohlcv(self, ticker: str,
                           outputsize: int = 5000) -> Optional[dict]:
        """
        Récupère les données OHLCV journalières depuis Twelve Data.

        Args:
            ticker     : symbole boursier
            outputsize : nombre de points de données (max 5000)

        Returns:
            Dictionnaire JSON brut de Twelve Data, ou None si échec.

        Structure de réponse Twelve Data :
        {
            "meta": {
                "symbol": "AAPL",
                "interval": "1day",
                "currency": "USD",
                ...
            },
            "values": [
                {
                    "datetime": "2026-06-17",
                    "open": "185.00000",
                    "high": "186.50000",
                    "low": "184.20000",
                    "close": "185.92000",
                    "volume": "52000000"
                },
                ...
            ],
            "status": "ok"
        }
        """
        logger.info(f"[TwelveData] Récupération OHLCV pour {ticker}")

        url = f"{self.base_url}/time_series"
        params = {
            "symbol":     ticker,
            "interval":   "1day",
            "outputsize": outputsize,
            "format":     "JSON",
            "apikey":     self.api_key,
        }

        data = _call_with_retry(url, params, source="twelvedata")

        if data is None:
            return None

        # Vérifier le statut dans la réponse Twelve Data
        if data.get("status") == "error":
            logger.error(f"[TwelveData] Erreur pour {ticker}: {data.get('message')}")
            return None

        if "values" not in data or not data["values"]:
            logger.error(f"[TwelveData] Pas de données 'values' pour {ticker}")
            return None

        n_records = len(data["values"])
        metrics.record_ingestion(ticker=ticker, source="twelvedata", n_records=n_records)
        logger.info(f"[TwelveData] {ticker} — {n_records} jours récupérés")

        return data



# ORCHESTRATEUR D'INGESTION
# Gère la logique source principale + fallback pour tous les tickers.


class IngestionManager:
    """
    Orchestre l'ingestion depuis les deux APIs.
    Essaie Alpha Vantage en premier, bascule sur Twelve Data en cas d'échec.
    """

    def __init__(self):
        self.av_client  = AlphaVantageClient()
        self.td_client  = TwelveDataClient()

    def fetch_ticker(self, ticker: str) -> tuple[Optional[dict], str]:
        """
        Récupère les données d'un ticker depuis la meilleure source disponible.

        Stratégie :
          1. Essayer Alpha Vantage (source principale)
          2. Si échec → essayer Twelve Data (fallback)
          3. Si les deux échouent → retourner None

        Args:
            ticker : symbole boursier

        Returns:
            Tuple (data_dict, source_name) où source_name indique quelle
            API a été utilisée. data_dict est None si les deux APIs échouent.
        """
        # Tentative 1 : Alpha Vantage
        logger.info(f"Ingestion {ticker} : tentative Alpha Vantage")
        data = self.av_client.fetch_daily_ohlcv(ticker)

        if data is not None:
            return data, "alphavantage"

        # Tentative 2 : Twelve Data (fallback)
        logger.warning(
            f"Alpha Vantage a échoué pour {ticker}: basculement sur Twelve Data"
        )
        data = self.td_client.fetch_daily_ohlcv(ticker)

        if data is not None:
            return data, "twelvedata"

        # Échec total
        logger.error(f"Impossible de récupérer les données pour {ticker} (toutes sources)")
        return None, ""

    def fetch_all_tickers(self) -> dict[str, tuple[dict, str]]:
        """
        Récupère les données pour tous les tickers configurés dans settings.TICKERS.

        Returns:
            Dictionnaire {ticker: (data_json, source)} pour les tickers réussis.
            Les tickers en échec sont absents du dictionnaire.
        """
        results = {}
        total   = len(settings.TICKERS)
        success = 0
        failed  = 0

        logger.info(f"Démarrage ingestion: {total} tickers à traiter")

        for i, ticker in enumerate(settings.TICKERS, 1):
            logger.info(f"[{i}/{total}] Traitement de {ticker}")

            data, source = self.fetch_ticker(ticker)

            if data is not None:
                results[ticker] = (data, source)
                success += 1
                logger.info(f" {ticker} ingéré depuis {source}")
            else:
                failed += 1
                logger.error(f" {ticker}: Échec ingestion")

        # Résumé final
        logger.info(
            f"Ingestion terminée: Succès: {success}/{total}, Échecs: {failed}/{total}"
        )
        metrics.set_run_summary(n_success=success, n_failed=failed)

        return results
