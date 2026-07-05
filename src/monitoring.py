# src/monitoring.py — Métriques Prometheus du pipeline
#
# Ce module expose des métriques temps réel sur http://localhost:8000/metrics
# que Prometheus (dans Docker) vient scraper via host.docker.internal:8000.
#
# UTILISATION :
#   from src.monitoring import metrics
#   metrics.start()                          # À appeler une seule fois au démarrage
#   metrics.record_ingestion("AAPL", 252)    # Après chaque ingestion réussie
#   metrics.record_api_error("alphavantage") # En cas d'erreur API
#
# LIBRAIRIE :
#   pip install prometheus-client


import time
import logging
from prometheus_client import (
    Counter,        # Compteur monotone croissant (nb d'événements)
    Gauge,          # Valeur instantanée (peut monter ou descendre)
    Histogram,      # Distribution de valeurs (durées, tailles)
    start_http_server,
    REGISTRY,
)
from config import settings

logger = logging.getLogger(__name__)



# DÉFINITION DES MÉTRIQUES
#
# Convention de nommage Prometheus :
#   {pipeline}_{objet}_{unité_ou_action}_{type_suffix}
#   Ex: pipeline_api_calls_total  (Counter se termine toujours par _total)
#       pipeline_run_duration_seconds (Gauge/Histogram avec l'unité)


# Compteurs (Counter)
# Un Counter ne peut qu'augmenter: parfait pour les totaux cumulés.

records_ingested_total = Counter(
    "pipeline_records_ingested_total",
    "Nombre total de lignes ingérées depuis les APIs",
    ["ticker", "source"],   # Labels : filtrer par ticker ou par source API
)

api_calls_total = Counter(
    "pipeline_api_calls_total",
    "Nombre total d'appels aux APIs financières",
    ["source"],   # alphavantage | twelvedata
)

api_errors_total = Counter(
    "pipeline_api_errors_total",
    "Nombre total d'erreurs lors des appels API",
    ["source", "error_type"],   # source + type d'erreur (timeout, rate_limit, etc.)
)

azure_uploads_total = Counter(
    "pipeline_azure_uploads_total",
    "Nombre total de fichiers uploadés sur Azure Blob",
    ["container", "status"],   # container (bronze/silver) + status (success/error)
)

mysql_inserts_total = Counter(
    "pipeline_mysql_inserts_total",
    "Nombre total de lignes insérées dans MySQL",
    ["table", "status"],   # table (stock_silver/stock_gold) + status
)

# Jauges (Gauge)
# Une Gauge peut monter et descendre — pour les états actuels.

pipeline_running = Gauge(
    "pipeline_is_running",
    "1 si le pipeline est en cours d'exécution, 0 sinon",
)

last_success_timestamp = Gauge(
    "pipeline_last_success_timestamp_seconds",
    "Timestamp Unix du dernier run réussi (0 si jamais réussi)",
)

tickers_processed = Gauge(
    "pipeline_tickers_processed_current",
    "Nombre de tickers traités lors du dernier run",
)

tickers_failed = Gauge(
    "pipeline_tickers_failed_current",
    "Nombre de tickers en échec lors du dernier run",
)

data_quality_score = Gauge(
    "pipeline_data_quality_score",
    "Score de qualité des données (0.0 à 1.0) du dernier run",
)

rsi_current_value = Gauge(
    "pipeline_rsi_current_value",
    "Valeur RSI(14) actuelle par ticker (dernier point calculé)",
    ["ticker"],
)

mysql_table_rows = Gauge(
    "pipeline_mysql_table_rows",
    "Nombre de lignes dans chaque table MySQL",
    ["table"],
)

# Histogrammes (Histogram)
# Un Histogram mesure la distribution des valeurs (utile pour les durées).
# Il crée automatiquement _bucket, _count et _sum.

api_call_duration_seconds = Histogram(
    "pipeline_api_call_duration_seconds",
    "Durée des appels API en secondes",
    ["source"],
    buckets=[0.5, 1.0, 2.0, 5.0, 10.0, 15.0, 30.0],   # Seuils de bucket
)

azure_upload_duration_seconds = Histogram(
    "pipeline_azure_upload_duration_seconds",
    "Durée des uploads Azure Blob en secondes",
    ["container"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0],
)

mysql_insert_duration_seconds = Histogram(
    "pipeline_mysql_insert_duration_seconds",
    "Durée des insertions MySQL (df.to_sql) en secondes",
    ["table"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
)

run_duration_seconds = Histogram(
    "pipeline_run_duration_seconds",
    "Durée totale d'un run complet du pipeline",
    buckets=[30, 60, 120, 300, 600, 900, 1800],   # 30s à 30min
)



# CLASSE PRINCIPALE
# Encapsule toutes les métriques et fournit des méthodes métier claires.


class PipelineMetrics:
    """
    Gestionnaire centralisé des métriques Prometheus du pipeline.

    Usage :
        metrics = PipelineMetrics()
        metrics.start()
        with metrics.run_timer():
            # ... code du pipeline ...
    """

    def __init__(self):
        self._server_started = False

    def start(self) -> None:
        """
        Démarre le serveur HTTP Prometheus sur le port configuré.
        Ce serveur expose /metrics que Prometheus scrape depuis Docker.

        À appeler UNE SEULE FOIS au démarrage du script main.py.
        Si déjà démarré, ne fait rien (idempotent).
        """
        if self._server_started:
            return

        try:
            start_http_server(settings.PROMETHEUS_PORT)
            self._server_started = True
            logger.info(
                f"Serveur métriques Prometheus démarré sur "
                f"http://localhost:{settings.PROMETHEUS_PORT}/metrics"
            )
        except OSError as e:
            # Le port est déjà utilisé (run précédent encore actif ?)
            logger.warning(f"Impossible de démarrer le serveur métriques: {e}")

    #Méthodes d'enregistrement

    def record_api_call(self, source: str, duration: float, success: bool,
                        error_type: str = "") -> None:
        """
        Enregistre un appel API (durée + succès/échec).

        Args:
            source     : "alphavantage" ou "twelvedata"
            duration   : durée de l'appel en secondes
            success    : True si la réponse est valide
            error_type : type d'erreur si success=False ("timeout", "rate_limit", "http_error")
        """
        api_calls_total.labels(source=source).inc()
        api_call_duration_seconds.labels(source=source).observe(duration)

        if not success:
            api_errors_total.labels(
                source=source,
                error_type=error_type or "unknown"
            ).inc()

    def record_ingestion(self, ticker: str, source: str, n_records: int) -> None:
        """
        Enregistre une ingestion réussie pour un ticker donné.

        Args:
            ticker    : symbole boursier ("AAPL")
            source    : source API utilisée
            n_records : nombre de lignes récupérées
        """
        records_ingested_total.labels(ticker=ticker, source=source).inc(n_records)

    def record_azure_upload(self, container: str, duration: float,
                            success: bool) -> None:
        """
        Enregistre un upload Azure Blob.

        Args:
            container : "bronze" ou "silver"
            duration  : durée de l'upload en secondes
            success   : True si l'upload a réussi
        """
        status = "success" if success else "error"
        azure_uploads_total.labels(container=container, status=status).inc()
        azure_upload_duration_seconds.labels(container=container).observe(duration)

    def record_mysql_insert(self, table: str, n_rows: int, duration: float,
                            success: bool) -> None:
        """
        Enregistre une insertion MySQL.

        Args:
            table    : nom de la table ("stock_silver", "stock_gold")
            n_rows   : nombre de lignes insérées
            duration : durée de l'insertion en secondes
            success  : True si l'insertion a réussi
        """
        status = "success" if success else "error"
        mysql_inserts_total.labels(table=table, status=status).inc(n_rows)
        mysql_insert_duration_seconds.labels(table=table).observe(duration)

    def record_rsi(self, ticker: str, rsi_value: float) -> None:
        """
        Met à jour la valeur RSI actuelle d'un ticker.
        Utilisé pour déclencher les alertes Grafana/Prometheus (surachat/survente).

        Args:
            ticker    : symbole boursier
            rsi_value : valeur RSI(14) entre 0 et 100
        """
        rsi_current_value.labels(ticker=ticker).set(rsi_value)

    def record_quality_score(self, score: float) -> None:
        """
        Met à jour le score de qualité des données (0.0 à 1.0).

        Args:
            score : proportion de lignes valides (ex: 0.994 = 99.4%)
        """
        data_quality_score.set(score)

    def record_mysql_table_size(self, table: str, n_rows: int) -> None:
        """Met à jour le nombre de lignes dans une table MySQL."""
        mysql_table_rows.labels(table=table).set(n_rows)

    def set_run_summary(self, n_success: int, n_failed: int) -> None:
        """
        Met à jour les jauges de résumé du run.

        Args:
            n_success : nb de tickers traités avec succès
            n_failed  : nb de tickers en échec
        """
        tickers_processed.set(n_success)
        tickers_failed.set(n_failed)

    def mark_success(self) -> None:
        """Enregistre le timestamp du dernier run réussi (maintenant)."""
        last_success_timestamp.set(time.time())

    def set_running(self, is_running: bool) -> None:
        """Met à jour l'état d'exécution du pipeline (0 ou 1)."""
        pipeline_running.set(1 if is_running else 0)

    def run_timer(self):
        """
        Context manager pour mesurer la durée totale d'un run.

        Usage :
            with metrics.run_timer():
                run_pipeline()
        """
        return _RunTimer()


class _RunTimer:
    """Context manager interne pour chronométrer un run complet."""

    def __enter__(self):
        self._start = time.time()
        pipeline_running.set(1)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self._start
        run_duration_seconds.observe(duration)
        pipeline_running.set(0)
        logger.info(f"Durée totale de l'exécution : {duration:.1f}s")
        return False   # Ne supprime pas l'exception si elle existe


# Instance globale — importée dans tous les autres modules
metrics = PipelineMetrics()
