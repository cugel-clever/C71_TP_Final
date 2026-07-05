# 
# main.py — Point d'entrée du pipeline de données boursières
#
# Ce script orchestre l'ensemble du pipeline dans l'ordre :
#
#   1. INIT          : logging, validation config, démarrage métriques Prometheus
#   2. INGESTION     : récupération des données depuis Alpha Vantage / Twelve Data
#   3. BRONZE        : upload des JSON bruts sur Azure Blob Storage
#   4. SILVER        : nettoyage pandas + upload Parquet Azure + insertion MySQL
#   5. GOLD          : fusion + indicateurs + upload Parquet Azure + insertion MySQL
#   6. FINALISATION  : métriques finales, log du run, résumé
#
# EXÉCUTION :
#   python main.py                   # Run unique
#   python main.py --schedule        # Mode planifié (toutes les 6h)
#   python main.py --tickers AAPL MSFT   # Seulement certains tickers
#
# PRÉREQUIS :
#   pip install -r requirements.txt
#   remplir les variables dans le fichier.env 
# 

import argparse
import logging
import sys
import time
from datetime import datetime

#  Imports internes
from config import settings
from config.settings import validate_config
from src.monitoring import metrics
from src.ingestion import IngestionManager
from src.azure_storage import AzureStorageManager
from src.transformation import BronzeToSilver, SilverToGold
from src.database import DatabaseManager


# 
# CONFIGURATION DU LOGGING
# 

def setup_logging() -> None:
    """
    Configure le système de logging.

    Deux handlers :
      - Console (stdout) : pour voir les logs en temps réel dans le terminal
      - Fichier           : pour archiver les logs (utile en production)

    Format : [2024-01-15 14:30:25] INFO     ingestion   : Message du log
    """
    import os
    os.makedirs(settings.LOG_DIR, exist_ok=True)

    log_filename = datetime.now().strftime("%Y-%m-%d_%H-%M") + "_pipeline.log"
    log_filepath = os.path.join(settings.LOG_DIR, log_filename)

    # Format de log lisible (date, niveau, module, message)
    log_format = "[%(asctime)s] %(levelname)-8s %(name)-20s : %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL, logging.INFO),
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.StreamHandler(sys.stdout),                    # Console
            logging.FileHandler(log_filepath, encoding="utf-8"),  # Fichier
        ]
    )

    # Réduire le bruit des librairies externes
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialisé-fichier : {log_filepath}")


# 
# PIPELINE PRINCIPAL
# 

logger = logging.getLogger(__name__)


def run_pipeline(tickers: list[str] = None) -> dict:
    """
    Exécute le pipeline complet pour la liste de tickers fournie.

    Args:
        tickers : liste de symboles à traiter. Si None, utilise settings.TICKERS.

    Returns:
        Dictionnaire de résumé du run avec les statistiques clés.
    """
    run_start = time.time()

    # Utiliser la liste par défaut si aucun ticker spécifié
    if tickers:
        settings.TICKERS = tickers

    # Initialisation des services
    ingestion_mgr  = IngestionManager()
    azure_mgr      = AzureStorageManager()
    bronze2silver  = BronzeToSilver()
    silver2gold    = SilverToGold()
    db_mgr         = DatabaseManager()

    # Compteurs pour le résumé final
    stats = {
        "tickers_total":   len(settings.TICKERS),
        "tickers_success": 0,
        "tickers_failed":  0,
        "records_bronze":  0,
        "records_silver":  0,
        "records_gold":    0,
        "api_calls":       0,
        "api_errors":      0,
        "quality_score":   0.0,
    }

    #   Vérifications préalables
    logger.info("=" * 60)
    logger.info("DÉMARRAGE DU PIPELINE")
    logger.info(f"Tickers : {settings.TICKERS}")
    logger.info(f"Source principale : {settings.PRIMARY_SOURCE}")
    logger.info("=" * 60)

    # Vérifier la connexion MySQL avant de commencer
    if not db_mgr.test_connection():
        logger.error("Connexion MySQL impossible — pipeline annulé")
        return {**stats, "status": "error", "error": "MySQL connexion échouée"}

    # Initialiser les containers Azure si nécessaire
    azure_mgr.initialize_containers()

    # Enregistrer le début du run dans MySQL
    run_id = db_mgr.start_run(tickers_total=stats["tickers_total"])

    # Mesure de la durée totale avec le context manager
    with metrics.run_timer():

        # 
        # ÉTAPE 1 : INGESTION API
        # Récupérer les données brutes pour tous les tickers
        # 
        logger.info("\n ÉTAPE 1 : INGESTION API")
        raw_data = ingestion_mgr.fetch_all_tickers()
        # raw_data = { "AAPL": ({json}, "alphavantage"), "MSFT": ({json}, "twelvedata"), ... }

        if not raw_data:
            logger.error("Aucune donnée récupérée: Arrêt du pipeline")
            db_mgr.end_run(run_id, status="error",
                           error_message="Aucune donnée API",
                           **{k: 0 for k in stats if k != "tickers_total"})
            return {**stats, "status": "error"}

        logger.info(
            f"Ingestion terminée : {len(raw_data)}/{stats['tickers_total']} tickers récupérés"
        )

        
        # ÉTAPES 2 + 3 : BRONZE + SILVER (par ticker)
        # Pour chaque ticker :
        #   - Upload JSON brut vers Azure Blob (Bronze)
        #   - Nettoyage pandas (Bronze vers Silver)
        #   - Upload Parquet vers Azure Blob (Silver)
        #   - Insertion dans MySQL stock_silver
        
        logger.info("\n ÉTAPES 2+3 : BRONZE & SILVER")

        silver_frames = {}   # Stocke les DataFrames Silver pour l'étape Gold

        for ticker, (raw_json, source) in raw_data.items():

            logger.info(f"\n  Traitement {ticker} (source: {source})")

            # ── BRONZE : Upload JSON brut sur Azure
            logger.info(f"  [Bronze] Upload JSON {ticker} vers Azure Blob")
            upload_success, blob_path = azure_mgr.upload_bronze_json(
                ticker=ticker,
                data_json=raw_json,
            )

            if upload_success:
                stats["records_bronze"] += 1
                # Journal de l'upload dans MySQL
                json_str = str(raw_json)
                db_mgr.insert_bronze_log(
                    ticker=ticker,
                    source=source,
                    blob_path=blob_path,
                    record_count=len(raw_json.get("Time Series (Daily)", {})),
                    file_size_bytes=len(json_str.encode("utf-8")),
                    status="success",
                )
            else:
                logger.warning(f"  [Bronze] Upload Azure échoué pour {ticker} - on continue")

            # SILVER : Nettoyage pandas
            logger.info(f"  [Silver] Nettoyage et typage {ticker}")

            if source == "alphavantage":
                df_silver = bronze2silver.transform_alphavantage(raw_json, ticker)
            else:
                df_silver = bronze2silver.transform_twelvedata(raw_json, ticker)

            if df_silver is None:
                logger.error(f"  [Silver] Transformation échouée pour {ticker}")
                stats["tickers_failed"] += 1
                continue   # Passer au ticker suivant

            logger.info(f"  [Silver] {ticker} → {len(df_silver)} lignes propres")

            # SILVER : Upload Parquet sur Azure
            logger.info(f"  [Silver] Upload Parquet {ticker} vers Azure Blob")
            year_month = datetime.now().strftime("%Y-%m")
            azure_mgr.upload_silver_parquet(
                ticker=ticker,
                df=df_silver,
                year_month=year_month,
            )

            # SILVER : Insertion MySQL
            logger.info(f"  [Silver] Insertion MySQL stock_silver → {ticker}")
            n_inserted, insert_ok = db_mgr.insert_silver(df_silver)
            stats["records_silver"] += n_inserted

            if not insert_ok:
                logger.warning(f"  [Silver] Insertion MySQL échouée pour {ticker}")

            # Garder le DataFrame Silver pour la couche Gold
            silver_frames[ticker] = df_silver
            stats["tickers_success"] += 1

            logger.info(f" {ticker} traité avec succès")

        
        # ÉTAPE 4 : GOLD
        # Fusion de tous les tickers + calcul des indicateurs techniques
        # + upload Parquet Azure + insertion MySQL
        
        logger.info("\n ÉTAPE 4 : GOLD (indicateurs)")

        if not silver_frames:
            logger.error("Aucun DataFrame Silver disponible pour créer la couche Gold")
        else:
            # Fusion de tous les tickers
            logger.info(f"[Gold] Fusion de {len(silver_frames)} tickers")
            df_merged = silver2gold.merge_tickers(silver_frames)

            # Calcul des indicateurs techniques
            logger.info("[Gold] Calcul SMA, EMA, RSI(14), VWAP, Bollinger Bands")
            df_gold = silver2gold.compute_indicators(df_merged)

            # Upload Parquet Gold sur Azure
            logger.info("[Gold] Upload Parquet vers Azure Blob (gold/)")
            azure_mgr.upload_gold_parquet(df_gold)

            # Préparer pour MySQL (nettoyage final)
            df_gold_mysql = silver2gold.prepare_for_mysql(df_gold)

            # Insertion MySQL stock_gold
            logger.info(f"[Gold] Insertion MySQL stock_gold: {len(df_gold_mysql)} lignes")
            n_gold, gold_ok = db_mgr.insert_gold(df_gold_mysql)
            stats["records_gold"] = n_gold

            if gold_ok:
                logger.info(f"[Gold]  {n_gold} lignes insérées dans stock_gold")
            else:
                logger.error("[Gold]  Insertion MySQL Gold échouée")

        
        # ÉTAPE 5 : FINALISATION
        
        logger.info("\nÉTAPE 5 : FINALISATION")

        # Calculer le score de qualité global
        total_tickers    = stats["tickers_success"] + stats["tickers_failed"]
        quality          = stats["tickers_success"] / total_tickers if total_tickers else 0
        stats["quality_score"] = quality
        metrics.record_quality_score(quality)

        # Mettre à jour les métriques Prometheus de taille de tables
        db_mgr.update_all_table_sizes()

        # Déterminer le statut final
        if stats["tickers_failed"] == 0:
            final_status = "success"
            metrics.mark_success()
        elif stats["tickers_success"] > 0:
            final_status = "partial"
        else:
            final_status = "error"

        # Clôturer le run dans MySQL
        db_mgr.end_run(
            run_id=run_id,
            status=final_status,
            tickers_success=stats["tickers_success"],
            tickers_failed=stats["tickers_failed"],
            records_bronze=stats["records_bronze"],
            records_silver=stats["records_silver"],
            records_gold=stats["records_gold"],
            api_calls=0,     # À récupérer depuis les compteurs Prometheus si besoin
            api_errors=0,
            quality_score=quality * 100,
        )

    #Résumé final
    elapsed = time.time() - run_start
    logger.info("\n" + "*" * 60)
    logger.info("Recap du lancement(éxécution): ")
    logger.info("*" * 60)
    logger.info(f"  Statut         : {final_status.upper()}")
    logger.info(f"  Durée totale   : {elapsed:.1f}s")
    logger.info(f"  Tickers        : {stats['tickers_success']}/{stats['tickers_total']} réussis")
    logger.info(f"  Bronze         : {stats['records_bronze']} fichiers Azure")
    logger.info(f"  Silver         : {stats['records_silver']} lignes MySQL")
    logger.info(f"  Gold           : {stats['records_gold']} lignes MySQL")
    logger.info(f"  Qualité        : {quality:.1%}")
    logger.info("=" * 60)

    return {**stats, "status": final_status, "duration_seconds": elapsed}


 
# MODE PLANIFIÉ
 

def run_scheduled(interval_hours: float = 3.0) -> None:
    """
    Lance le pipeline en mode planifié- s'exécute toutes les N heures.

    

    Args:
        interval_hours : intervalle entre chaque run (défaut : 6h)
    """
    import schedule

    interval_seconds = interval_hours * 3600
    logger.info(f"Mode planifié activé- lancement toutes les {interval_hours}h")

    # Premier run immédiat
    run_pipeline()

    # Planification des runs suivants
    schedule.every(interval_hours).hours.do(run_pipeline)

    while True:
        schedule.run_pending()
        time.sleep(60)   # Vérifier la planification chaque minute



# POINT D'ENTRÉE


def main() -> None:
    """Point d'entrée principal — parse les arguments et lance le pipeline."""

    # Arguments CLI
    parser = argparse.ArgumentParser(
        description="Pipeline de données boursières: Alpha Vantage + Twelve Data vers Azure Blob vers MySQL"
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        help="Liste de tickers à traiter (exemple: --tickers AAPL MSFT TSLA). "
             "Par défaut : utilise settings.TICKERS",
    )
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Activer le mode planifié (run toutes les 3h)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=3.0,
        help="Intervalle en heures entre les runs en mode planifié (défaut: 3)",
    )
    args = parser.parse_args()

    # Setup
    setup_logging()

    # Valider la configuration avant de démarrer
    config_errors = validate_config()
    if config_errors:
        for error in config_errors:
            logger.error(f"Configuration : {error}")
        logger.error("Corriger le fichier .env avant de relancer.")
        sys.exit(1)

    # Démarrer le serveur métriques Prometheus
    # (port 8000, scrape par Prometheus Docker via host.docker.internal:8000)
    metrics.start()

    # Lancement
    if args.schedule:
        run_scheduled(interval_hours=args.interval)
    else:
        result = run_pipeline(tickers=args.tickers)
        # Code de sortie : 0 = succès, 1 = erreur partielle ou totale
        sys.exit(0 if result.get("status") == "success" else 1)


if __name__ == "__main__":
    main()
