# src/database.py — Interactions avec MySQL
#
# Ce module gère :
#   1. La connexion à MySQL via SQLAlchemy
#   2. L'insertion des données Silver et Gold via pandas df.to_sql()
#   3. L'enregistrement des runs dans la table pipeline_runs
#   4. La mise à jour des métriques Prometheus sur les tailles de tables
#
# LIBRAIRIES :
#   pip install sqlalchemy pymysql
#   (pymysql = driver pur Python, pas besoin d'installer MySQL client)


import logging
import time
import uuid
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import SQLAlchemyError

from config import settings
from src.monitoring import metrics

logger = logging.getLogger(__name__)



# CONNEXION


def _create_engine() -> Engine:
    """
    Crée et retourne un moteur SQLAlchemy connecté à MySQL.

    pool_pre_ping=True : vérifie la connexion avant chaque utilisation
                          (évite les erreurs "MySQL server has gone away"
                          si la connexion a été inactive trop longtemps)
    pool_size=5        : nombre de connexions dans le pool
    max_overflow=10    : connexions supplémentaires autorisées en surcharge

    Returns:
        Engine SQLAlchemy prêt à l'emploi.
    """
    engine = create_engine(
        settings.MYSQL_URL,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        pool_recycle=3600,   # Recycler les connexions après 1h (évite les timeouts)
    )
    logger.info(
        f"Connexion MySQL initialisée : "
        f"{settings.MYSQL_HOST}:{settings.MYSQL_PORT}/{settings.MYSQL_DATABASE}"
    )
    return engine



# CLASSE PRINCIPALE


class DatabaseManager:
    """
    Gestionnaire des opérations MySQL pour le pipeline.
    """

    def __init__(self):
        self._engine: Optional[Engine] = None

    @property
    def engine(self) -> Engine:
        """Retourne le moteur SQLAlchemy (lazy init)."""
        if self._engine is None:
            self._engine = _create_engine()
        return self._engine

    def test_connection(self) -> bool:
        """
        Vérifie que la connexion à MySQL fonctionne.
        À appeler au démarrage du pipeline.

        Returns:
            True si la connexion est OK, False sinon.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            logger.info("✓ Connexion MySQL OK")
            return True
        except SQLAlchemyError as e:
            logger.error(f"✗ Connexion MySQL ÉCHOUÉE : {e}")
            return False

    # Insertion des données

    def insert_silver(self, df: pd.DataFrame) -> tuple[int, bool]:
        """
        Insère les données Silver dans la table stock_silver.

        Utilise INSERT IGNORE via if_exists='append' + gestion des doublons
        au niveau de la contrainte unique de MySQL (symbol, date, source).

        Args:
            df : DataFrame Silver nettoyé (colonnes: symbol, date, open, high,
                 low, close, volume, source)

        Returns:
            Tuple (nb_lignes_insérées, success: bool)
        """
        if df is None or df.empty:
            logger.warning("[MySQL] DataFrame Silver vide: insertion ignorée")
            return 0, True

        table_name = "stock_silver"
        start_time = time.time()

        try:
            # df.to_sql() insère le DataFrame dans MySQL
            #INSERT IGNORE : ignore silencieusement les doublons
            # (contrainte unique uq_symbol_date_source)
            # if_exists='append' : ajoute aux données existantes
            # index=False        : ne pas insérer l'index pandas
            # chunksize          : nombre de lignes par batch INSERT
            #                      (évite les requêtes trop larges)
            from sqlalchemy.dialects.mysql import insert as mysql_insert

            def insert_ignore(table, conn, keys, data_iter):
                stmt = mysql_insert(table.table).values(
                    [dict(zip(keys, row)) for row in data_iter]
                    )
                stmt = stmt.prefix_with("IGNORE")
                conn.execute(stmt)




            rows_inserted = df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists="append",
                index=False,
                chunksize=settings.MYSQL_CHUNK_SIZE,
                method=insert_ignore,
            )

            duration  = time.time() - start_time
            n_rows    = rows_inserted if rows_inserted else len(df)

            logger.info(
                f"[MySQL] {table_name} — {n_rows} lignes insérées en {duration:.2f}s"
            )
            metrics.record_mysql_insert(
                table=table_name,
                n_rows=n_rows,
                duration=duration,
                success=True
            )
            return n_rows, True

        except SQLAlchemyError as e:
            duration = time.time() - start_time
            logger.error(f"[MySQL] Erreur insertion {table_name} : {e}")
            
            metrics.record_mysql_insert(
                table=table_name, n_rows=0,
                duration=duration, success=False
                )
            return 0, False
            

    def insert_gold(self, df: pd.DataFrame) -> tuple[int, bool]:
        """
        Insère les données Gold (avec indicateurs) dans la table stock_gold.

        Stratégie : INSERT + UPDATE si le ticker/date existe déjà.
        Utilise INSERT ... ON DUPLICATE KEY UPDATE via une requête SQL custom
        pour mettre à jour les indicateurs sans dupliquer les lignes.

        Dans notre cas, on utilise l'approche plus simple :
        - Supprimer les données existantes pour les (symbol, date) concernés
        - Réinsérer avec les nouvelles valeurs (upsert simplifié)

        Args:
            df : DataFrame Gold avec indicateurs (prêt via prepare_for_mysql())

        Returns:
            Tuple (nb_lignes_insérées, success: bool)
        """
        if df is None or df.empty:
            logger.warning("[MySQL] DataFrame Gold vide: insertion ignorée")
            return 0, True

        table_name = "stock_gold"
        start_time = time.time()

        try:
            # Upsert simplifié
            # 1. Identifier les (symbol, date) à mettre à jour
            # 2. Supprimer ces enregistrements existants
            # 3. Insérer les nouvelles valeurs

            symbols = df["symbol"].unique().tolist()
            dates   = df["date"].unique().tolist()

            # Supprimer les enregistrements existants pour ces tickers/dates
            with self.engine.begin() as conn:   # begin() = transaction auto-commit
                placeholders_sym  = ",".join([f"'{s}'" for s in symbols])
                placeholders_date = ",".join([f"'{str(d)}'" for d in dates])

                delete_sql = text(
                    f"DELETE FROM {table_name} "
                    f"WHERE symbol IN ({placeholders_sym}) "
                    f"AND date IN ({placeholders_date})"
                )
                conn.execute(delete_sql)

            # Insérer les nouvelles valeurs
            rows_inserted = df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists="append",
                index=False,
                chunksize=settings.MYSQL_CHUNK_SIZE,
                method="multi",
            )

            duration  = time.time() - start_time
            n_rows    = rows_inserted if rows_inserted else len(df)

            logger.info(
                f"[MySQL] {table_name} — {n_rows} lignes insérées en {duration:.2f}s"
            )
            metrics.record_mysql_insert(
                table=table_name,
                n_rows=n_rows,
                duration=duration,
                success=True
            )

            # Mettre à jour la métrique de taille de table
            self._update_table_size_metric(table_name)

            return n_rows, True

        except SQLAlchemyError as e:
            duration = time.time() - start_time
            logger.error(f"[MySQL] Erreur insertion {table_name} : {e}")
            metrics.record_mysql_insert(
                table=table_name, n_rows=0,
                duration=duration, success=False
            )
            return 0, False

    def insert_bronze_log(self, ticker: str, source: str, blob_path: str,
                           record_count: int, file_size_bytes: int,
                           status: str = "success",
                           error_message: str = "") -> bool:
        """
        Enregistre un upload Bronze dans la table stock_bronze (journal).

        Args:
            ticker         : symbole boursier
            source         : "alphavantage" ou "twelvedata"
            blob_path      : chemin Azure Blob (bronze/AAPL/2024-01-15.json)
            record_count   : nombre de lignes dans le fichier JSON
            file_size_bytes: taille du fichier uploadé
            status         : "success", "error" ou "partial"
            error_message  : message d'erreur (si status != "success")

        Returns:
            True si l'enregistrement a réussi, False sinon.
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO stock_bronze
                            (symbol, source, blob_path, record_count,
                             file_size_bytes, status, error_message)
                        VALUES
                            (:symbol, :source, :blob_path, :record_count,
                             :file_size_bytes, :status, :error_message)
                    """),
                    {
                        "symbol":           ticker,
                        "source":           source,
                        "blob_path":        blob_path,
                        "record_count":     record_count,
                        "file_size_bytes":  file_size_bytes,
                        "status":           status,
                        "error_message":    error_message or None,
                    }
                )
            return True
        except SQLAlchemyError as e:
            logger.error(f"[MySQL] Erreur log bronze {ticker}: {e}")
            return False

    # ── Gestion des lancements(exécutions)

    def start_run(self, tickers_total: int) -> str:
        """
        Enregistre le début d'un run dans la table pipeline_runs.

        Args:
            tickers_total : nombre de tickers à traiter

        Returns:
            run_id : UUID unique du run (pour le clore avec end_run())
        """
        run_id = str(uuid.uuid4())

        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO pipeline_runs
                            (run_id, started_at, status, tickers_total)
                        VALUES
                            (:run_id, :started_at, 'running', :tickers_total)
                    """),
                    {
                        "run_id":         run_id,
                        "started_at":     datetime.now(),
                        "tickers_total":  tickers_total,
                    }
                )
            logger.info(f"[MySQL] exécution démarré — ID: {run_id}")
        except SQLAlchemyError as e:
            logger.warning(f"[MySQL] Impossible d'enregistrer le début de l'exécution: {e}")

        return run_id

    def end_run(self, run_id: str, status: str, tickers_success: int,
                tickers_failed: int, records_bronze: int, records_silver: int,
                records_gold: int, api_calls: int, api_errors: int,
                quality_score: float, error_message: str = "") -> None:
        """
        Met à jour le run dans pipeline_runs à la fin de l'exécution.

        Args:
            run_id           : UUID du run (retourné par start_run())
            status           : "success", "error" ou "partial"
            tickers_success  : nb tickers traités avec succès
            tickers_failed   : nb tickers en échec
            records_bronze   : nb fichiers JSON uploadés sur Azure
            records_silver   : nb lignes insérées en Silver
            records_gold     : nb lignes insérées en Gold
            api_calls        : nb total d'appels API
            api_errors       : nb d'erreurs API
            quality_score    : score qualité 0-100
            error_message    : message d'erreur global (si status=error)
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE pipeline_runs SET
                            ended_at         = :ended_at,
                            status           = :status,
                            tickers_success  = :tickers_success,
                            tickers_failed   = :tickers_failed,
                            records_bronze   = :records_bronze,
                            records_silver   = :records_silver,
                            records_gold     = :records_gold,
                            api_calls_total  = :api_calls,
                            api_errors       = :api_errors,
                            quality_score    = :quality_score,
                            error_message    = :error_message
                        WHERE run_id = :run_id
                    """),
                    {
                        "run_id":           run_id,
                        "ended_at":         datetime.now(),
                        "status":           status,
                        "tickers_success":  tickers_success,
                        "tickers_failed":   tickers_failed,
                        "records_bronze":   records_bronze,
                        "records_silver":   records_silver,
                        "records_gold":     records_gold,
                        "api_calls":        api_calls,
                        "api_errors":       api_errors,
                        "quality_score":    round(quality_score, 2),
                        "error_message":    error_message or None,
                    }
                )
            logger.info(f"[MySQL] Run {run_id} clôturé statut: {status}")
        except SQLAlchemyError as e:
            logger.warning(f"[MySQL] Impossible de clôturer l'exécution {run_id}: {e}")

    # Métriques

    def _update_table_size_metric(self, table_name: str) -> None:
        """
        Récupère le nombre de lignes d'une table et met à jour Prometheus.

        Args:
            table_name : nom de la table MySQL
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT COUNT(*) FROM {table_name}")
                )
                n_rows = result.scalar()
                metrics.record_mysql_table_size(table=table_name, n_rows=n_rows)
                logger.debug(f"[MySQL] {table_name} : {n_rows} lignes")
        except SQLAlchemyError:
            pass   # Non critique — ne pas bloquer le pipeline

    def update_all_table_sizes(self) -> None:
        """Met à jour les métriques de taille pour toutes les tables."""
        for table in ["stock_bronze", "stock_silver", "stock_gold", "pipeline_runs"]:
            self._update_table_size_metric(table)
