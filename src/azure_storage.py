
# src/azure_storage.py — Upload et lecture sur Azure Blob Storage
#
# Ce module gère toutes les interactions avec Azure Blob Storage :
#   - Upload des JSON bruts (couche Bronze)
#   - Upload des Parquet nettoyés (couche Silver)
#   - Lecture des fichiers existants (pour le traitement incrémental)
#   - Vérification de l'existence d'un blob (éviter les doublons)
#
# STRUCTURE AZURE BLOB :
#   Container "bronze":  bronze/{ticker}/{YYYY-MM-DD}.json
#   Container "silver": silver/{ticker}/{YYYY-MM}.parquet
#   Container "gold"  : gold/stock_analytics_{YYYY-MM}.parquet
#   Container "logs"  : logs/pipeline_{YYYY-MM-DD}.log
#
# LIBRAIRIE :
#   pip install azure-storage-blob


import io
import json
import logging
import time
from datetime import datetime
from typing import Optional

import pandas as pd
from azure.storage.blob import (
    BlobServiceClient,
    BlobClient,
    ContainerClient,
    ContentSettings,
)
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from config import settings
from src.monitoring import metrics

logger = logging.getLogger(__name__)



# CLIENT AZURE — Initialisation unique (pattern Singleton léger)


def _get_blob_service_client() -> BlobServiceClient:
    """
    Crée et retourne un client Azure Blob Service.

    On crée le client depuis la chaîne de connexion (connection string),
    récupérée depuis la variable d'environnement AZURE_STORAGE_CONNECTION_STRING.

    La connection string contient le nom du compte ET la clé d'accès.
    Format : DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;...

    Returns:
        BlobServiceClient prêt à l'emploi.
    """
    if not settings.AZURE_CONNECTION_STRING:
        raise ValueError(
            "AZURE_STORAGE_CONNECTION_STRING non définie dans .env. "
            "Copier depuis le portail Azure > Compte de stockage > Clés d'accès."
        )

    return BlobServiceClient.from_connection_string(
        settings.AZURE_CONNECTION_STRING
    )



# CLASSE PRINCIPALE


class AzureStorageManager:
    """
    Gestionnaire des opérations Azure Blob Storage pour le pipeline.

    Toutes les méthodes sont auto-documentées avec le chemin Azure cible.
    """

    def __init__(self):
        # Initialisation du client au premier appel (lazy init)
        self._client: Optional[BlobServiceClient] = None

    @property
    def client(self) -> BlobServiceClient:
        """Retourne le client Azure, en l'initialisant si nécessaire."""
        if self._client is None:
            self._client = _get_blob_service_client()
        return self._client

    def _ensure_container_exists(self, container_name: str) -> None:
        """
        Crée le container s'il n'existe pas encore.
        Silencieux si le container existe déjà.

        Args:
            container_name : "bronze", "silver", "gold" ou "logs"
        """
        try:
            self.client.create_container(container_name)
            logger.info(f"Container Azure '{container_name}' créé")
        except ResourceExistsError:
            pass   # Le container existe déjà, c'est normal

    def _upload_bytes(self, container: str, blob_path: str,
                      data: bytes, content_type: str = "application/octet-stream",
                      overwrite: bool = True) -> bool:
        """
        Méthode interne d'upload de bytes vers Azure Blob.

        Args:
            container    : nom du container ("bronze", "silver", etc.)
            blob_path    : chemin du fichier dans le container ("AAPL/2024-01-15.json")
            data         : contenu du fichier en bytes
            content_type : MIME type du fichier
            overwrite    : True = écrase si le fichier existe déjà

        Returns:
            True si l'upload a réussi, False sinon.
        """
        start_time = time.time()

        try:
            # S'assurer que le container existe
            self._ensure_container_exists(container)

            # Obtenir un client pour ce blob spécifique
            blob_client: BlobClient = self.client.get_blob_client(
                container=container,
                blob=blob_path,
            )

            # Configurer les métadonnées du fichier
            content_settings = ContentSettings(content_type=content_type)

            # Upload — overwrite=True remplace le fichier s'il existe
            blob_client.upload_blob(
                data,
                overwrite=overwrite,
                content_settings=content_settings,
            )

            duration = time.time() - start_time
            logger.info(
                f" Azure upload OK : {container}/{blob_path} "
                f"({len(data)/1024:.1f} KB en {duration:.2f}s)"
            )
            metrics.record_azure_upload(
                container=container,
                duration=duration,
                success=True
            )
            return True

        except Exception as e:
            duration = time.time() - start_time
            logger.error(
                f" Azure upload ÉCHOUÉ : {container}/{blob_path} — {e}"
            )
            metrics.record_azure_upload(
                container=container,
                duration=duration,
                success=False
            )
            return False

    #  Couche Bronze

    def upload_bronze_json(self, ticker: str, data_json: dict,
                            date: Optional[str] = None) -> tuple[bool, str]:
        """
        Upload le JSON brut d'un ticker dans la couche Bronze.

        Chemin Azure : bronze/{ticker}/{YYYY-MM-DD}.json
        Le JSON est sérialisé avec indentation pour lisibilité humaine.

        Args:
            ticker    : symbole boursier ("AAPL")
            data_json : dictionnaire Python à sauvegarder en JSON
            date      : date du fichier (défaut = aujourd'hui)

        Returns:
            Tuple (success: bool, blob_path: str)
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        # Construction du chemin selon le template de settings
        blob_path = settings.AZURE_BRONZE_PATH_TEMPLATE.format(
            ticker=ticker,
            date=date,
        )

        # Sérialisation JSON avec indentation (pour lisibilité dans Azure Storage Explorer)
        json_bytes = json.dumps(
            data_json,
            indent=2,
            ensure_ascii=False,   # Permet les caractères non-ASCII (accents, etc.)
        ).encode("utf-8")

        success = self._upload_bytes(
            container=settings.AZURE_CONTAINER_BRONZE,
            blob_path=blob_path,
            data=json_bytes,
            content_type="application/json",
        )

        return success, f"{settings.AZURE_CONTAINER_BRONZE}/{blob_path}"

    # Couche Silver

    def upload_silver_parquet(self, ticker: str,
                               df: pd.DataFrame,
                               year_month: Optional[str] = None) -> tuple[bool, str]:
        """
        Upload un DataFrame pandas nettoyé en format Parquet dans la couche Silver.

        Parquet est le format columnar optimal pour les données tabulaires :
        - Compression ~5x meilleure que CSV
        - Types de données préservés (pas de problème de parsing)
        - Lecture partielle par colonne (column pruning)

        Chemin Azure : silver/{ticker}/{YYYY-MM}.parquet

        Args:
            ticker      : symbole boursier
            df          : DataFrame pandas nettoyé (couche Silver)
            year_month  : mois du fichier format "YYYY-MM" (défaut = mois courant)

        Returns:
            Tuple (success: bool, blob_path: str)
        """
        if year_month is None:
            year_month = datetime.now().strftime("%Y-%m")

        blob_path = settings.AZURE_SILVER_PATH_TEMPLATE.format(
            ticker=ticker,
            year_month=year_month,
        )

        # Sérialisation Parquet en mémoire (pas besoin d'un fichier temporaire)
        # engine="pyarrow" est plus rapide et plus compatible que "fastparquet"
        buffer = io.BytesIO()
        df.to_parquet(
            buffer,
            engine="pyarrow",
            compression="snappy",   # Snappy : bon compromis vitesse/ratio
            index=False,            # Ne pas sauvegarder l'index pandas
        )
        parquet_bytes = buffer.getvalue()

        success = self._upload_bytes(
            container=settings.AZURE_CONTAINER_SILVER,
            blob_path=blob_path,
            data=parquet_bytes,
            content_type="application/octet-stream",
        )

        return success, f"{settings.AZURE_CONTAINER_SILVER}/{blob_path}"

    # Couche Gold

    def upload_gold_parquet(self, df: pd.DataFrame,
                             year_month: Optional[str] = None) -> tuple[bool, str]:
        """
        Upload le DataFrame Gold (tous tickers + indicateurs) en Parquet.

        La couche Gold contient TOUS les tickers fusionnés.
        Un seul fichier par mois regroupe toutes les données enrichies.

        Chemin Azure : gold/stock_analytics_{YYYY-MM}.parquet

        Args:
            df         : DataFrame pandas enrichi (tous tickers + indicateurs)
            year_month : mois format "YYYY-MM" (défaut = mois courant)

        Returns:
            Tuple (success: bool, blob_path: str)
        """
        if year_month is None:
            year_month = datetime.now().strftime("%Y-%m")

        blob_path = settings.AZURE_GOLD_PATH_TEMPLATE.format(
            year_month=year_month
        )

        buffer = io.BytesIO()
        df.to_parquet(
            buffer,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )
        parquet_bytes = buffer.getvalue()

        success = self._upload_bytes(
            container=settings.AZURE_CONTAINER_GOLD,
            blob_path=blob_path,
            data=parquet_bytes,
        )

        return success, f"{settings.AZURE_CONTAINER_GOLD}/{blob_path}"

    #  Lecture

    def read_silver_parquet(self, ticker: str,
                             year_month: str) -> Optional[pd.DataFrame]:
        """
        Lit un fichier Parquet Silver depuis Azure Blob et le retourne en DataFrame.

        Utile pour le traitement incrémental : récupérer les données Silver
        d'un mois précédent pour recalculer les indicateurs Gold.

        Args:
            ticker     : symbole boursier
            year_month : mois format "YYYY-MM"

        Returns:
            DataFrame pandas, ou None si le fichier n'existe pas.
        """
        blob_path = settings.AZURE_SILVER_PATH_TEMPLATE.format(
            ticker=ticker,
            year_month=year_month,
        )

        try:
            blob_client = self.client.get_blob_client(
                container=settings.AZURE_CONTAINER_SILVER,
                blob=blob_path,
            )

            # Téléchargement en mémoire
            download_stream = blob_client.download_blob()
            parquet_bytes = download_stream.readall()

            # Désérialisation Parquet → DataFrame
            df = pd.read_parquet(io.BytesIO(parquet_bytes), engine="pyarrow")
            logger.info(
                f"Azure read OK : silver/{blob_path} "
                f"({len(df)} lignes)"
            )
            return df

        except ResourceNotFoundError:
            logger.warning(f"Fichier non trouvé sur Azure : silver/{blob_path}")
            return None
        except Exception as e:
            logger.error(f"Erreur lecture Azure : silver/{blob_path} — {e}")
            return None

    def blob_exists(self, container: str, blob_path: str) -> bool:
        """
        Vérifie si un blob existe dans Azure (pour le traitement incrémental).

        Args:
            container : nom du container
            blob_path : chemin du blob

        Returns:
            True si le blob existe, False sinon.
        """
        try:
            blob_client = self.client.get_blob_client(
                container=container,
                blob=blob_path,
            )
            blob_client.get_blob_properties()   # Lève une exception si absent
            return True
        except ResourceNotFoundError:
            return False

    def initialize_containers(self) -> None:
        """
        Crée tous les containers nécessaires s'ils n'existent pas.
        À appeler une seule fois au démarrage du pipeline.
        """
        containers = [
            settings.AZURE_CONTAINER_BRONZE,
            settings.AZURE_CONTAINER_SILVER,
            settings.AZURE_CONTAINER_GOLD,
            settings.AZURE_CONTAINER_LOGS,
        ]
        for container in containers:
            self._ensure_container_exists(container)
        logger.info("Containers Azure initialisés")
