# src/transformation.py — Nettoyage et calcul des indicateurs techniques
#
# Ce module implémente les trois couches de l'architecture Medallion :
#
#   BRONZE vers SILVER : parsing JSON, nettoyage, typage, déduplication
#   SILVER vers GOLD   : fusion des tickers + calcul des indicateurs techniques
#                     (SMA, EMA, RSI, VWAP, Bollinger Bands)
#
# Tout est réalisé en Python / pandas.
# Pandas est suffisant pour des volumes de quelques millions de lignes.


import logging
import warnings
from typing import Optional

import numpy as np
import pandas as pd

from config import settings
from src.monitoring import metrics

# Supprimer les warnings pandas sur les copies de DataFrame (SettingWithCopyWarning)
#warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)
try:
    warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)
except AttributeError:
    pass  # pandas 3.0+ a supprimé ce warning

logger = logging.getLogger(__name__)



# COUCHE BRONZE vers SILVER
# Parsing et nettoyage des JSON bruts


class BronzeToSilver:
    """
    Transforme les données JSON brutes (couche Bronze) en DataFrame
    pandas propre et typé (couche Silver).
    """

    def transform_alphavantage(self, raw_json: dict,
                                ticker: str) -> Optional[pd.DataFrame]:
        """
        Transforme une réponse JSON Alpha Vantage en DataFrame Silver.

        La réponse Alpha Vantage a une structure imbriquée :
        {
            "Time Series (Daily)": {
                "2026-06-17": { "1. open": "...", "2. high": "...", ... },
                "2026-06-16": { ... },
            }
        }

        On la "aplatit" en tableau à 2 dimensions (une ligne = un jour).

        Args:
            raw_json : réponse JSON brute d'Alpha Vantage
            ticker   : symbole boursier (ajouté comme colonne)

        Returns:
            DataFrame Silver propre, ou None si les données sont invalides.
        """
        logger.info(f"[Silver] Transformation Alpha Vantage: {ticker}")

        try:
            #  Étape 1 : Extraire la série temporelle
            # La clé varie selon la fonction utilisée
            ts_key = "Time Series (Daily)"
            if ts_key not in raw_json:
                logger.error(f"Clé '{ts_key}' absente pour {ticker}")
                return None

            time_series = raw_json[ts_key]

            #  Étape 2 : Convertir en DataFrame
            # pd.DataFrame.from_dict avec orient="index" transforme le dict
            # {date: {col: val}} en DataFrame avec les dates comme index
            df = pd.DataFrame.from_dict(time_series, orient="index")

            # Étape 3 : Renommer les colonnes
            # Alpha Vantage préfixe les colonnes avec des numéros ("1. open")
            # On les renomme en noms propres
            rename_map = {
                "1. open":             "open",
                "2. high":             "high",
                "3. low":              "low",
                "4. close":            "close",
                "5. adjusted close":   "adj_close",
                "6. volume":           "volume",
                "7. dividend amount":  "dividend",
                "8. split coefficient":"split_coeff",
            }
            df.rename(columns=rename_map, inplace=True)

            # Étape 4 : Garder uniquement les colonnes OHLCV utiles
            cols_to_keep = ["open", "high", "low", "close", "volume"]
            df = df[[c for c in cols_to_keep if c in df.columns]]

        except Exception as e:
            logger.error(f"Erreur parsing Alpha Vantage pour {ticker}: {e}")
            return None

        # Appliquer le nettoyage commun
        return self._clean_dataframe(df, ticker, source="alphavantage")

    def transform_twelvedata(self, raw_json: dict,
                              ticker: str) -> Optional[pd.DataFrame]:
        """
        Transforme une réponse JSON Twelve Data en DataFrame Silver.

        Structure Twelve Data (liste de dicts) :
        {
            "values": [
                { "datetime": "2026-06-17", "open": "...", ... },
                ...
            ]
        }

        Args:
            raw_json : réponse JSON brute de Twelve Data
            ticker   : symbole boursier

        Returns:
            DataFrame Silver propre, ou None si invalide.
        """
        logger.info(f"[Silver] Transformation Twelve Data: {ticker}")

        try:
            values = raw_json.get("values", [])
            if not values:
                logger.error(f"Pas de 'values' dans la réponse Twelve Data pour {ticker}")
                return None

            # Twelve Data retourne déjà une liste de dicts- conversion directe
            df = pd.DataFrame(values)

            # Renommer "datetime" → "date" pour uniformiser avec Alpha Vantage
            if "datetime" in df.columns:
                df.rename(columns={"datetime": "date"}, inplace=True)
                df.set_index("date", inplace=True)

        except Exception as e:
            logger.error(f"Erreur parsing Twelve Data pour {ticker}: {e}")
            return None

        return self._clean_dataframe(df, ticker, source="twelvedata")

    def _clean_dataframe(self, df: pd.DataFrame, ticker: str,
                          source: str) -> Optional[pd.DataFrame]:
        """
        Nettoyage commun appliqué quelle que soit la source API.

        Étapes :
          1. Conversion des types (tout est str dans les JSON >>> float/int/date)
          2. Réinitialisation de l'index pour avoir "date" comme colonne
          3. Ajout de la colonne "symbol"
          4. Suppression des valeurs manquantes et aberrantes
          5. Déduplication
          6. Tri chronologique
          7. Calcul du score de qualité

        Args:
            df     : DataFrame brut (colonnes: open/high/low/close/volume)
            ticker : symbole boursier
            source : "alphavantage" ou "twelvedata"

        Returns:
            DataFrame propre, ou None si trop peu de données valides.
        """
        try:
            # Étape 1 : Conversion des types
            # Toutes les valeurs arrivent en string depuis le JSON
            numeric_cols = ["open", "high", "low", "close", "volume"]

            for col in numeric_cols:
                if col in df.columns:
                    # errors="coerce" transforme les valeurs non-numériques en NaN
                    # au lieu de lever une exception
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Volume = entier (pas besoin de décimales)
            if "volume" in df.columns:
                df["volume"] = df["volume"].astype("Int64")   # Int64 supporte les NaN

            # Étape 2 : Convertir l'index en colonne "date"
            # L'index est la date sous forme de string "YYYY-MM-DD"
            df.index.name = "date"
            df.reset_index(inplace=True)

            # Conversion string → datetime.date
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

            # Étape 3 : Ajouter les colonnes de métadonnées
            df["symbol"] = ticker.upper()
            df["source"] = source

            # Étape 4 : Supprimer les lignes invalides
            initial_rows = len(df)

            # 4a. Supprimer les lignes où la date est invalide
            df.dropna(subset=["date"], inplace=True)

            # 4b. Supprimer les lignes où close ou volume est manquant
            #     (open/high/low peuvent parfois être manquants, close est critique)
            df.dropna(subset=["close", "volume"], inplace=True)

            # 4c. Supprimer les valeurs aberrantes (prix négatif ou nul)
            df = df[df["close"] > 0]
            df = df[df["open"]  > 0]
            df = df[df["high"]  >= df["close"]]   # High doit être ≥ Close
            df = df[df["low"]   <= df["close"]]   # Low doit être ≤ Close
            df = df[df["volume"] >= 0]            # Volume ne peut pas être négatif

            # Étape 5 : Déduplication
            # Garder la première occurrence si doublon (symbol, date)
            before_dedup = len(df)
            df.drop_duplicates(subset=["symbol", "date"], keep="first", inplace=True)
            duplicates_removed = before_dedup - len(df)
            if duplicates_removed > 0:
                logger.warning(
                    f"[Silver] {ticker}: {duplicates_removed} doublon(s) supprimé(s)"
                )

            # Étape 6 : Tri chronologique
            df.sort_values("date", inplace=True)
            df.reset_index(drop=True, inplace=True)

            # Étape 7 : Vérification du résultat
            final_rows = len(df)
            removed    = initial_rows - final_rows

            if final_rows < settings.MIN_ROWS_THRESHOLD:
                logger.error(
                    f"[Silver] {ticker} : Seulement {final_rows} lignes valides "
                    f"(minimum requis: {settings.MIN_ROWS_THRESHOLD})"
                )
                return None

            # Calcul du score de qualité (proportion de lignes valides)
            quality = final_rows / initial_rows if initial_rows > 0 else 0
            metrics.record_quality_score(quality)

            logger.info(
                f"[Silver] {ticker} — {final_rows} lignes propres "
                f"({removed} supprimées, qualité: {quality:.1%})"
            )

            return df

        except Exception as e:
            logger.error(f"[Silver] Erreur nettoyage {ticker}: {e}", exc_info=True)
            return None



# COUCHE SILVER vers GOLD
# Fusion et calcul des indicateurs techniques


class SilverToGold:
    """
    Transforme les DataFrames Silver individuels en un DataFrame Gold
    enrichi avec tous les indicateurs techniques.
    """

    def merge_tickers(self,
                       silver_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Fusionne les DataFrames Silver de chaque ticker en un seul DataFrame.

        Args:
            silver_frames : dict {ticker: df_silver}

        Returns:
            DataFrame unique avec tous les tickers (colonne "symbol" les distingue).
        """
        if not silver_frames:
            raise ValueError("Aucun DataFrame Silver à fusionner")

        frames = list(silver_frames.values())

        # pd.concat empile verticalement les DataFrames
        # ignore_index=True réinitialise l'index
        df_merged = pd.concat(frames, ignore_index=True)

        # Tri par ticker puis par date (requis pour les calculs de rolling)
        df_merged.sort_values(["symbol", "date"], inplace=True)
        df_merged.reset_index(drop=True, inplace=True)

        n_tickers = df_merged["symbol"].nunique()
        n_rows    = len(df_merged)
        logger.info(
            f"[Gold] Fusion terminée — {n_tickers} tickers, {n_rows} lignes au total"
        )

        return df_merged

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcule tous les indicateurs techniques sur le DataFrame fusionné.

        Les calculs sont effectués par groupe de ticker (groupby "symbol")
        pour ne pas mélanger les séries de différents tickers.

        Indicateurs calculés :
          - SMA 20, SMA 50   : Moyennes mobiles simples
          - EMA 20, EMA 50   : Moyennes mobiles exponentielles
          - RSI 14           : Relative Strength Index (méthode Wilder)
          - VWAP             : Volume Weighted Average Price
          - BB Upper/Lower   : Bandes de Bollinger (20 périodes, 2 écarts-types)
          - Daily Return     : Rendement journalier en pourcentage

        Args:
            df : DataFrame fusionné (Silver avec tous les tickers)

        Returns:
            DataFrame enrichi avec les colonnes d'indicateurs.
        """
        logger.info(f"[Gold] Calcul des indicateurs — {len(df)} lignes")

        # Copie pour éviter de modifier le DataFrame d'entrée
        df = df.copy()

        # Calculer les indicateurs par groupe de ticker
        # apply() est plus lent que transform() mais permet des calculs complexes
        df = df.groupby("symbol", group_keys=True).apply(
            self._compute_ticker_indicators
        ).reset_index(drop=True)

        # Enregistrer les valeurs RSI actuelles dans Prometheus (dernier point par ticker)
        self._publish_rsi_metrics(df.reset_index(drop=True) if "symbol" not in df.columns else df)

        logger.info("[Gold] Indicateurs calculés avec succès")
        return df

    def _compute_ticker_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcule les indicateurs pour UN SEUL ticker.
        Appelé par groupby().apply() pour chaque groupe.

        Args:
            df : DataFrame d'un seul ticker, trié par date

        Returns:
            DataFrame avec les colonnes d'indicateurs ajoutées.
        """
        ticker = df.name if "symbol" not in df.columns else df["symbol"].iloc[0]
        # Réinjecter symbol si absent (pandas 3.0 l'exclut dans groupby)
        if "symbol" not in df.columns:
            df = df.copy()
            df["symbol"] = ticker


        # S'assurer que le DataFrame est trié chronologiquement
        df.sort_values("date", inplace=True)

        close  = df["close"]
        volume = df["volume"].fillna(0).astype(float)

        # Moyennes Mobiles Simples (SMA)
        # SMA = moyenne arithmétique des N derniers cours de clôture
        # min_periods=1 : calculer même avec moins de N points (début de série)
        for window in settings.SMA_WINDOWS:
            col_name = f"sma_{window}"
            df[col_name] = close.rolling(
                window=window,
                min_periods=1,   # Evite les NaN au début de la série
            ).mean().round(4)

        # Moyennes Mobiles Exponentielles (EMA)
        # EMA donne plus de poids aux données récentes qu'aux anciennes
        # span = période de l'EMA (équivalent à N jours)
        # adjust=False : utilise la formule récurrente standard
        for span in settings.EMA_WINDOWS:
            col_name = f"ema_{span}"
            df[col_name] = close.ewm(
                span=span,
                min_periods=1,
                adjust=False,
            ).mean().round(4)

        #  RSI - Relative Strength Index (méthode Wilder)
        # Formule : RSI = 100 - 100 / (1 + RS)
        # où RS = moyenne_gains / moyenne_pertes sur N périodes
        #
        # Méthode Wilder (originale) : utilise une EMA avec alpha = 1/N
        # C'est différent d'une EMA standard — alpha = 2/(N+1)
        df[f"rsi_{settings.RSI_PERIOD}"] = self._compute_rsi(
            close,
            period=settings.RSI_PERIOD,
        )

        # VWAP : Volume Weighted Average Price
        # VWAP = somme(prix_typique * volume) / somme(volume)
        # Prix typique = (high + low + close) / 3
        # VWAP cumulatif depuis le début de la série (usage journalier en général,
        # mais ici on l'utilise sur l'historique complet du ticker)
        typical_price = (df["high"] + df["low"] + df["close"]) / 3
        cumulative_vol = volume.cumsum().replace(0, np.nan)   # Eviter division par 0
        df["vwap"] = (
            (typical_price * volume).cumsum() / cumulative_vol
        ).round(4)

        #  Bandes de Bollinger
        # Bande centrale  = SMA(20)
        # Bande supérieure = SMA(20) + 2 * écart-type(20)
        # Bande inférieure = SMA(20) - 2 * écart-type(20)
        rolling_std = close.rolling(
            window=settings.BBANDS_PERIOD,
            min_periods=1,
        ).std()

        sma_20 = df.get(f"sma_{settings.BBANDS_PERIOD}", close.rolling(20).mean())
        df["bb_upper"] = (sma_20 + settings.BBANDS_STD * rolling_std).round(4)
        df["bb_lower"] = (sma_20 - settings.BBANDS_STD * rolling_std).round(4)

        # Rendement journalier
        # Rendement = (close_aujourd_hui / close_hier) - 1
        # pct_change() calcule exactement ça
        # Multiplié par 100 pour avoir un pourcentage
        df["daily_return"] = close.pct_change().multiply(100).round(4)

        logger.debug(f"[Gold] {ticker} — indicateurs calculés ({len(df)} lignes)")
        return df

    @staticmethod
    def _compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
        """
        Calcule le RSI selon la méthode originale de J. Welles Wilder (1978).

        Étapes :
          1. Calculer les variations journalières (delta)
          2. Séparer gains (delta > 0) et pertes (delta < 0, en valeur absolue)
          3. Calculer la moyenne mobile exponentielle de Wilder sur N périodes
             (EWM avec alpha = 1/N, ce qui est différent de l'EMA classique)
          4. RS = moyenne_gains / moyenne_pertes
          5. RSI = 100 - (100 / (1 + RS))

        Args:
            series : Series pandas des prix de clôture
            period : nombre de périodes (14 par défaut)

        Returns:
            Series RSI avec des valeurs entre 0 et 100.
        """
        # Variations journalières
        delta = series.diff()

        # Gains = variations positives (les pertes → 0)
        # Pertes = valeur absolue des variations négatives (les gains → 0)
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)

        # EMA de Wilder : alpha = 1/period (différent de l'EMA classique 2/(N+1))
        # com = period - 1 car ewm(com=x) utilise alpha = 1/(1+x)
        avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
        avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()

        # Eviter la division par zéro (si avg_loss = 0, RSI = 100)
        rs = avg_gain / avg_loss.replace(0, np.nan)

        rsi = 100 - (100 / (1 + rs))
        rsi = rsi.clip(0, 100)   # Forcer entre 0 et 100 (robustesse)

        return rsi.round(2)

    def _publish_rsi_metrics(self, df: pd.DataFrame) -> None:
        """
        Publie la valeur RSI actuelle de chaque ticker dans Prometheus.
        Utilisée par Grafana pour les alertes surachat/survente.

        Args:
            df : DataFrame Gold complet
        """
        rsi_col = f"rsi_{settings.RSI_PERIOD}"
        if rsi_col not in df.columns:
            return

        # Pour chaque ticker, récupérer la DERNIÈRE valeur RSI valide
        for ticker in df["symbol"].unique():
            ticker_df = df[df["symbol"] == ticker]
            rsi_series = ticker_df[rsi_col].dropna()

            if not rsi_series.empty:
                last_rsi = float(rsi_series.iloc[-1])
                metrics.record_rsi(ticker=ticker, rsi_value=last_rsi)

                # Log si zone extrême
                if last_rsi > settings.RSI_OVERBOUGHT:
                    logger.warning(
                        f"[RSI] {ticker} en SURACHAT - RSI = {last_rsi:.1f} "
                        f"(> {settings.RSI_OVERBOUGHT})"
                    )
                elif last_rsi < settings.RSI_OVERSOLD:
                    logger.warning(
                        f"[RSI] {ticker} en SURVENTE - RSI = {last_rsi:.1f} "
                        f"(< {settings.RSI_OVERSOLD})"
                    )

    def prepare_for_mysql(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prépare le DataFrame Gold pour l'insertion dans MySQL.

        Ajustements nécessaires :
          - Les dates doivent être des objets datetime.date (pas string)
          - Les NaN doivent être None (MySQL NULL)
          - Garder uniquement les colonnes qui existent dans la table MySQL
          - Limiter les décimales pour respecter DECIMAL(12,4) de MySQL

        Args:
            df : DataFrame Gold avec tous les indicateurs

        Returns:
            DataFrame prêt pour df.to_sql()
        """
        df = df.copy()

        # Colonnes attendues dans la table stock_gold de MySQL
        mysql_columns = [
            "symbol", "date", "open", "high", "low", "close", "volume",
            "sma_20", "sma_50", "ema_20", "ema_50",
            "rsi_14", "vwap", "bb_upper", "bb_lower", "daily_return",
        ]

        # Garder uniquement les colonnes qui existent dans le DataFrame
        cols_available = [c for c in mysql_columns if c in df.columns]
        df = df[cols_available]

        # Remplacer les NaN par None (MySQL stocke NULL, pas NaN)
        # NaN est un concept NumPy/pandas, None est compris par SQLAlchemy
        df = df.where(pd.notnull(df), None)

        logger.info(
            f"[Gold] DataFrame prêt pour MySQL: "
            f"{len(df)} lignes, {len(df.columns)} colonnes"
        )
        return df
