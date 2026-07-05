-- init-sql/init_stock_db.sql — Initialisation de la base de données MySQL
--
-- EMPLACEMENT : ./init-sql/init_stock_db.sql
--
-- Ce script est exécuté automatiquement par MySQL au PREMIER démarrage
-- du container (dossier monté dans /docker-entrypoint-initdb.d/).
--
-- Il crée toutes les tables nécessaires à l'architecture Medallion.
-- Le script Python (pandas df.to_sql) utilise if_exists='append', donc
-- les tables doivent exister en amont avec les bons index.


-- S'assurer qu'on travaille sur la bonne base
USE stockdb;


-- TABLE 1 : stock_bronze
-- Couche Bronze: Métadonnées des fichiers JSON bruts uploadés sur Azure Blob
-- (le JSON lui-même est stocké sur Azure, pas dans MySQL)

CREATE TABLE IF NOT EXISTS stock_bronze (
    id              BIGINT        AUTO_INCREMENT PRIMARY KEY,
    symbol          VARCHAR(10)   NOT NULL COMMENT 'Ticker boursier (ex: AAPL)',
    source          VARCHAR(50)   NOT NULL COMMENT 'alphavantage | twelvedata',
    blob_path       VARCHAR(500)  NOT NULL COMMENT 'Chemin Azure Blob (bronze/AAPL/2024-01-15.json)',
    record_count    INT           DEFAULT 0 COMMENT 'Nombre de lignes dans le fichier',
    file_size_bytes BIGINT        DEFAULT 0 COMMENT 'Taille du fichier JSON en octets',
    ingested_at     DATETIME      DEFAULT CURRENT_TIMESTAMP COMMENT 'Horodatage ingestion',
    status          ENUM('success','error','partial') DEFAULT 'success',
    error_message   TEXT          NULL COMMENT 'Message d erreur si status=error',

    INDEX idx_symbol         (symbol),
    INDEX idx_source         (source),
    INDEX idx_ingested_at    (ingested_at),
    INDEX idx_symbol_source  (symbol, source)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COMMENT='Couche Bronze : registre des fichiers JSON bruts sur Azure Blob';



-- TABLE 2 : stock_silver
-- Couche Silver: Données nettoyées et typées (une ligne = un jour = un ticker)

CREATE TABLE IF NOT EXISTS stock_silver (
    id              BIGINT          AUTO_INCREMENT PRIMARY KEY,
    symbol          VARCHAR(10)     NOT NULL    COMMENT 'Ticker boursier',
    date            DATE            NOT NULL    COMMENT 'Date de la séance',
    open            DECIMAL(12, 4)  NULL        COMMENT 'Prix d ouverture',
    high            DECIMAL(12, 4)  NULL        COMMENT 'Plus haut de la séance',
    low             DECIMAL(12, 4)  NULL        COMMENT 'Plus bas de la séance',
    close           DECIMAL(12, 4)  NULL        COMMENT 'Prix de clôture',
    volume          BIGINT          NULL        COMMENT 'Volume échangé',
    source          VARCHAR(50)     NOT NULL    COMMENT 'alphavantage | twelvedata',
    processed_at    DATETIME        DEFAULT CURRENT_TIMESTAMP,

    -- Contrainte d'unicité : un seul enregistrement par ticker par jour par source
    UNIQUE KEY uq_symbol_date_source (symbol, date, source),

    INDEX idx_silver_symbol      (symbol),
    INDEX idx_silver_date        (date),
    INDEX idx_silver_symbol_date (symbol, date)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COMMENT='Couche Silver : données OHLCV nettoyées et typées';



-- TABLE 3 : stock_gold (principale- lue par Grafana)
-- Couche Gold: Données enrichies avec indicateurs techniques calculés
-- C est cette table qui alimente les dashboards Grafana.

CREATE TABLE IF NOT EXISTS stock_gold (
    id              BIGINT          AUTO_INCREMENT PRIMARY KEY,
    symbol          VARCHAR(10)     NOT NULL    COMMENT 'Ticker boursier',
    date            DATE            NOT NULL    COMMENT 'Date de la séance',

    -- OHLCV
    open            DECIMAL(12, 4)  NULL,
    high            DECIMAL(12, 4)  NULL,
    low             DECIMAL(12, 4)  NULL,
    close           DECIMAL(12, 4)  NULL        COMMENT 'Prix de clôture ajusté',
    volume          BIGINT          NULL,

    -- Moyennes mobiles
    sma_20          DECIMAL(12, 4)  NULL        COMMENT 'Moyenne mobile simple 20 jours',
    sma_50          DECIMAL(12, 4)  NULL        COMMENT 'Moyenne mobile simple 50 jours',
    ema_20          DECIMAL(12, 4)  NULL        COMMENT 'Moyenne mobile exponentielle 20 jours',
    ema_50          DECIMAL(12, 4)  NULL        COMMENT 'Moyenne mobile exponentielle 50 jours',

    -- Oscillateurs
    rsi_14          DECIMAL(6, 2)   NULL        COMMENT 'RSI 14 périodes (0-100)',
    vwap            DECIMAL(12, 4)  NULL        COMMENT 'Volume Weighted Average Price',

    -- Bandes de Bollinger
    bb_upper        DECIMAL(12, 4)  NULL        COMMENT 'Bande supérieure Bollinger (SMA20 + 2*std)',
    bb_lower        DECIMAL(12, 4)  NULL        COMMENT 'Bande inférieure Bollinger (SMA20 - 2*std)',

    -- Rendements
    daily_return    DECIMAL(8, 4)   NULL        COMMENT 'Rendement journalier en % (close/close_prev - 1)',

    -- Méta
    processed_at    DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT 'Horodatage du calcul',

    -- Contrainte d'unicité
    UNIQUE KEY uq_gold_symbol_date (symbol, date),

    -- Index pour Grafana (filtres les plus fréquents)
    INDEX idx_gold_symbol           (symbol),
    INDEX idx_gold_date             (date),
    INDEX idx_gold_symbol_date      (symbol, date),
    INDEX idx_gold_rsi              (rsi_14)        COMMENT 'Pour les alertes RSI'
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COMMENT='Couche Gold : données OHLCV + indicateurs techniques (source Grafana)';



-- TABLE 4 : pipeline_runs
-- Journal d'exécution du pipeline alimenté par le script Python
-- Permet de suivre chaque exécution, son statut et ses métriques.

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              BIGINT      AUTO_INCREMENT PRIMARY KEY,
    run_id          VARCHAR(50) NOT NULL    COMMENT 'UUID unique du run (uuid4)',
    started_at      DATETIME    NOT NULL    COMMENT 'Début du run',
    ended_at        DATETIME    NULL        COMMENT 'Fin du run (NULL si en cours)',
    status          ENUM('running','success','error','partial') DEFAULT 'running',

    -- Métriques du run
    tickers_total   INT         DEFAULT 0   COMMENT 'Nb tickers à traiter',
    tickers_success INT         DEFAULT 0   COMMENT 'Nb tickers traités avec succès',
    records_bronze  INT         DEFAULT 0   COMMENT 'Nb fichiers JSON uploadés sur Azure',
    records_silver  INT         DEFAULT 0   COMMENT 'Nb lignes insérées en Silver',
    records_gold    INT         DEFAULT 0   COMMENT 'Nb lignes insérées en Gold',
    api_calls_total INT         DEFAULT 0   COMMENT 'Nb total d appels API',
    api_errors      INT         DEFAULT 0   COMMENT 'Nb d erreurs API',
    quality_score   DECIMAL(5,2) NULL       COMMENT 'Score qualité (0-100)',

    error_message   TEXT        NULL        COMMENT 'Détail erreur si status=error',

    INDEX idx_runs_started  (started_at),
    INDEX idx_runs_status   (status)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COMMENT='Journal des exécutions du pipeline Python';



-- DONNÉES DE TEST (optionnel- commenter pour désactiver)
-- Quelques lignes pour vérifier que Grafana peut lire les données dès le démarrage

INSERT IGNORE INTO stock_gold (symbol, date, open, high, low, close, volume, sma_20, rsi_14)
VALUES
    ('AAPL', '2026-06-17', 185.00, 186.50, 184.20, 185.92, 52000000, 184.50, 58.3),
    ('AAPL', '2026-06-18', 185.92, 187.00, 185.10, 186.50, 48000000, 184.80, 61.2),
    ('MSFT', '2026-06-19', 375.00, 378.00, 374.50, 376.00, 21000000, 374.00, 55.8),
    ('MSFT', '2026-06-20', 376.00, 379.50, 375.80, 378.90, 19000000, 374.50, 59.1);

-- Confirmation
SELECT 'Base de données initialisée avec succès.' AS message;
SELECT table_name, table_comment
FROM information_schema.tables
WHERE table_schema = 'stockdb'
ORDER BY table_name;

