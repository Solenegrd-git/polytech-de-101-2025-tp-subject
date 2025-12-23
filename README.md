# TP - Introduction à l'ingénierie de données

## Objectif du projet

Ce projet vise à construire un pipeline ETL complet pour ingérer, transformer et stocker des données. Vous travaillerez avec des données publiques en temps réel sur l'utilisation des systèmes de vélos partagés dans plusieurs grandes villes françaises.

Un pipeline de base est fourni, couvrant la récupération, le stockage et la transformation des données pour Paris. Votre mission consiste à l'enrichir en ajoutant des données de consolidation, des dimensions et des faits pour Paris, puis d'intégrer d'autres villes (Nantes, Toulouse, Strasbourg). Vous enrichirez également ces données avec des informations descriptives sur les communes françaises via une API publique de l'État.

## Architecture du projet

Le projet s'articule autour de trois composants principaux :

1. **Récupération et stockage** : un script Python qui collecte les données et les sauvegarde localement
2. **Consolidation** : un script Python qui nettoie les données et les charge dans une base de données type data warehouse
3. **Agrégation** : un script Python qui crée une modélisation dimensionnelle à partir des données consolidées

Le fichier `main.py` orchestre l'exécution séquentielle de ces trois étapes.

## Installation et configuration

Clonez le dépôt et configurez l'environnement :

```bash 
git clone https://github.com/Solenegrd-git/polytech-de-101-2025-tp-subject.git
cd polytech-de-101-2025-tp-subject
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Lancement du pipeline

Une fois l'environnement activé et les dépendances installées, exécutez :

```bash
python src/main.py
```

Le pipeline s'exécute en trois phases :

**Phase 1 - Ingestion**  
Collecte des données en temps réel depuis les APIs publiques pour Paris (Vélib), Nantes et les données démographiques des communes.

**Phase 2 - Consolidation**  
Transformation et chargement dans DuckDB avec création des tables de consolidation pour les villes, populations, stations et relevés.

**Phase 3 - Agrégation**  
Construction du modèle dimensionnel avec création des tables de dimensions (villes, stations) et de la table de faits (relevés de stations).

## Organisation des données

- **Données brutes** : stockées au format JSON dans `data/raw_data/YYYY-MM-DD/`
- **Base de données** : `data/duckdb/mobility_analysis.duckdb` contient l'ensemble des tables consolidées et agrégées

## Exploitation des résultats

Vous pouvez interroger la base de données DuckDB directement :

```bash
duckdb data/duckdb/mobility_analysis.duckdb
```

Exemples de requêtes possibles :

```sql
-- Nombre total d'emplacements disponibles par ville
SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
FROM DIM_CITY dm INNER JOIN (
    SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
    FROM FACT_STATION_STATEMENT
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
    GROUP BY CITY_ID
) tmp ON dm.ID = tmp.CITY_ID
WHERE lower(dm.NAME) in ('paris', 'nantes', 'vincennes', 'toulouse');

-- Nombre moyen de vélos disponibles par station
SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
FROM DIM_STATION ds JOIN (
    SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available
    FROM FACT_STATION_STATEMENT
    GROUP BY station_id
) AS tmp ON ds.id = tmp.station_id;
```