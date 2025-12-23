# Sujet de travaux pratiques "Introduction à la data ingénierie"

Le but de ce projet est de créer un pipeline ETL d'ingestion, de transformation et de stockage de données pour mettre en pratique les connaissances acquises lors du cours d'introduction à la data ingénierie. Ce sujet présenté propose d'utiliser les données d'utilisation des bornes de vélos open-sources et "temps réel" dans les grandes villes de France.

Le sujet propose une base qui est un pipeline ETL complet qui couvre la récupération, le stockage et la transformation d'une partie des données de la ville de Paris.

Le but du sujet de travaux pratiques est d'ajouter à ce pipeline des données de consolidation, de dimensions et de faits pour la ville de Paris, ainsi que les données provenant d'autres grandes villes de France. Ces données sont disponibles pour les villes de Nantes, de Toulouse ou encore de Strasbourg. Il faudra aussi enrichir ces données avec les données descriptives des villes de France, via une API de l'État français open-source.

## Explication du code existant

Le projet est découpé en 3 parties :

1. Un fichier python pour récupérer et stocker les données dans des fichiers localement

2. Un fichier python pour consolider les données et faire un premier load dans une base de données type data-warehouse

3. Un fichier python pour agréger les données et créer une modélisation de type dimensionnelle


### Le fichier main.py

Le fichier `main.py` contient le code principal du processus et exécute séquentiellement les différentes fonctions expliquées plus haut.

### Comment faire fonctionner ce projet?

Pour faire fonctionner ce sujet, c'est assez simple:

```bash 
git clone https://github.com/salahzaame/data_ing_project

cd polytech-de-101-2025-tp-subject

python3 -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt

python src/main.py
```

## Comment exécuter le code

Une fois l'environnement virtuel activé et les dépendances installées, vous pouvez exécuter le pipeline ETL complet :

### Exécution du pipeline complet

```bash
python src/main.py
```

Cette commande lance le processus ETL complet dans l'ordre suivant :

1. **Ingestion des données** : Récupération des données en temps réel depuis les APIs open-source
   - Données de Paris (Vélib)
   - Données de Nantes
   - Données de population des communes

2. **Consolidation des données** : Transformation et chargement des données brutes dans DuckDB
   - Création des tables de consolidation
   - Consolidation des données de villes
   - Consolidation des données de population
   - Consolidation des données de stations
   - Consolidation des relevés de stations

3. **Agrégation des données** : Création de la modélisation dimensionnelle
   - Création des tables de dimensions et de faits
   - Agrégation des dimensions (villes, stations)
   - Agrégation de la table de faits (relevés de stations)

### Vérification des résultats

Après l'exécution, vous pouvez vérifier les résultats en interrogeant la base de données DuckDB :

```bash
duckdb data/duckdb/mobility_analysis.duckdb
```


### Structure des données générées

- **Données brutes** : Stockées dans `data/raw_data/YYYY-MM-DD/` sous format JSON
- **Base de données** : `data/duckdb/mobility_analysis.duckdb` contient les tables consolidées et agrégées

![Process final](images/image.png)

Au final, vous devriez être capable de réaliser les requêtes SQL suivantes sur votre base de données DuckDB :

```sql
-- Nb d'emplacements disponibles de vélos dans une ville
SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
FROM DIM_CITY dm INNER JOIN (
    SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
    FROM FACT_STATION_STATEMENT
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
    GROUP BY CITY_ID
) tmp ON dm.ID = tmp.CITY_ID
WHERE lower(dm.NAME) in ('paris', 'nantes', 'vincennes', 'toulouse');

-- Nb de vélos disponibles en moyenne dans chaque station
SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
FROM DIM_STATION ds JOIN (
    SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available
    FROM FACT_STATION_STATEMENT
    GROUP BY station_id
) AS tmp ON ds.id = tmp.station_id;
```

Vous pouvez utiliser la commande `duckdb data/duckdb/mobility_analysis.duckdb` pour ouvrir l'invite de commande DuckDB. 

