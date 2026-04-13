# Carburants DBT - Prix des Carburants France

[![dbt CI](https://github.com/YOUR_USERNAME/carburants_dbt/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/carburants_dbt/actions/workflows/ci.yml)
[![Data Freshness](https://github.com/YOUR_USERNAME/carburants_dbt/actions/workflows/ingest.yml/badge.svg)](https://github.com/YOUR_USERNAME/carburants_dbt/actions/workflows/ingest.yml)

Projet dbt professionnel pour l'analyse des prix des carburants en France, utilisant les données ouvertes de [data.gouv.fr](https://www.data.gouv.fr/).

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Source    │────▶│   Staging   │────▶│Intermediate │────▶│    Marts    │
│  (RAW API)  │     │  (cleaned)  │     │  (enriched) │     │ (analytics) │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                                                    │
                                                                    ▼
                                                            ┌─────────────┐
                                                            │  Reports    │
                                                            │ (dashboard) │
                                                            └─────────────┘
```

### Couches de données

| Couche | Description | Matérialisation |
|--------|-------------|-----------------|
| **Raw** | Données brutes de l'API | Tables (via Python) |
| **Staging** | Nettoyage et typage | Views |
| **Intermediate** | Enrichissement et agrégation | Ephemeral |
| **Marts/Core** | Schéma en étoile dimensionnel | Tables |
| **Marts/Analytics** | Rapports et KPIs | Tables |
| **Snapshots** | Historisation SCD Type 2 | Tables |

## Modèle de données

### Schéma en étoile

```
                    ┌───────────────┐
                    │  dim_regions  │
                    └───────┬───────┘
                            │
┌───────────────┐   ┌───────┴───────┐   ┌───────────────┐
│ dim_carburants│───│fct_prix_carbu │───│  dim_stations │
└───────────────┘   └───────┬───────┘   └───────────────┘
                            │
                    ┌───────┴───────┐
                    │   dim_date    │
                    └───────────────┘
```

## Démarrage rapide

### Prérequis

- Python 3.10+
- dbt-core 1.7+
- Compte Snowflake (trial gratuit disponible)

### Installation

```bash
# Cloner le repository
git clone https://github.com/YOUR_USERNAME/carburants_dbt.git
cd carburants_dbt

# Installer dbt
pip install dbt-snowflake

# Copier et configurer le profil
cp profiles.yml.example ~/.dbt/profiles.yml
# Éditer ~/.dbt/profiles.yml avec vos credentials Snowflake

# Installer les packages dbt
dbt deps

# Vérifier la connexion
dbt debug
```

### Première exécution

```bash
# Charger les données de référence
dbt seed

# Exécuter tous les modèles et tests
dbt build

# Générer la documentation
dbt docs generate
dbt docs serve
```

## Ingestion des données

L'ingestion est automatisée via GitHub Actions (chaque jour) ou peut être lancée manuellement :

```bash
# Installer les dépendances Python
pip install -r scripts/requirements.txt

# Configurer les variables d'environnement
export SNOWFLAKE_ACCOUNT=xxx
export SNOWFLAKE_USER=xxx
export SNOWFLAKE_PASSWORD=xxx

# Lancer l'ingestion
python scripts/ingest_carburants.py
```

## Tests

Le projet inclut plusieurs niveaux de tests :

```bash
# Tests génériques (unique, not_null, relationships)
dbt test --select test_type:generic

# Tests singuliers (freshness, orphans, bounds)
dbt test --select test_type:singular

# Tests dbt_expectations (ranges, patterns)
dbt test --select tag:dbt_expectations
```

### Tests inclus

- **Freshness** : Vérification que les données sont récentes (< 24h)
- **Orphan records** : Pas de prix sans station correspondante
- **Geographic bounds** : Coordonnées dans les limites de la France
- **Price ranges** : Prix dans des plages réalistes (0.5€ - 5€)
- **Referential integrity** : Relations entre dimensions et faits

## CI/CD

### Pull Request (CI)

1. **SQLFluff** : Linting du SQL
2. **dbt build** : Slim CI sur les modèles modifiés
3. **Tests** : Exécution des tests
4. **Cleanup** : Suppression du schéma CI

### Merge to Main (CD)

1. **dbt build** : Build complet en production
2. **Snapshots** : Mise à jour de l'historique
3. **Elementary** : Rapport d'observabilité
4. **Documentation** : Déploiement sur GitHub Pages

## Observabilité

Le projet utilise [Elementary](https://www.elementary-data.com/) pour :

- Monitoring de la fraîcheur des données
- Détection des anomalies
- Alertes sur les échecs de tests
- Dashboard de santé du pipeline

## Structure du projet

```
carburants_dbt/
├── .github/workflows/      # CI/CD pipelines
├── models/
│   ├── staging/           # Nettoyage données brutes
│   ├── intermediate/      # Logique métier
│   └── marts/
│       ├── core/          # Schéma en étoile
│       └── analytics/     # Rapports
├── snapshots/             # SCD Type 2
├── macros/                # Macros réutilisables
├── tests/singular/        # Tests personnalisés
├── data/                  # Seeds (données de référence)
└── scripts/               # Ingestion Python
```

## Packages utilisés

| Package | Usage |
|---------|-------|
| dbt_utils | Surrogate keys, date spine |
| dbt_expectations | Tests avancés |
| dbt_date | Dimension date |
| elementary | Observabilité |
| codegen | Génération de code |

## Bonnes pratiques implémentées

- ✅ Architecture 3 couches (staging, intermediate, marts)
- ✅ Schéma en étoile dimensionnel
- ✅ Snapshots SCD Type 2 pour l'historisation
- ✅ Slim CI avec `state:modified+`
- ✅ Tests dbt_expectations avancés
- ✅ Pre-commit hooks (SQLFluff, dbt-checkpoint)
- ✅ Documentation auto-générée
- ✅ Observabilité avec Elementary

## Ressources

- [Documentation dbt](https://docs.getdbt.com/)
- [API Prix Carburants](https://www.prix-carburants.gouv.fr/rubrique/opendata/)
- [Snowflake Trial](https://signup.snowflake.com/)

## Licence

MIT
