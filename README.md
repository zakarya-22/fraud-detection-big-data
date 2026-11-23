# Détection de fraude bancaire — PySpark / Spark ML sous Docker

Classification de transactions frauduleuses (dataset PaySim) avec un Pipeline Spark ML,
exécutée sur un cluster Spark simulé par Docker (1 master + 3 workers) sur une seule machine.
Les données de départ sont transformées en **fichiers Parquet bruts avec des problèmes de qualité**,
qui sont ensuite diagnostiqués et corrigés avec Spark.

## Données

- Page officielle (Kaggle, compte gratuit nécessaire) : https://www.kaggle.com/datasets/ealaxi/paysim1
  → bouton **Download**, puis dézipper.
- Copie sans compte (Hugging Face) : https://huggingface.co/datasets/vitaliy-sharandin/synthetic-fraud-detection/tree/main
  → cliquer sur le fichier `.csv`, puis sur l'icône de téléchargement.
- Fichier à placer dans `data/source/` : `PS_20174392719_1491204439457_log.csv` (≈ 494 Mo)
- ≈ 6,3 M de transactions, 11 colonnes, 8 213 fraudes (≈ 0,13 %), uniquement dans `TRANSFER` et `CASH_OUT`
- Licence CC BY-SA 4.0. Référence : Lopez-Rojas, Elmir, Axelsson, *PaySim: A financial mobile money simulator for fraud detection*, EMSS 2016

## Organisation du dossier

```
projet-fraude-spark/
├── Dockerfile               image my-spark:4.2.0 (Spark 4.2.0, Java 21, Python 3, JupyterLab)
├── start-spark.sh           démarre master / worker / history selon SPARK_ROLE
├── docker-compose.yml       1 master + Jupyter, 3 workers, 1 history server
├── apps/                    -> /opt/spark-apps (notebooks, à lancer dans l'ordre)
│   ├── 00_creation_donnees_brutes.ipynb   CSV -> Parquet par jour, avec 11 problèmes de qualité
│   ├── 01_nettoyage.ipynb                 diagnostic et correction de chaque problème
│   ├── 02_exploration_preparation.ipynb   exploration Spark SQL, séparation 80 / 20
│   ├── 03_entrainement.ipynb              pipeline, comparaison des modèles, CrossValidator, seuil
│   └── 04_test_final.ipynb                évaluation finale sur le jeu de test
├── data/                    -> /opt/spark-data
│   ├── source/              CSV PaySim téléchargé
│   ├── brut/                Parquet bruts, un dossier par jour (date=2024-01-01, ...)
│   ├── propre/              Parquet nettoyé
│   ├── ml/                  train.parquet (80 %), test.parquet (20 %)
│   └── modeles/             modèle sauvegardé + seuil.txt
└── spark-logs/              -> /tmp/spark-events (History Server)
```

## Problèmes introduits (notebook 00) et corrections (notebook 01)

| # | Problème | Correction |
|---|---|---|
| 1 | Deux formats de date (`yyyy-MM-dd HH:mm:ss` et `dd/MM/yyyy HH:mm`) | `try_to_timestamp` avec les deux formats + `coalesce` |
| 2 | Code d'erreur 9999999999 dans le montant | remplacé par une valeur manquante |
| 3 | Montants négatifs | valeur absolue |
| 4 | Montant en texte, parfois avec une virgule | `regexp_replace` + `try_cast` en double |
| 5 | Montants manquants | `Imputer` (médiane) dans le pipeline |
| 6 | Libellés de type incohérents (`transfer`, ` TRANSFER `, `TRF`, `CASHOUT`) | `upper`, `trim`, remplacement des abréviations |
| 7 | Type manquant | lignes supprimées |
| 8 | Soldes manquants | `Imputer` (médiane) dans le pipeline |
| 9 | Label en texte : `0`/`1` ou `yes`/`no` | conversion en 0.0 / 1.0 |
| 10 | Label manquant | lignes supprimées |
| 11 | Lignes en double | `dropDuplicates` |

L'`Imputer` est dans le pipeline (et pas dans le nettoyage) pour que la médiane soit calculée
uniquement sur les données d'entraînement.

## Ressources (12 cœurs au maximum)

| Conteneur | Cœurs | Mémoire |
|---|---|---|
| spark-master (+ Jupyter, driver) | 2,5 | driver 2 Go |
| spark-worker-a / b / c | 3 chacun | executor 2 Go (limite conteneur 3 Go) |
| spark-history | 0,5 | — |

Docker doit disposer d'environ 14 Go de RAM. Sous Windows, cette limite se règle dans
`C:\Users\<vous>\.wslconfig` (`[wsl2]` puis `memory=14GB`). Si la machine n'en a pas assez, passer
`SPARK_WORKER_MEMORY` et `SPARK_EXECUTOR_MEMORY` à `1500M` dans le compose et dans les notebooks.

## Lancement

```bash
docker compose up -d --build
```

| Interface | Adresse |
|---|---|
| JupyterLab | http://localhost:8888 |
| Spark Master | http://localhost:9080 |
| Application en cours (jobs, stages) | http://localhost:4040 |
| Workers A / B / C | http://localhost:9082, 9083, 9084 |
| History Server | http://localhost:18080 |

Exécuter les notebooks dans l'ordre : 00 → 01 → 02 → 03 → 04 (le 00 une seule fois).

Arrêt : `docker compose down`

## Choix techniques

- **Séparation des données** : 80 % entraînement / 20 % test, écrits dans deux fichiers séparés.
  La validation est faite à l'intérieur de l'entraînement : 80/20 interne pour comparer les modèles
  et choisir le seuil, et validation croisée à 3 plis pour les hyperparamètres.
  Le test n'est utilisé que dans le notebook 04.
- **Pipeline** (étages Spark uniquement) : `Imputer` → `SQLTransformer` → `StringIndexer` → `OneHotEncoder`
  → `VectorAssembler` → `StandardScaler` → modèle.
- **Modèles** : régression logistique, Random Forest, GBT.
- **Déséquilibre** : poids de classe dans `weightCol`, calculés sur le jeu d'entraînement.
- **Métriques** : PR-AUC, puis précision, rappel et F1 sur la classe fraude.
- **Mise en garde** : les soldes de PaySim contiennent des artefacts du simulateur (l'auteur déconseille
  de les utiliser). Les modèles sont donc comparés avec et sans ces colonnes.
