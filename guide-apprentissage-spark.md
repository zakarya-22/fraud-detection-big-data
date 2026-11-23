# Guide d’appropriation du projet Spark

## Ton objectif

Tu maîtrises déjà le nettoyage et le machine learning en Python. Le fil conducteur de ton apprentissage est donc : **comment les données et le calcul sont répartis, exécutés, échangés et observés par Spark ?** Docker sert à rendre visibles les rôles des processus et à démarrer un petit environnement reproductible.

## Ce que le projet fait réellement

Le projet prépare des Parquet, les relit avec Spark, construit des DataFrames et lance des transformations/actions distribuables. Spark découpe le travail en tâches sur des partitions, puis planifie ces tâches sur les workers.

Compose démarre un master Spark, trois workers, un History Server et Jupyter. Le driver Spark est créé par les notebooks dans le conteneur `spark-master` : le master Spark et le driver sont deux rôles différents, même s’ils partagent ici le même conteneur. Le master attribue les ressources et coordonne l’application ; le driver construit le plan d’exécution et orchestre les jobs ; les executors exécutent les tâches sur les workers.

Cette installation est un **cluster local simulé par plusieurs conteneurs sur un seul ordinateur**. Les dossiers `data/` sont montés dans les conteneurs et restent sur le disque de cette machine. Ce n’est donc pas un stockage distribué HDFS, ni un cluster réparti sur plusieurs machines. C’est un bon laboratoire pour apprendre le modèle d’exécution Spark et les limites d’une configuration locale.

## Carte des sujets et des compétences

| Sujet | Déjà acquis / présent dans le projet | À apprendre et à savoir expliquer |
|---|---|---|
| Nettoyage et qualité | Types incohérents, nulls, doublons, dates, valeurs aberrantes dans `01_nettoyage.ipynb` | Peu prioritaire. Comprendre surtout quelles opérations sont distribuées et lesquelles déclenchent un calcul. |
| Machine learning | Pipeline, imputation, modèles, métriques, déséquilibre, validation | Peu prioritaire. Savoir que les étapes Spark ML traitent des colonnes distribuées ; expliquer le coût des validations répétées. |
| Modèle de données Spark | DataFrames, SQL, Parquet | Comprendre schéma, partitions physiques, partitions de calcul, transformations et actions. Ces deux sens du mot « partition » ne sont pas identiques. |
| Exécution distribuée | `SparkSession` pointe vers `spark://spark-master:7077` | Driver, master, worker, executor, job, stage, task ; parallélisme ; sérialisation et transfert réseau. Suivre un `count()` dans l’UI Spark. |
| Optimisation | `cache()`, `shuffle.partitions=18`, agrégations et écritures | Évaluation paresseuse, DAG, narrow/wide transformations, shuffle, skew, cache utile ou inutile, partitionnement et petits fichiers. Lire le plan avec `explain()`. |
| Docker | `Dockerfile`, `docker-compose.yml`, volumes, ports, réseau bridge | Image vs conteneur, rôle de chaque service, DNS par nom de service, ports hôte/conteneur, ressources CPU/RAM, démarrage et journaux. |
| Stockage | Parquet, chemins partagés, partitionnement de sortie par `date` dans le notebook 00 | Différence entre stockage partagé local et stockage distribué ; formats colonnes, compression, partitionnement de fichiers, lecture sélective. |
| Exploitation | Web UI master, UI applicative, History Server, event logs | Diagnostiquer lenteur, tâche échouée, executor indisponible, mémoire insuffisante ; distinguer logs de conteneurs et événements Spark. |
| Industrialisation | Notebooks ordonnés et sorties persistées | Reproductibilité, paramètres, scripts `spark-submit`, idempotence, orchestration, tests de données et déploiement. À aborder après les fondamentaux. |

## Parcours conseillé

### 1. Docker et réseau : comprendre le terrain

À partir de `docker-compose.yml`, explique chaque service, son rôle, ses ressources, ses ports et ses volumes. Dans `start-spark.sh`, suis les branches `master`, `worker` et `history`. Repère que les workers attendent le master via `spark-master:7077`, nom résolu sur le réseau Docker.

**À retenir :** un conteneur est un processus isolé avec son environnement ; il n’est pas une machine physique. Les limites CPU/RAM du compose contraignent le processus, tandis que les paramètres Spark déclarent les ressources que Spark peut utiliser. Le système hôte et Docker partagent ici les mêmes ressources physiques.

### 2. Architecture Spark : suivre une action de bout en bout

Prends `brut.count()` dans le notebook 01. Le driver reçoit l’action, demande à Spark de construire/exécuter le plan, le plan est découpé en stages et tâches, puis les executors lisent des partitions et renvoient les résultats. Le master gère l’allocation des ressources dans le mode Standalone ; il ne remplace pas le driver et ne réalise pas lui-même le calcul des tâches.

**Vocabulaire à savoir distinguer :** application, driver, master, worker, executor, job, stage, task, partition. Un worker est un processus/service hôte pour les executors ; un executor est un processus de calcul associé à une application.

### 3. Modèle Spark : prédire quand le calcul part

Les transformations (`select`, `filter`, `withColumn`, `groupBy`) construisent un plan de façon paresseuse. Les actions (`count`, `show`, `collect`, `write`) déclenchent du travail. Regroupe plusieurs transformations avant une action et observe les jobs dans l’UI. Utilise `df.explain("formatted")` pour relier le code au plan logique/physique.

Une transformation dite *narrow* peut généralement lire les partitions parentes sans redistribuer les données (par exemple un filtre). Une transformation *wide* nécessite souvent un échange entre partitions (*shuffle*), par exemple un `groupBy` ou certaines jointures. Le shuffle utilise réseau, disque et mémoire, et constitue souvent un coût important.

### 4. Partitionnement, Parquet et performance

Dans le notebook 00, `partitionBy("date")` crée des répertoires de données par date lors de l’écriture. Ce partitionnement de stockage permet l’élagage de partitions si les requêtes filtrent sur `date`. Il ne faut pas le confondre avec les partitions de calcul Spark, unités de travail distribuées en mémoire/disque.

Observe le nombre de fichiers et de partitions avant/après les écritures. Demande-toi si un fichier par date est adapté, si une partition est trop petite ou trop grande, et si une requête peut éviter de lire certaines dates. `spark.sql.shuffle.partitions=18` concerne principalement les partitions après shuffle SQL ; ce n’est pas le nombre de workers ni un réglage universel.

### 5. ML distribué et coût des actions

Le pipeline Spark ML exprime les transformations des colonnes dans le moteur distribué. Les appels répétés à `count()` dans les boucles de seuil du notebook 03 peuvent relancer des jobs pour chaque filtre ; c’est un bon exercice pour compter les jobs et remplacer cette approche par des agrégations conditionnelles en un seul passage. Le `toPandas()` du notebook 02 est acceptable pour le petit résultat agrégé par heure, mais pas pour rapatrier les millions de transactions.

La validation croisée est coûteuse : chaque pli et chaque jeu de paramètres réentraîne le pipeline. `parallelism=3` autorise des évaluations concurrentes dans CrossValidator, mais ne signifie pas strictement « un modèle par worker » ; l’exécution dépend des ressources disponibles et du nombre de tâches.

### 6. Observabilité et diagnostic

Pour chaque action intéressante, ouvre l’UI de l’application (port 4040) : jobs, stages, tâches, durée, shuffle, stockage persistant et executors. L’UI du master montre les workers et ressources annoncées. Le History Server relit les event logs après la fin de l’application. Utilise aussi `docker compose ps`, `docker compose logs <service>` et les journaux Spark pour relier les symptômes au bon niveau.

## Exercices pratiques, dans cet ordre

1. **Cartographier sans coder :** annoter chaque conteneur, processus, port, volume et rôle Spark du compose.
2. **Prédire puis observer :** pour `filter`, `withColumn`, `groupBy`, `count` et `write`, dire si c’est transformation/action, anticiper les stages et vérifier dans l’UI.
3. **Comparer un shuffle :** exécuter une agrégation, lire `explain("formatted")`, repérer les échanges et observer la durée dans l’UI.
4. **Faire varier les partitions :** modifier temporairement `spark.sql.shuffle.partitions` (par exemple 6, 18, 36), comparer nombre de tâches, durée et volume de données. Conserver le même jeu de données et noter les limites du test sur un seul ordinateur.
5. **Comprendre le stockage :** comparer les chemins de sortie Parquet avec et sans `partitionBy`, inspecter l’arborescence et lire une requête filtrée sur la date.
6. **Raisonner sur l’échelle :** expliquer ce qui devrait changer pour répartir le calcul sur plusieurs machines et le stockage sur HDFS ou un stockage objet. Identifier les coûts réseau et la tolérance aux pannes.
7. **Améliorer une action coûteuse :** remplacer les nombreux `count()` de la recherche de seuils par une agrégation conditionnelle et comparer les jobs déclenchés.

## Questions probables du jury

- Quelle différence entre Spark Master, driver, worker et executor ? Où tourne chacun dans ton projet ?
- Que se passe-t-il entre l’appel à `count()` et le résultat ? Quels objets sont créés et où le calcul s’exécute-t-il ?
- Pourquoi Spark est-il paresseux ? Qu’est-ce qui déclenche réellement le calcul ?
- Qu’est-ce qu’une partition de calcul ? Quelle différence avec `partitionBy("date")` ?
- Quelles opérations causent un shuffle dans ton projet, et pourquoi le shuffle est-il coûteux ?
- Pourquoi Parquet est-il pertinent ici ? Quels avantages et quels compromis apporte le partitionnement par date ?
- À quoi sert `spark.sql.shuffle.partitions=18` ? Que se passerait-il si la valeur était trop faible ou trop élevée ?
- Ce projet est-il vraiment un cluster distribué ? Qu’est-ce qui est distribué et qu’est-ce qui reste local ?
- Les données sont-elles répliquées ou tolérantes à la panne d’une machine ? Que perd-on si l’hôte s’arrête ?
- Comment vérifier que les workers participent réellement au calcul ? Quelles pages ou métriques consulter ?
- Pourquoi ne pas convertir le DataFrame complet avec `toPandas()` ?
- Comment choisir la mémoire et le nombre de cœurs des workers ? Quelle est la différence entre ressources Docker et ressources annoncées à Spark ?
- Que faudrait-il changer pour passer de ce laboratoire local à un vrai cluster multi-nœuds ?
- Pourquoi une validation croisée peut-elle consommer beaucoup de temps et de ressources ?

## Réponse courte à mémoriser sur la portée du projet

« J’utilise Spark Standalone dans plusieurs conteneurs pour étudier l’exécution distribuée d’une application Spark. Le driver s’exécute avec Jupyter dans le conteneur master, et les tâches de calcul sont envoyées aux executors des workers. Les DataFrames et Parquet permettent le traitement partitionné. En revanche, tous les conteneurs sont sur un seul hôte et les fichiers sont dans un volume local partagé : le projet illustre l’ordonnancement et le calcul Spark, mais ne démontre ni le stockage distribué ni la résilience d’un cluster multi-machines. »

## Compétences à pouvoir revendiquer après appropriation

- Lire un déploiement Docker Compose et expliquer réseaux, ports, volumes et limites de ressources.
- Décrire le chemin driver → master/gestion des ressources → executors → résultats.
- Écrire des transformations Spark SQL/DataFrame et identifier les actions qui déclenchent des jobs.
- Expliquer partitions de calcul, stages, tâches, shuffle et partitionnement Parquet.
- Lire un plan Spark et diagnostiquer une exécution avec les interfaces Spark.
- Éviter les collectes excessives vers le driver et repérer les recalculs inutiles.
- Décrire honnêtement la différence entre un laboratoire Spark local et une plateforme Big Data multi-nœuds.

## Points précis du code à vérifier avec le mentorat

- Le notebook 02 filtre les types `TRANSFER` et `CASH_OUT`, puis fait un `randomSplit` non stratifié. Savoir expliquer ce choix et le risque faible de variation des taux de fraude entre échantillons.
- Le commentaire de `CrossValidator` disant « une par worker » est une intuition pédagogique, pas une garantie d’affectation. Reformuler en termes de concurrence et de ressources disponibles.
- Les chemins `/opt/spark-data` désignent les mêmes fichiers de l’hôte via les bind mounts. C’est ce partage qui rend les lectures possibles entre conteneurs ; ce n’est pas une couche de stockage Spark distribuée.
- Les interfaces Web aident à observer les rôles, mais une page worker active à elle seule ne prouve pas que les tâches sont équilibrées ou efficaces : regarder les tâches et leurs métriques dans l’UI de l’application.
