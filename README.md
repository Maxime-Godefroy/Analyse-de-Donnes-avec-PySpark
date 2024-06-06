# Projet d'Analyse de Données avec PySpark

Ce projet comprend trois scripts principaux pour analyser des données provenant de différentes sources : Airbnb, Netflix et les tweets de Donald Trump. Les scripts utilisent Apache Spark pour le traitement des données et Plotext pour la visualisation.

## Prérequis

Assurez-vous d'avoir les éléments suivants installés sur votre machine :

1. **Python 3.x** : Vous pouvez télécharger la dernière version de Python depuis le site officiel [Python.org](https://www.python.org/downloads/).
2. **Apache Spark** : Suivez les instructions d'installation sur le site officiel de [Spark](https://spark.apache.org/downloads.html).
3. **Dépendances Python** : Installez les bibliothèques nécessaires avec `pip`.

## Installation des Dépendances

Utilisez `pip` pour installer les dépendances Python nécessaires :

```bash
pip install pyspark plotext
```

Exécution des Scripts
Analyse Airbnb

Le script AIRBNB.py analyse les données des annonces Airbnb pour extraire des informations telles que la répartition des types de chambres, les revenus moyens, etc.

Placez le fichier listings.csv dans le répertoire ressources.
Exécutez le script :

```bash
python AIRBNB.py
```

Analyse Netflix

Le script Netflix.py analyse les données des titres Netflix pour extraire des informations sur les réalisateurs les plus prolifiques, les durées moyennes des films, etc.

Placez le fichier netflix_titles.csv dans le répertoire ressources.
Exécutez le script :

```bash
python Netflix.py
```

Analyse des Tweets de Donald Trump

Le script TrumpTweeter.py analyse les tweets de Donald Trump pour extraire des informations sur les cibles de ses insultes, les insultes les plus courantes, etc.

Placez le fichier trump_insult_tweets_2014_to_2021.csv dans le répertoire ressources.
Exécutez le script :

```bash
python TrumpTweeter.py
```

Résultats et Visualisations

Les scripts afficheront les résultats dans la console et généreront des visualisations de barres horizontales en utilisant Plotext.
Structure du Projet

```plaintext

├── AIRBNB.py
├── Netflix.py
├── TrumpTweeter.py
├── README.md
└── ressources
    ├── listings.csv
    ├── netflix_titles.csv
    └── trump_insult_tweets_2014_to_2021.csv
```

Auteur

Ce projet a été réalisé par Maxime Godefroy.