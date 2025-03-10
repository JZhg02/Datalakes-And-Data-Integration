# Datalakes-And-Data-Integration
Project for the Datalakes &amp; Data Integration class

## Airflow Setup

Ce projet utilise Apache Airflow pour orchestrer le pipeline de transformation des données. L'environnement Airflow est déployé via Docker Compose, ce qui permet de démarrer simultanément tous les conteneurs nécessaires (Airflow, Redis, PostgreSQL, Localstack, Cassandra, etc.).

### Démarrage de l'environnement Airflow

Après avoir lancé la commande `docker-compose up -d`, les conteneurs suivants sont démarrés :

- **airflow-init** : Initialise la base de métadonnées d'Airflow et crée un utilisateur administrateur par défaut.
- **airflow-webserver** : Fournit l'interface web pour visualiser et gérer les DAGs.
- **airflow-scheduler** : Planifie l'exécution des tâches selon les dépendances définies dans les DAGs.
- **airflow-worker** : Exécute les tâches distribuées via Celery.

### Accès au Webserver d'Airflow

Ouvrez votre navigateur et accédez à :

http://localhost:8080

Connectez-vous avec les identifiants suivants :

- **Username :** admin  
- **Password :** admin  

Une fois authentifié, vous pourrez accéder au DAG qui orchestre l'ensemble des transformations de données.

Assurez-vous d'avoir un .env avec les clés API associés
---
## API 

### Flask API 
We choose to use flask for our API as it is a simple and efficient way to do an API Gateway for a POC such as this.
To run the api please run the following command at the root of the project folder:
(you may need to replace pyton by python3 or py)
```
python src/main.py
```

#### Endpoints
We have 4 endpoints in our API, all are of the method POST:
- /ingest/blob : This endpoint is used to ingest data from one or several blob made out of csv files. It takes a json payload with the following format:
```
{
    "data" : [blob1, blob2]
}
```
- /ingest/csv : This endpoint is used to ingest data from one or several csv files. It takes a json payload with the following format:
```
{
    "files" : ["file1.csv", "file2.csv"]
}
```
- /ingest : This endpoint autoredirect to the right endpoint depending on the type of the file. It takes a json payload with the following format:
```
{
    "files" : ["file1.csv", "file2.csv"]
}
OR
{
    "data" : [blob1, blob2]
}
```
- /ingest/fast : This endpoint is an optimised version of the endpoint /ingest. It takes the same possible payload.

#### TESTING ENDPOINTS

TODO 
