# Datalakes-And-Data-Integration
Project for the Datalakes &amp; Data Integration class

## Docker compose
```
docker-compose up -d
```

# API 

## Flask API 
We choose to use flask for our API as it is a simple and efficient way to do an API Gateway for a POC such as this.
To run the api please run the following command at the root of the project folder:
(you may need to replace pyton by python3 or py)
```
python src/main.py
```

### Endpoints
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

### TESTING ENDPOINTS

TODO 