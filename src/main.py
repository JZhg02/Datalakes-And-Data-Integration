from flask import Flask, request, jsonify
import pandas as pd
import io
from unpacked_to_raw import upload_to_S3_with_csv
import requests 
import requests
import base64

app = Flask(__name__)


@app.route('/ingest/csv', methods=['POST'])
def ingest_csv():
    # to improve
    if 'files' not in request.files:
        return jsonify({'error': 'No files provided'}), 400

    csv_files = request.files.getlist('files')
    results = {}

    for file in csv_files:
        if file.filename.endswith('.csv'):
            try:
                df = pd.read_csv(io.StringIO(file.stream.read().decode("utf-8")))
                results[file.filename] = df.to_dict()  # Convert DataFrame to dictionary
            except Exception as e:
                results[file.filename] = {'error': str(e)}
        else:
            results[file.filename] = {'error': 'Invalid file format'}

    return jsonify({'message': 'CSV files processed successfully', 'data': results}), 200


@app.route('/ingest/blob', methods=['POST'])
def ingest_blob():
    # to improve
    if not request.data:
        return jsonify({'error': 'No blob data provided'}), 400

    try:
        blobs = request.get_json().get('blobs', [])  # Expecting a JSON array of blobs
        results = {f'blob_{i}': blob for i, blob in enumerate(blobs)}
        return jsonify({'message': 'Blobs received', 'data': results}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@app.route('/ingest', methods=['POST'])
def ingest():

    upload_to_S3_with_csv(request.files.getlist('files'))

    # Trigger the Airflow DAG for the regular pipeline
    dag_response = trigger_dag('regular_datalake_pipeline')


    return jsonify(dag_response), 200

def trigger_dag(dag_id, conf=None):
    # Airflow webserver URL
    airflow_url = 'http://localhost:8080'

    # Authentication credentials
    username = 'admin'
    password = 'admin'

    # Endpoint for triggering DAGs
    endpoint = f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns"

    # Create basic auth header
    auth_credentials = f"{username}:{password}"
    auth_bytes = auth_credentials.encode('ascii')
    auth_base64 = base64.b64encode(auth_bytes).decode('ascii')

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {auth_base64}"
    }

    payload = {}
    if conf:
        payload["conf"] = conf

    try:
        response = requests.post(endpoint, headers=headers, json=payload)

        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error triggering DAG: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response body: {e.response.text}")
        return None

@app.route('/ingest/fast', methods=['POST'])
def ingest_fast():

    upload_to_S3_with_csv(request.files.getlist('files'))

    # Trigger the Airflow DAG
    dag_response = trigger_dag('datalake_pipeline')

    return jsonify(dag_response), 200


if __name__ == '__main__':
    app.run(debug=True)


#TO DO Un aspect crucial de cet endpoint sera la mesure de ses performances. Vous devrez
# chronométrer et documenter le temps d’exécution de votre pipeline pour :
# • Un batch contenant un seul élément
# • Un batch de 10 éléments
# Ces mesures serviront de base de comparaison pour le second endpoint optimisé.
# FAIRE UN README pour expliquer la marche à suivre pour tester et/ou vidéo
