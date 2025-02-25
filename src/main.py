from flask import Flask, request, jsonify
import pandas as pd
import io

app = Flask(__name__)


@app.route('/ingest/csv', methods=['POST'])
def ingest_csv():
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
    response = {}

    # Handle CSV files
    if 'files' in request.files:
        csv_response = ingest_csv()
        response['csv'] = csv_response.get_json()

    # Handle Blobs
    if request.is_json:
        blob_response = ingest_blob()
        response['blobs'] = blob_response.get_json()

    if not response:
        return jsonify({'error': 'No valid CSV files or blobs provided'}), 400

    return jsonify(response), 200


@app.route('/ingest/fast', methods=['POST'])
def ingest_fast():
    #should be an optimized version of ingest, should be at least 30% faster
    return ingest()

if __name__ == '__main__':
    app.run(debug=True)


#TO DO Un aspect crucial de cet endpoint sera la mesure de ses performances. Vous devrez
# chronométrer et documenter le temps d’exécution de votre pipeline pour :
# • Un batch contenant un seul élément
# • Un batch de 10 éléments
# Ces mesures serviront de base de comparaison pour le second endpoint optimisé.
# FAIRE UN README pour expliquer la marche à suivre pour tester et/ou vidéo
