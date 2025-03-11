import os
import requests

def test_ingest():

    #test ingest function with all files from test_files
    base_url = 'http://localhost:5000'
    url = f'{base_url}/ingest'

    files_list = []
    for file in os.listdir('../test_files'):
        files_list.append(('files', open(f'../test_files/{file}', 'rb')))

    response = requests.post(url, files=files_list)

    print(response.status_code)

def test_ingest_fast():

    #test ingest_fast function with all files from test_files
    base_url = 'http://localhost:5000'
    url = f'{base_url}/ingest/fast'

    files_list = []
    for file in os.listdir('../test_files'):
        files_list.append(('files', open(f'../test_files/{file}', 'rb')))

    response = requests.post(url, files=files_list)

    assert response.status_code == 200

def main():
    test_ingest()
    test_ingest_fast()

if __name__ == '__main__':
    main()
