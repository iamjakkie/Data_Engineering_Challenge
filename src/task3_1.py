import requests

URL = 'https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data'
LOCAL_FILE = 'data/iris.csv'

def download_iris_data():
    url = URL
    r = requests.get(url)
    with open(LOCAL_FILE, 'wb') as f:
        f.write(r.content)

def main():
    download_iris_data()

if __name__ == '__main__':
    main()