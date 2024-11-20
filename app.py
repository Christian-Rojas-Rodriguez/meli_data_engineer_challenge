from flask import Flask
from google.cloud import bigquery
app = Flask(__name__)


@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'


if __name__ == '__main__':
    client = bigquery.Client(project="meli-prueba-data")
    print(client)
    app.run()
