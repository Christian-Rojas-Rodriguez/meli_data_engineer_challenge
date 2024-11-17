from flask import Flask, jsonify
from parser import DataParser
import pandas as pd

app = Flask(__name__)

INPUT_FILE = "downloader/data.json"
PROJECT_ID = "meli-prueba-data"
DATASET_ID = ("meli_dataset")

@app.route('/etl', methods=['POST'])
def run_etl():
    try:
        parser = DataParser(INPUT_FILE, PROJECT_ID, DATASET_ID)

        df = parser.extract()
        if df.empty:
            return jsonify({"status": "error", "message": "El archivo está vacío o no existe."}), 400

        parser.transform(df)
        parser.load()

        return jsonify({"status": "success", "message": "ETL ejecutado correctamente."}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "ok", "message": "El servicio Parser está funcionando correctamente."}), 200

if __name__ == "__main__":
    app.run(port=5003)
