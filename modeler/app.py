# modeler/app.py
from flask import Flask, jsonify
from modeler import DataModeler

app = Flask(__name__)

data_modeler = DataModeler(project_id="meli-prueba-data", dataset_id="meli_dataset")

@app.route('/create_tables', methods=['POST'])
def create_tables():
    try:
        data_modeler.create_all_tables()
        return jsonify({"status": "success", "message": "Tablas creadas exitosamente."}), 200
    except Exception as e:
        data_modeler.log_error(str(e))
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(port=5002)
