from flask import Flask, jsonify, request
from lookml_generator import LookMLGenerator

app = Flask(__name__)

lookml_generator = LookMLGenerator(project_id="meli-prueba-data", dataset_id="meli_dataset")


@app.route("/generate_views", methods=["POST"])
def generate_views():
    try:
        data = request.get_json()
        table_names = data["table_names"]

        for table_name in table_names:
            lookml_generator.create_view(table_name)

        return jsonify({"status": "success", "message": "Views generadas correctamente."}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

@app.route("/get_views", methods=["GET"])
def get_views():
    try:
        result = lookml_generator.get_created_views()
        return jsonify(result), 200 if result["status"] == "success" else 400
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/generate_explore", methods=["POST"])
def generate_explore():
    try:
        data = request.get_json()
        explore_name = data["explore_name"]
        joins = data["joins"]

        lookml_generator.create_explore(explore_name, joins)
        return jsonify({"status": "success", "message": f"Explore {explore_name} generado correctamente."}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400


@app.route("/export_lookml", methods=["POST"])
def export_lookml():
    try:
        data = request.get_json()
        print("Recibe el json")
        output_dir = data.get("output_dir")
        explore_name = data.get("explore_name")
        print("Se dividio el json")

        if not output_dir or not explore_name:
            raise ValueError("Los campos 'output_dir' y 'explore_name' son obligatorios.")

        lookml_generator.generate_view_files(output_dir)
        lookml_generator.generate_explore_file(output_dir, explore_name)

        return jsonify({"status": "success", "message": "Archivos LookML exportados."}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400


if __name__ == "__main__":
    app.run(port=5004)
