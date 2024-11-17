from flask import Flask, jsonify, request
from lookml_generator import LookMLGenerator

app = Flask(__name__)

OUTPUT_DIR = "lookml_files"
lookml_generator = LookMLGenerator(output_dir=OUTPUT_DIR)

@app.route('/generate_view', methods=['POST'])
def generate_view():
    try:
        data = request.json
        table_name = data.get("table_name")
        fields = data.get("fields")

        if not table_name or not fields:
            return jsonify({
                "status": "error",
                "message": "El campo 'table_name' y la lista 'fields' son obligatorios."
            }), 400

        lookml_generator.generate_view(table_name, fields)
        return jsonify({
            "status": "success",
            "message": f"Archivo {table_name}.view.lkml generado exitosamente."
        }), 200

    except Exception as e:
        lookml_generator.log_error(str(e))
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/generate_explore', methods=['POST'])
def generate_explore():
    try:
        data = request.json
        explore_name = data.get("explore_name")
        joins = data.get("joins")

        if not explore_name or not joins:
            return jsonify({
                "status": "error",
                "message": "El campo 'explore_name' y la lista 'joins' son obligatorios."
            }), 400

        lookml_generator.generate_explore(explore_name, joins)
        return jsonify({
            "status": "success",
            "message": f"Archivo {explore_name}.explore.lkml generado exitosamente."
        }), 200

    except Exception as e:
        lookml_generator.log_error(str(e))
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "ok",
        "message": "El servicio LookML Generator está funcionando correctamente."
    }), 200


if __name__ == "__main__":
    app.run(port=5004)
