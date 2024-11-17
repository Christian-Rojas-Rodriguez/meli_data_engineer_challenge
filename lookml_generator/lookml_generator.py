import os
import lkml

class LookMLGenerator:
    def __init__(self, output_dir):
        self.output_dir = output_dir

    def generate_view(self, table_name, fields):
        try:
            view = {
                "view": {
                    "name": table_name,
                    "fields": [
                        {
                            "dimension": {
                                "name": field["name"],
                                "type": field["type"],
                                "sql": f"${{TABLE}}.{field['name']} ;;"
                            }
                        } for field in fields
                    ]
                }
            }
            output_path = f"{table_name}.view.lkml"
            self.write_lookml_file(view, output_path)
            print(f"Archivo generado: {output_path}")
        except Exception as e:
            self.log_error(f"Error al generar view para {table_name}: {e}")

    def generate_explore(self, explore_name, joins):
        try:
            explore = {
                "explore": {
                    "name": explore_name,
                    "joins": [
                        {
                            "join": {
                                "name": join["table"],
                                "type": join["type"],
                                "sql_on": join["condition"]
                            }
                        } for join in joins
                    ]
                }
            }
            output_path = f"{explore_name}.explore.lkml"
            self.write_lookml_file(explore, output_path)
            print(f"Archivo generado: {output_path}")
        except Exception as e:
            self.log_error(f"Error al generar explore {explore_name}: {e}")

    def write_lookml_file(self, lookml_object, file_name):
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            output_path = os.path.join(self.output_dir, file_name)
            with open(output_path, "w") as file:
                file.write(lkml.dump(lookml_object))
        except Exception as e:
            self.log_error(f"Error al escribir archivo {file_name}: {e}")

    def log_error(self, message):
        print(f"Error: {message}")
