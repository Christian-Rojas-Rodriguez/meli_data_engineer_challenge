from google.cloud import bigquery
import lkml
import os


class LookMLGenerator:
    def __init__(self, project_id, dataset_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        self.views = {}
        self.explore = None

    def fetch_table_schema(self, table_name):
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_name}"
        table = self.client.get_table(table_ref)
        return [{"name": field.name, "type": field.field_type} for field in table.schema]

    def create_view(self, table_name):
        fields = self.fetch_table_schema(table_name)
        view = {
            "view": {
                "name": table_name,
                "sql_table_name": f"{self.project_id}.{self.dataset_id}.{table_name}",
                "fields": [
                    {"name": field["name"], "type": self.map_field_type(field["type"])}
                    for field in fields
                ]
            }
        }
        self.views[table_name] = view

    def get_created_views(self):
        if not self.views:
            return {"status": "error", "message": "No se han creado views aún."}

        return {
            "status": "success",
            "views": [
                {
                    "name": view["view"]["name"],
                    "sql_table_name": view["view"]["sql_table_name"],
                    "fields": view["view"]["fields"]
                }
                for view in self.views.values()
            ]
        }

    def create_explore(self, explore_name, joins):
        explore = {
            "explore": {
                "name": explore_name,
                "joins": [
                    {
                        "name": join["view_name"],
                        "type": join.get("type", "left_outer"),
                        "sql_on": join["sql_on"]
                    }
                    for join in joins
                ]
            }
        }
        self.explore = explore

    def get_explore(self):
        if not hasattr(self, "explore") or not self.explore:
            return {"status": "error", "message": "No se ha creado ningún explore aún."}

        return {"status": "success", "explore": self.explore}
    """
    def generate_view_files(self, output_dir):
        os.makedirs(output_dir, exist_ok=True)
        if not isinstance(self.views, dict):
            raise ValueError("self.views debe ser un diccionario con vistas")
        for table_name, view in self.views.items():
            file_path = f"{output_dir}/{table_name}.view.lkml"
            with open(file_path, "w") as file:
                file.write(lkml.dump(view))
            print(f"Archivo generado: {file_path}")
    """
    def generate_view_files(self, output_dir):
        os.makedirs(output_dir, exist_ok=True)
        print(f"Views recibidos: {self.views}")
        for table_name, view in self.views.items():
            print(f"Procesando: {table_name}, View: {view}")
            file_path = f"{output_dir}/{table_name}.view.lkml"
        with open(file_path, "w") as file:
            try:
                file.write(lkml.dumps(view))
                print(f"Archivo generado: {file_path}")
            except Exception as e:
                print(f"Error al escribir el archivo: {e}")
                raise


    def generate_explore_file(self, output_dir, explore_name):
        os.makedirs(output_dir, exist_ok=True)

        if not self.explore:
            raise ValueError(f"No se encontró la definición del explore: {explore_name}")

        file_path = f"{output_dir}/{explore_name}.explore.lkml"
        with open(file_path, "w") as file:
            file.write(lkml.dump(self.explore))
        print(f"Archivo generado: {file_path}")

    @staticmethod
    def map_field_type(field_type):
        mapping = {
            "STRING": "string",
            "INTEGER": "number",
            "FLOAT": "number",
            "BOOL": "yesno",
            "BOOLEAN": "yesno"
        }
        return mapping.get(field_type, "string")