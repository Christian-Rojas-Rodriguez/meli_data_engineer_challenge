from google.cloud import bigquery

class DataModeler:
    def __init__(self, project_id, dataset_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)

    def define_schema(self):
        schema = {
            "Producto": [
                bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("price", "FLOAT", mode="NULLABLE")
            ],
            "Vendedor": [
                bigquery.SchemaField("seller_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("seller_name", "STRING", mode="NULLABLE")
            ],
            "Ubicacion": [
                bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("address", "STRING", mode="NULLABLE")
            ]
        }
        return schema

    def create_table(self, table_name, schema):
        dataset_ref = f"{self.project_id}.{self.dataset_id}"  # String en lugar de `self.client.dataset`
        table_ref = bigquery.TableReference.from_string(f"{dataset_ref}.{table_name}")
        table = bigquery.Table(table_ref, schema=schema)
        try:
            table = self.client.create_table(table)
            print(f"Tabla {table_name} creada exitosamente.")
        except Exception as e:
            self.log_error(f"Error al crear la tabla {table_name}: {e}")

    def create_all_tables(self):
        schema = self.define_schema()
        for table_name, table_schema in schema.items():
            self.create_table(table_name, table_schema)

    def log_error(self, error_message):
        print(f"Error: {error_message}")
