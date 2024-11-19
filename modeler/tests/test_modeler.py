import unittest
from unittest.mock import patch, MagicMock
from google.cloud import bigquery
from modeler import DataModeler


class TestDataModeler(unittest.TestCase):

    def setUp(self):
        self.project_id = "meli-prueba-data"
        self.dataset_id = "meli_dataset"
        self.modeler = DataModeler(self.project_id, self.dataset_id)

    @patch("google.cloud.bigquery.Client")
    def test_initialize_modeler(self, mock_client):
        modeler = DataModeler(self.project_id, self.dataset_id)
        self.assertEqual(modeler.project_id, self.project_id)
        self.assertEqual(modeler.dataset_id, self.dataset_id)
        mock_client.assert_called_once_with(project=self.project_id)

    def test_define_schema(self):
        expected_schema = {
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
        schema = self.modeler.define_schema()
        self.assertEqual(schema, expected_schema)

    @patch("google.cloud.bigquery.Client")
    def test_create_table_success(self, mock_client):
        mock_table = MagicMock()
        mock_client.return_value.create_table.return_value = mock_table

        schema = self.modeler.define_schema()["Producto"]
        self.modeler.create_table("Producto", schema)

        mock_client.return_value.create_table.assert_called_once()

    @patch("google.cloud.bigquery.Client")
    def test_create_table_failure(self, mock_client):
        mock_client.return_value.create_table.side_effect = Exception("Error de prueba")

        schema = self.modeler.define_schema()["Producto"]

        with patch("builtins.print") as mock_print:
            self.modeler.create_table("Producto", schema)

            mock_print.assert_called_with("Error: Error al crear la tabla Producto: Error de prueba")

    @patch("google.cloud.bigquery.Client")
    def test_create_all_tables(self, mock_client):
        mock_client.return_value.create_table = MagicMock()

        with patch.object(self.modeler, "create_table") as mock_create_table:
            self.modeler.create_all_tables()
            self.assertEqual(mock_create_table.call_count, 3)  # Debe llamarse una vez por cada tabla

    def test_log_error(self):
        error_message = "Este es un error de prueba"
        with patch("builtins.print") as mock_print:
            self.modeler.log_error(error_message)
            mock_print.assert_called_once_with(f"Error: {error_message}")


if __name__ == "__main__":
    unittest.main()
