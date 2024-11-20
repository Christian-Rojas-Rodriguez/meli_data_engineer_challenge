import unittest
from unittest.mock import patch, MagicMock
from google.cloud import bigquery
from modeler import DataModeler


class TestDataModeler(unittest.TestCase):
    def setUp(self):
        self.project_id = "meli-prueba-data"
        self.dataset_id = "meli_dataset"
        self.modeler = DataModeler(self.project_id, self.dataset_id)

    def test_define_schema(self):
        schema = self.modeler.define_schema()

        self.assertIn("Producto", schema)
        self.assertIn("Vendedor", schema)
        self.assertIn("Envio", schema)

        self.assertEqual(len(schema["Producto"]), 3)
        self.assertEqual(len(schema["Vendedor"]), 2)
        self.assertEqual(len(schema["Envio"]), 4)

    @patch("google.cloud.bigquery.Client", autospec=True)
    def test_create_table_success(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.create_table.return_value = MagicMock()

        schema = self.modeler.define_schema()["Producto"]
        with patch("builtins.print") as mock_print:
            self.modeler.create_table("Producto", schema)

            table_ref = f"{self.project_id}.{self.dataset_id}.Producto"
            mock_client.create_table.assert_called_once()
            mock_print.assert_called_once_with(f"Tabla Producto creada exitosamente.")

    @patch("google.cloud.bigquery.Client")
    def test_create_table_failure(self, mock_client):
        mock_client.return_value.create_table.side_effect = Exception("Error de prueba")

        schema = self.modeler.define_schema()["Producto"]
        with patch("builtins.print") as mock_print:
            self.modeler.create_table("Producto", schema)

            mock_print.assert_called_with("Error: Error al crear la tabla Producto: Error de prueba")

    @patch("google.cloud.bigquery.Client")
    def test_create_all_tables_success(self, mock_client):
        mock_client.return_value.create_table.return_value = MagicMock()

        with patch("builtins.print") as mock_print:
            self.modeler.create_all_tables()

            self.assertEqual(mock_client.return_value.create_table.call_count, 3)
            mock_print.assert_any_call("Tabla Producto creada exitosamente.")
            mock_print.assert_any_call("Tabla Vendedor creada exitosamente.")
            mock_print.assert_any_call("Tabla Envio creada exitosamente.")

    @patch("google.cloud.bigquery.Client")
    def test_create_all_tables_partial_failure(self, mock_client):
        mock_client.return_value.create_table.side_effect = [
            MagicMock(),
            Exception("Error al crear tabla Vendedor"),
            MagicMock(),
        ]

        with patch("builtins.print") as mock_print:
            self.modeler.create_all_tables()

            self.assertEqual(mock_client.return_value.create_table.call_count, 3)

            mock_print.assert_any_call("Tabla Producto creada exitosamente.")
            mock_print.assert_any_call("Error: Error al crear la tabla Vendedor: Error al crear tabla Vendedor")
            mock_print.assert_any_call("Tabla Envio creada exitosamente.")


if __name__ == "__main__":
    unittest.main()
