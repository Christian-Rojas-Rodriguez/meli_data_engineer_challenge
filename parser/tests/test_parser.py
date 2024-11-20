import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from parser import DataParser


class TestDataParser(unittest.TestCase):
    def setUp(self):
        self.input_file = "test_data.json"
        self.project_id = "meli-prueba-data"
        self.dataset_id = "meli_dataset"
        self.parser = DataParser(self.input_file, self.project_id, self.dataset_id)

    @patch("pandas.read_json")
    def test_extract_success(self, mock_read_json):
        mock_df = pd.DataFrame([
            {"id": "1", "title": "Producto 1", "price": 100.0}
        ])
        mock_read_json.return_value = mock_df

        result = self.parser.extract()

        mock_read_json.assert_called_once_with(self.input_file)
        pd.testing.assert_frame_equal(result, mock_df)

    @patch("pandas.read_json", side_effect=FileNotFoundError("Archivo no encontrado"))
    def test_extract_file_not_found(self, mock_read_json):
        with patch("builtins.print") as mock_print:
            result = self.parser.extract()
            self.assertTrue(result.empty)
            mock_print.assert_called_once_with("Error: Archivo no encontrado: Archivo no encontrado")

    @patch("pandas.read_json", side_effect=ValueError("Error al cargar JSON"))
    def test_extract_invalid_json(self, mock_read_json):
        with patch("builtins.print") as mock_print:
            result = self.parser.extract()
            self.assertTrue(result.empty)
            mock_print.assert_called_once_with("Error: Error al cargar JSON: Error al cargar JSON")

    def test_transform_success(self):
        df = pd.DataFrame([
            {
                "id": "1",
                "title": "Producto 1",
                "price": 100.0,
                "seller": {"id": 12345, "nickname": "SellerName"},
                "shipping": {
                    "free_shipping": True,
                    "logistic_type": "cross_docking",
                    "mode": "me2",
                    "store_pick_up": False
                }
            }
        ])

        self.parser.transform(df)

        # Verificar DataFrame "Producto"
        expected_producto = pd.DataFrame([
            {"id": "1", "title": "Producto 1", "price": 100.0}
        ])
        pd.testing.assert_frame_equal(self.parser.dataframes["Producto"], expected_producto)

        # Verificar DataFrame "Vendedor"
        expected_vendedor = pd.DataFrame([
            {"seller_id": "12345", "seller_name": "SellerName"}
        ])
        expected_vendedor["seller_id"] = expected_vendedor["seller_id"].astype(str)  # Ajuste necesario
        self.parser.dataframes["Vendedor"]["seller_id"] = self.parser.dataframes["Vendedor"]["seller_id"].astype(str)
        pd.testing.assert_frame_equal(self.parser.dataframes["Vendedor"], expected_vendedor)

    def test_transform_empty_dataframe(self):
        df = pd.DataFrame()

        with patch("builtins.print") as mock_print:
            self.parser.transform(df)
            mock_print.assert_called_once_with("Error: El DataFrame está vacío. No se puede transformar.")
            self.assertEqual(self.parser.dataframes, {})

    @patch("google.cloud.bigquery.Client")
    def test_load_success(self, mock_client):
        mock_dataframe = pd.DataFrame([{"id": "1", "title": "Producto 1", "price": 100.0}])
        self.parser.dataframes = {"Producto": mock_dataframe}

        mock_client.return_value.load_table_from_dataframe.return_value.result = MagicMock()

        with patch("builtins.print") as mock_print:
            self.parser.load()

            table_id = f"{self.project_id}.{self.dataset_id}.Producto"
            mock_client.return_value.load_table_from_dataframe.assert_called_once_with(mock_dataframe, table_id)
            mock_print.assert_called_once_with(f"Datos cargados correctamente en la tabla: {table_id}")

    @patch("google.cloud.bigquery.Client")
    def test_load_failure(self, mock_client):
        mock_dataframe = pd.DataFrame([{"id": "1", "title": "Producto 1", "price": 100.0}])
        self.parser.dataframes = {"Producto": mock_dataframe}

        mock_client.return_value.load_table_from_dataframe.side_effect = Exception("Error de prueba")

        with patch("builtins.print") as mock_print:
            self.parser.load()

            mock_print.assert_called_with("Error: Error al cargar datos en la tabla Producto: Error de prueba")

    def test_log_error(self):
        error_message = "Este es un error de prueba"
        with patch("builtins.print") as mock_print:
            self.parser.log_error(error_message)
            mock_print.assert_called_once_with(f"Error: {error_message}")


if __name__ == "__main__":
    unittest.main()
