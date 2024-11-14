import unittest
from unittest.mock import patch, MagicMock
import json
from downloader import MLDataFetcher

class TestMLDataFetcher(unittest.TestCase):

    def setUp(self):
        self.fetcher = MLDataFetcher(query="chromecast", limit=50, total_results=100)

    @patch('downloader.requests.get')
    def test_fetch_batch(self, mock_get):
        # Simulación de respuesta de la API
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"results": [{"id": "1"}, {"id": "2"}]}
        mock_get.return_value = mock_response

        data = self.fetcher._fetch_batch(offset=0)
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["id"], "1")

    @patch('downloader.requests.get')
    def test_fetch_data(self, mock_get):
        # Simulación de múltiples llamadas a la API para completar el total de resultados
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"results": [{"id": f"{i}"} for i in range(50)]}
        mock_get.return_value = mock_response

        data = self.fetcher.fetch_data()
        self.assertEqual(len(data), 100)  # Debe devolver 100 resultados en total

    @patch("builtins.open", new_callable=unittest.mock.mock_open)
    def test_save_data(self, mock_open):
        data = [{"id": "1"}, {"id": "2"}]
        self.fetcher.save_data(data, filename="test_data.json")

        # Verifica que el archivo se abrió correctamente y los datos se escribieron
        mock_open.assert_called_once_with("test_data.json", "w")
        mock_open().write.assert_called_once()
        saved_data = json.loads(mock_open().write.call_args[0][0])
        self.assertEqual(saved_data, data)

    def test_log_error(self):
        with patch('builtins.print') as mock_print:
            self.fetcher.log_error("Test error")
            mock_print.assert_called_once_with("Error: Test error")

if __name__ == "__main__":
    unittest.main()
