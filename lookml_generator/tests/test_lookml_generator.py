import unittest
from unittest.mock import patch, MagicMock
from lookml_generator import LookMLGenerator


class TestLookMLGenerator(unittest.TestCase):
    def setUp(self):
        self.project_id = "test_project"
        self.dataset_id = "test_dataset"
        self.generator = LookMLGenerator(self.project_id, self.dataset_id)

    @patch("google.cloud.bigquery.Client")
    def test_fetch_table_schema(self, mock_client):
        # Simula el esquema de una tabla
        mock_table = MagicMock()
        mock_table.schema = [
            MagicMock(name="id", field_type="STRING"),
            MagicMock(name="price", field_type="FLOAT")
        ]
        mock_client.return_value.get_table.return_value = mock_table

        schema = self.generator.fetch_table_schema("test_table")
        self.assertEqual(schema, [
            {"name": "id", "type": "STRING"},
            {"name": "price", "type": "FLOAT"}
        ])

    @patch("lookml_generator.LookMLGenerator.fetch_table_schema")
    def test_create_view(self, mock_fetch_schema):
        # Simula los resultados del esquema de una tabla
        mock_fetch_schema.return_value = [
            {"name": "id", "type": "STRING"},
            {"name": "price", "type": "FLOAT"}
        ]
        self.generator.create_view("test_table")

        self.assertIn("test_table", self.generator.views)
        self.assertEqual(self.generator.views["test_table"], {
            "view": {
                "name": "test_table",
                "sql_table_name": "test_project.test_dataset.test_table",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "price", "type": "number"}
                ]
            }
        })

    def test_get_created_views(self):
        # Agrega una vista manualmente para probar el método
        self.generator.views = {
            "test_table": {
                "view": {
                    "name": "test_table",
                    "sql_table_name": "test_project.test_dataset.test_table",
                    "fields": [{"name": "id", "type": "string"}]
                }
            }
        }

        result = self.generator.get_created_views()
        self.assertEqual(result["status"], "success")
        self.assertEqual(len(result["views"]), 1)
        self.assertEqual(result["views"][0]["name"], "test_table")

    def test_get_created_views_no_views(self):
        # Sin vistas creadas
        self.generator.views = {}
        result = self.generator.get_created_views()
        self.assertEqual(result["status"], "error")
        self.assertIn("No se han creado views", result["message"])

    def test_create_explore(self):
        joins = [
            {"view_name": "test_view", "sql_on": "${test_table.id} = ${test_view.id}"}
        ]
        self.generator.create_explore("test_explore", joins)

        self.assertIsNotNone(self.generator.explore)
        self.assertEqual(self.generator.explore["explore"]["name"], "test_explore")
        self.assertEqual(len(self.generator.explore["explore"]["joins"]), 1)
        self.assertEqual(self.generator.explore["explore"]["joins"][0]["name"], "test_view")

    def test_get_explore_no_explore(self):
        result = self.generator.get_explore()
        self.assertEqual(result["status"], "error")
        self.assertIn("No se ha creado ningún explore", result["message"])

    def test_get_explore(self):
        self.generator.explore = {
            "explore": {
                "name": "test_explore",
                "joins": []
            }
        }
        result = self.generator.get_explore()
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["explore"]["explore"]["name"], "test_explore")

    @patch("os.makedirs")
    @patch("builtins.open", new_callable=unittest.mock.mock_open)
    @patch("lkml.dump")
    def test_generate_view_files(self, mock_lkml_dump, mock_open, mock_makedirs):
        # Simula la existencia de una vista
        self.generator.views = {
            "test_table": {
                "view": {
                    "name": "test_table",
                    "sql_table_name": "test_project.test_dataset.test_table",
                    "fields": [{"name": "id", "type": "string"}]
                }
            }
        }

        mock_lkml_dump.return_value = "lkml_view_content"

        self.generator.generate_view_files("output_dir")
        mock_makedirs.assert_called_once_with("output_dir", exist_ok=True)
        mock_open.assert_called_once_with("output_dir/test_table.view.lkml", "w")
        mock_open().write.assert_called_once_with("lkml_view_content")

    @patch("os.makedirs")
    @patch("builtins.open", new_callable=unittest.mock.mock_open)
    @patch("lkml.dump")
    def test_generate_explore_file(self, mock_lkml_dump, mock_open, mock_makedirs):
        # Simula la existencia de un explore
        self.generator.explore = {
            "explore": {
                "name": "test_explore",
                "joins": []
            }
        }

        mock_lkml_dump.return_value = "lkml_explore_content"

        self.generator.generate_explore_file("output_dir", "test_explore")
        mock_makedirs.assert_called_once_with("output_dir", exist_ok=True)
        mock_open.assert_called_once_with("output_dir/test_explore.explore.lkml", "w")
        mock_open().write.assert_called_once_with("lkml_explore_content")

    @patch("lkml.dump")
    def test_generate_explore_file_no_explore(self, mock_lkml_dump):
        self.generator.explore = None
        with self.assertRaises(ValueError) as context:
            self.generator.generate_explore_file("output_dir", "test_explore")
        self.assertEqual(str(context.exception), "No se encontró la definición del explore: test_explore")


if __name__ == "__main__":
    unittest.main()
