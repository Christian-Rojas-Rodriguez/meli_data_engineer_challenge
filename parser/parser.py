from google.cloud import bigquery
import pandas as pd


class DataParser:
    def __init__(self, input_file, project_id, dataset_id):
        self.input_file = input_file
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.dataframes = {}
        self.client = bigquery.Client(project=project_id)

    def extract(self):
        try:
            df = pd.read_json(self.input_file)
            return df
        except FileNotFoundError as e:
            self.log_error(f"Archivo no encontrado: {e}")
            return pd.DataFrame()
        except ValueError as e:
            self.log_error(f"Error al cargar JSON: {e}")
            return pd.DataFrame()

    def transform(self, df):
        if df.empty:
            self.log_error("El DataFrame está vacío. No se puede transformar.")
            return

        self.dataframes["Producto"] = df[["id", "title", "price"]]

        if "seller" in df.columns:
            sellers = df["seller"].apply(lambda x: pd.json_normalize(x) if isinstance(x, dict) else pd.DataFrame())
            self.dataframes["Vendedor"] = pd.concat(sellers.tolist(), ignore_index=True).rename(
                columns={"id": "seller_id", "nickname": "seller_name"}
            )

        if "shipping" in df.columns:
            shipping_df = pd.json_normalize(df["shipping"]).rename(
                columns={
                    "free_shipping": "is_free_shipping",
                    "logistic_type": "logistic_type",
                    "mode": "shipping_mode",
                    "store_pick_up": "store_pick_up"
                }
            )

            shipping_df = shipping_df.drop(columns=["benefits", "promise", "tags", "shipping_score"], errors="ignore")
            self.dataframes["Envio"] = shipping_df

    def load(self):
        for table_name, df in self.dataframes.items():
            if df is not None:
                for column in df.select_dtypes(include=["int64"]).columns:
                    df[column] = df[column].astype(str)

                table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
                try:
                    self.client.load_table_from_dataframe(df, table_id).result()
                    print(f"Datos cargados correctamente en la tabla: {table_id}")
                except Exception as e:
                    self.log_error(f"Error al cargar datos en la tabla {table_name}: {e}")

    def log_error(self, error_message):
        print(f"Error: {error_message}")
