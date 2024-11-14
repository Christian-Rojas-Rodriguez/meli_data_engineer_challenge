import requests
import json


class MLDataFetcher:
    def __init__(self, query, limit=50, total_results=500):
        self.query = query
        self.limit = limit
        self.total_results = total_results
        self.base_url = "https://api.mercadolibre.com/sites/MLA/search"

    def fetch_data(self):
        all_data = []
        for offset in range(0, self.total_results, self.limit):
            try:
                data = self._fetch_batch(offset)
                all_data.extend(data)
            except Exception as e:
                self.log_error(str(e))
        return all_data

    def _fetch_batch(self, offset):
        params = {"q": self.query, "limit": self.limit, "offset": offset}
        response = requests.get(self.base_url, params=params)
        response.raise_for_status()  # Levanta una excepción en caso de error HTTP
        return response.json().get("results", [])

    @staticmethod
    def save_data(data, filename="data.json"):
        with open(filename, "w") as f:
            json.dump(data, f, indent=4)

    @staticmethod
    def log_error(error_message):
        print(f"Error: {error_message}")
