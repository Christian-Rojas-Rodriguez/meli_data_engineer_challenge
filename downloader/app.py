# downloader/app.py
from flask import Flask, jsonify
from downloader import MLDataFetcher

app = Flask(__name__)
data_fetcher = MLDataFetcher(query="chromecast")

@app.route('/download', methods=['GET'])
def download_data():
    try:
        data = data_fetcher.fetch_data()
        data_fetcher.save_data(data, filename="data.json")
        return jsonify({"status": "success", "data_count": len(data)}), 200
    except Exception as e:
        data_fetcher.log_error(str(e))
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(port=5001)
