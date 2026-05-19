from flask import Flask, jsonify
from flask_cors import CORS
from pymongo import MongoClient

app = Flask(__name__)
CORS(app)

client = MongoClient("mongodb://localhost:27017/")

db = client["kaltim_flood"]
collection = db["weather_data"]

@app.route("/api/weather")
def weather():

    data = list(collection.find({}, {"_id": 0}))

    return jsonify(data)

if __name__ == "__main__":
    app.run(debug=True)