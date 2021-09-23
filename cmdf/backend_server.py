from flask import Flask , jsonify, request, make_response, send_file, send_from_directory, g
import traceback
import base64
import time
import io
import pandas as pd
import json
import config

from flask_cors import CORS
from Predictor import Predictor
from utils import DataProcessor
from datetime import datetime

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
cors = CORS(app, resources={r"/*": {"origins": "http://localhost:3000"}})

@app.errorhandler(Exception)
def handle_default_error(error):
    traceback.print_exc()
    return {
        'code': 'INTERNAL_SERVER_ERROR',
        'message': 'Something went wrong.'
    }, 500

@app.route('/')
def home():
    return "CMDF Project", 200

"""
@app.route('/predict', methods=["GET"])
def predict():
    json_data = { key: value for key, value in request.args.items() }
    df = Predictor.predict(*json_data)
    df = df.reset_index()
    df["time"] = df["time"].apply(lambda v: v.strftime("%H%M%S"))
    return df.to_dict(), 200
"""

@app.route('/ticker/<working_date>', methods=['GET'])
def ticker(working_date):
    result = []
    with open('../date_ticker.txt') as json_file:
        data = json.load(json_file)
        working_date = DataProcessor.change_date_format(working_date)
        for date,ticker in data.items():
            if date == working_date:
                for i in range(len(ticker)):
                    res = {"ticker":ticker[i]}
                    result.append(res)
        return jsonify(result)
        
@app.route('/data', methods=['POST'])
def predict():
    submission_list = request.get_json()
    output = []
    for submission in submission_list:
        _id = submission["id"]
        ticker = submission["ticker"].upper()
        volume = int(submission["volume"].replace(',', ''))
        date = submission["date"]
        start_time = date + "-" + submission["start_time"] + ":00"
        end_time = date + "-" + submission["end_time"] + ":00"
        side = submission["side"]

        # predictor should return table in form of list of row where each row is dict contain following data like this
        # {'time': '10:05', 'volume': '2900.0', 'price': '68.0', 'otype': 'LO'},
        # and predictor should return marketVWAP too
        table , vwap = Predictor.predict(ticker, volume, start_time, end_time, side)

        avg = DataProcessor.avg_cal(table)
        diff = (avg - vwap)  / vwap * 100
        print(avg,vwap)
        print(diff)
        # table = DataProcessor.change_table_format(table)
        csv_format = DataProcessor.str_csv_format(ticker,date,side,volume,avg,vwap,diff,table)

        table = DataProcessor.change_table_format(table)
        #print(csv_format)
        box = {
            "id" : _id, 
            "ticker" : ticker,
            "order_volume" : format(volume, ","),
            "no_order" : str(len(table)),
            "avg" : format(avg, '.2f'),
            "result" : table,
            "vwap" : format(vwap, '.2f'),
            "diff" : format(diff, '.2f'),
            "side" : side,
            "csv_format" : csv_format
        }
        output.append(box)
    return jsonify(output)
           
if __name__ == "__main__":
    app.run(
    host=config.backend_hostname,
    port=config.backend_port,
    debug=config.debug,
)