from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import psycopg2
import configparser
from collections import defaultdict
from flask_compress import Compress
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.realpath(__file__))

# Build the full path to the application.properties file
config_path = os.path.join(script_dir, 'application.properties')

config = configparser.ConfigParser()
config.read(config_path)

host = config.get('db','host')
port = config.get('db', 'port')
user = config.get('db','user')
passwd = config.get('db','passwd')
auto_db = config.get('db','auto_db')
DATAFILE_DIR = config.get('datafile','location')

app = Flask(__name__)
CORS(app)
Compress(app)

@app.route('/api/get_cars', methods=['GET'])
def get_cars():
    try:
        conn = psycopg2.connect(
            host=host,
            database=auto_db,
            user=user,
            password=passwd)
        cursor = conn.cursor()
        cursor.execute('SELECT vin, "Model Year" AS model_year, "Make" AS make, "Model" AS model, '
                       'CASE WHEN "Trim" = \'Not Applicable\' AND "Series" = \'Not Applicable\' THEN NULL '
                       'ELSE TRIM(CONCAT(CASE WHEN "Series" = \'Not Applicable\' THEN \'\' ELSE "Series" END, \' \', '
                       'CASE WHEN "Trim" = \'Not Applicable\' THEN \'\' ELSE "Trim" END)) '
                       'END AS series_trim, auction_date, lot_number, '
                       'state, lienholder_name, borough, location_order '
                       'FROM v_auction_list '
                       'WHERE auction_date >= CURRENT_DATE '
                       'ORDER BY auction_date, borough, location_order , lot_number;')

        columns = [x[0] for x in cursor.description]
        rows = cursor.fetchall()

        grouped_data = defaultdict(list)
        for result in rows:
            record = dict(zip(columns, result))
            global_key = (record['auction_date'], record['borough'], record['location_order'])
            grouped_data[global_key].append(record)

        optimized_data = []
        for global_key, records in grouped_data.items():
            global_attributes = {
                "auction_date": global_key[0],
                "borough": global_key[1],
                "location_order": global_key[2]
            }

            transformed_records = {}
            for key in records[0].keys():
                if key not in global_attributes:
                    transformed_records[key] = [item[key] for item in records]

            group_data = {
                "global": global_attributes,
                "records": transformed_records
            }
            optimized_data.append(group_data)

        cursor.close()
        conn.close()
        return jsonify(optimized_data)
    except Exception as e:
        return str(e), 500

@app.route('/api/get_json', methods=['GET'])
def serve_json():
    if DATAFILE_DIR:
        file_path = os.path.join(DATAFILE_DIR, 'output.json')
        return send_file(file_path, mimetype='application/json')
    else:
        return "Datafile location not found.", 500

if __name__ == '__main__':
    app.run()