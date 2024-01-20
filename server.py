import pandas as pd
from flask import Flask, request, jsonify


# Read the CSV file
nasbas_data = pd.read_csv("./NASBA.csv", sep=";")
json_data_all = nasbas_data.to_json(orient='records')

app = Flask(__name__)

@app.route('/api/all')
def all():
    return jsonify(json_data_all)

@app.route('/api/', methods=['GET'])
def get_data():

    #day = date_hour[:10]
    #hour = date_hour[11:13]
    #minutes = date_hour[14:16]
    day = '2023-12-01'
    hour = request.args.get('hour')
    minutes = request.args.get('minute')

    hour = hour.zfill(2) if hour is not None else None
    minutes = minutes.zfill(2) if minutes is not None else None


    dataframe=nasbas_data[(nasbas_data["mdate"].str[:10] == day) & (nasbas_data["mdate"].str[11:13] == hour) & (nasbas_data["mdate"].str[14:16] == minutes)]

    json_data = dataframe.to_json(orient='records')

    return json_data

if __name__ == '__main__':
    app.run(debug=True, port=5001)
