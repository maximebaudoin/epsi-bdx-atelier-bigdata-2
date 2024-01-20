import time
import json
import random
import requests
from kafka import KafkaProducer

producteur = KafkaProducer(bootstrap_servers='localhost:9092')

minute = 55
hour = 11

while True:
    response = requests.get(f"http://127.0.0.1:5001/api/?hour={hour}&minute={minute}")
    if response.status_code != 200:
        continue
    
    datas = {}
    
    for table_line in response.json():
        if str(table_line["rs_sv_ligne_a"]) not in datas:
            datas[str(table_line["rs_sv_ligne_a"])] = [table_line["retard"]]
        else:
            datas[str(table_line["rs_sv_ligne_a"])].append(table_line["retard"])
    
    for line, line_datas in datas.items():
        print(f"Envoi de : {line_datas} ({line})")
        producteur.send("retards", json.dumps(line_datas).encode(), line.encode())

    time.sleep(1)

    minute += 1
    hour += minute // 60
    minute %= 60
