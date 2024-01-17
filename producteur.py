import time, json, random
import requests
from kafka import KafkaProducer

producteur = KafkaProducer(bootstrap_servers='localhost:9092')

minute = 55
hour = 11

while True:
    response = requests.get(f"https://[API_URL]/?hour={hour}&minute={minute}")
    if response.status_code != 200:
        continue
    
    datas = []
    
    for table_line in response.json:
        if table_line.line not in datas:
            setattr(
                datas,
                table_line.line,
                [
                    table_line.retard
                ]
            )
        else:
            datas[table_line.line].append(table_line.retard)
    
    for line, line_datas in datas.items():
        print(f"Envoi de : {line_datas} ({line})")
        producteur.send("retards", json.dumps(line_datas).encode(), line.encode())
    
    time.sleep(1)
    
    minute += 1
    hour += minute // 60
    minute %= 60