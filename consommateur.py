import json
from statistics import mean
from kafka import KafkaConsumer

mem = {}

consommateur = KafkaConsumer('retards', group_id='triplets-vue', bootstrap_servers='localhost:9092', enable_auto_commit=False)

for msg in consommateur:
    retards = json.loads(msg.value)
    mem[msg.key.decode()] = mean(retards)
    print(mem)
    consommateur.commit()