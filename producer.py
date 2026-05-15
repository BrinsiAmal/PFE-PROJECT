# simulator.py
import json
import time
import random
from datetime import datetime
from elasticsearch import Elasticsearch
from kafka import KafkaProducer
import os

ES_HOST = os.getenv('ES_HOST', 'http://elasticsearch:9200')
KAFKA_HOST = os.getenv('KAFKA_HOST', 'kafka:29092')
SIM_INTERVAL = float(os.getenv('SIM_INTERVAL', 1.0))
SIM_BURST = int(os.getenv('SIM_BURST', 1))

es = Elasticsearch([ES_HOST])

# Kafka Producer
kafka_producer = KafkaProducer(
    bootstrap_servers=[KAFKA_HOST],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_log():
    """Génère un log réaliste pour Attijari Bank"""
    actions = ['LOGIN', 'TRANSFER', 'DEPOSIT', 'WITHDRAWAL', 'BALANCE_CHECK', 'SESSION_EXPIRED']
    error_codes = ['NONE', 'ERR_001', 'ERR_101', 'ERR_201', 'ERR_301', 'ERR_401']
    
    return {
        'timestamp': datetime.utcnow().isoformat(),
        'user_id': f'USER_{random.randint(1000, 2000)}',
        'processus': random.choice(['VIREMENTS', 'DEPOTS', 'RETRAITS', 'CONSULTATIONS']),
        'action': random.choice(actions),
        'error_code': random.choice(error_codes),
        'success': str(random.choice([True, False, True, True])),
        'duree_action_sec': round(random.uniform(0.5, 45), 2),
        'ip': f'192.168.{random.randint(0, 255)}.{random.randint(0, 255)}',
        'user_agent': random.choice(['Firefox', 'Chrome', 'Safari', 'Edge']),
        'message': 'Operation completed successfully'
    }

def main():
    print("🚀 Simulator started with Kafka support...")
    print(f"   Elasticsearch: {ES_HOST}")
    print(f"   Kafka: {KAFKA_HOST}")
    
    try:
        while True:
            for _ in range(SIM_BURST):
                log = generate_log()
                
                # ✅ Envoyer à Elasticsearch
                es.index(index='attijari-logs', document=log)
                
                # ✅ Envoyer à Kafka
                kafka_producer.send('attijari-logs', value=log)
                
                print(f"✅ Log créé: {log['action']} | {log['processus']} | {log['error_code']}")
            
            time.sleep(SIM_INTERVAL)
    
    except KeyboardInterrupt:
        print("\n⛔ Simulator arrêté")
    finally:
        kafka_producer.close()

if __name__ == '__main__':
    main()