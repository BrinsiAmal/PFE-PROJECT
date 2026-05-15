# app.py
import json
from flask import Flask, render_template, jsonify
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
from threading import Thread
import os

app = Flask(__name__)
ES_HOST = os.getenv('ES_HOST', 'http://elasticsearch:9200')
KAFKA_HOST = os.getenv('KAFKA_HOST', 'kafka:29092')

es = Elasticsearch([ES_HOST])
latest_logs = []

def kafka_consumer_thread():
    """Consomme les logs de Kafka et met à jour latest_logs"""
    try:
        consumer = KafkaConsumer(
            'attijari-logs',
            bootstrap_servers=[KAFKA_HOST],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='flask-consumer'
        )
        
        print("🎯 Kafka Consumer démarré...")
        for message in consumer:
            log = message.value
            latest_logs.insert(0, log)  # Insérer au début pour ordre inversé
            if len(latest_logs) > 100:  # Garder seulement les 100 derniers
                latest_logs.pop()
    except Exception as e:
        print(f"❌ Erreur Kafka: {e}")

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/logs')
def get_logs():
    """API pour récupérer les logs en temps réel (depuis Kafka)"""
    return jsonify(latest_logs[:50])

@app.route('/api/stats')
def get_stats():
    """Statistiques depuis Elasticsearch"""
    try:
        response = es.search(index='attijari-logs', size=1000)
        hits = response['hits']['hits']
        
        stats = {
            'total_logs': response['hits']['total']['value'],
            'actions': {},
            'error_distribution': {}
        }
        
        for hit in hits:
            log = hit['_source']
            action = log.get('action', 'UNKNOWN')
            error = log.get('error_code', 'NONE')
            
            stats['actions'][action] = stats['actions'].get(action, 0) + 1
            stats['error_distribution'][error] = stats['error_distribution'].get(error, 0) + 1
        
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Démarrer le consumer Kafka en arrière-plan
    consumer_thread = Thread(target=kafka_consumer_thread, daemon=True)
    consumer_thread.start()
    
    app.run(host='0.0.0.0', port=5000, debug=True)