#!/usr/bin/env python3
"""
Producer - Génère les logs Attijari Bank
Envoie vers: Elasticsearch + Kafka
"""
import json
import time
import random
from datetime import datetime
from elasticsearch import Elasticsearch
from kafka import KafkaProducer
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
ES_HOST = os.getenv('ES_HOST', 'http://elasticsearch:9200')
KAFKA_HOST = os.getenv('KAFKA_HOST', 'kafka:29092')
SIM_INTERVAL = float(os.getenv('SIM_INTERVAL', 1.0))
SIM_BURST = int(os.getenv('SIM_BURST', 5))

# Elasticsearch client
try:
    es = Elasticsearch(
        [ES_HOST],
        request_timeout=10,
        max_retries=3,
        retry_on_timeout=True
    )
    if es.ping():
        logger.info(f"✅ Elasticsearch connecté: {ES_HOST}")
    else:
        logger.warning(f"⚠️  Elasticsearch non accessible: {ES_HOST}")
except Exception as e:
    logger.error(f"❌ Erreur Elasticsearch: {e}")
    es = None

# Kafka Producer
try:
    kafka_producer = KafkaProducer(
        bootstrap_servers=[KAFKA_HOST],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3,
        request_timeout_ms=5000
    )
    logger.info(f"✅ Kafka connecté: {KAFKA_HOST}")
except Exception as e:
    logger.error(f"❌ Erreur Kafka: {e}")
    kafka_producer = None

# Données réalistes
ACTIONS = ['LOGIN', 'TRANSFER', 'DEPOSIT', 'WITHDRAWAL', 'BALANCE_CHECK', 'SESSION_EXPIRED']
PROCESSUS = ['VIREMENTS', 'DEPOTS', 'RETRAITS', 'CONSULTATIONS']
ERROR_CODES = ['NONE', 'ERR_001', 'ERR_101', 'ERR_201', 'ERR_301', 'ERR_401', 'ERR_501', 'ERR_601', 'ERR_701', 'ERR_901']
USER_AGENTS = ['Firefox', 'Chrome', 'Safari', 'Edge', 'Mobile']

def generate_log():
    """Génère un log réaliste pour Attijari Bank"""
    # Créer des patterns de défaillance réalistes
    user_id = f'USER_{random.randint(1000, 2000)}'
    
    # 30% de chance d'échec pour certains users
    success = random.choices([True, False], weights=[70, 30])[0]
    
    return {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'user_id': user_id,
        'agence': f'AGENCE_{random.randint(100, 150)}',
        'processus': random.choice(PROCESSUS),
        'action': random.choice(ACTIONS),
        'error_code': 'NONE' if success else random.choice(ERROR_CODES[1:]),
        'success': success,
        'duree_action_sec': round(random.uniform(0.5, 120), 2),
        'ip': f'192.168.{random.randint(0, 255)}.{random.randint(0, 255)}',
        'user_agent': random.choice(USER_AGENTS),
        'message': 'Operation completed successfully' if success else 'Operation failed'
    }

def index_to_elasticsearch(log):
    """Index le log dans Elasticsearch"""
    if es is None:
        return False
    
    try:
        es.index(
            index='attijari-logs',
            document=log
        )
        return True
    except Exception as e:
        logger.error(f"❌ Erreur indexation ES: {e}")
        return False

def send_to_kafka(log):
    """Envoie le log à Kafka"""
    if kafka_producer is None:
        return False
    
    try:
        kafka_producer.send('attijari-logs', value=log)
        return True
    except Exception as e:
        logger.error(f"❌ Erreur Kafka: {e}")
        return False

def main():
    logger.info("=" * 70)
    logger.info("  PRODUCER - GÉNÉRATEUR DE LOGS ATTIJARI BANK")
    logger.info("=" * 70)
    logger.info(f"  📊 Elasticsearch: {ES_HOST}")
    logger.info(f"  🚀 Kafka: {KAFKA_HOST}")
    logger.info(f"  ⏱️  Intervalle: {SIM_INTERVAL}s")
    logger.info(f"  📦 Burst: {SIM_BURST} logs par envoi")
    logger.info("=" * 70)
    
    log_count = 0
    es_count = 0
    kafka_count = 0
    
    try:
        while True:
            for _ in range(SIM_BURST):
                log = generate_log()
                log_count += 1
                
                # Envoyer à Elasticsearch
                if index_to_elasticsearch(log):
                    es_count += 1
                
                # Envoyer à Kafka
                if send_to_kafka(log):
                    kafka_count += 1
                
                # Log formaté
                status = "✅" if log['success'] else "❌"
                logger.info(
                    f"{status} #{log_count:05d} | "
                    f"{log['action']:20} | "
                    f"{log['processus']:15} | "
                    f"{log['user_id']:12} | "
                    f"Durée: {log['duree_action_sec']:6.2f}s"
                )
            
            # Afficher stats
            if log_count % 50 == 0:
                logger.info(
                    f"\n📈 STATS: Total={log_count} | ES={es_count} | Kafka={kafka_count}\n"
                )
            
            time.sleep(SIM_INTERVAL)
    
    except KeyboardInterrupt:
        logger.info("\n⛔ Producer arrêté par l'utilisateur")
    except Exception as e:
        logger.error(f"\n❌ Erreur critique: {e}")
    finally:
        if kafka_producer:
            kafka_producer.close()
        logger.info(f"✅ Arrêt gracieux - Total logs générés: {log_count}")

if __name__ == '__main__':
    main()
