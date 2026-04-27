import os
import pandas as pd
from elasticsearch import Elasticsearch, helpers
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ CORRIGÉ : utilise la variable d'environnement ES_HOST
ES_HOST  = os.environ.get('ES_HOST', 'http://localhost:9200')
INDEX    = "attijari-logs"
CSV_FILE = "attijari_bank_logs_10000.csv"

logger.info(f"Connexion à Elasticsearch : {ES_HOST}")
es = Elasticsearch(ES_HOST, request_timeout=30, retry_on_timeout=True)

# Vérifier la connexion
if not es.ping():
    logger.error(f"❌ Impossible de se connecter à {ES_HOST}")
    logger.error("Assurez-vous qu'Elasticsearch est démarré : docker-compose up -d elasticsearch")
    exit(1)

logger.info("✅ Connecté à Elasticsearch")

# Supprimer et recréer l'index avec mapping complet
if es.indices.exists(index=INDEX):
    es.indices.delete(index=INDEX)
    logger.info(f"Index '{INDEX}' supprimé.")

es.indices.create(index=INDEX, mappings={
    "properties": {
        "timestamp"       : {"type": "date"},
        "user_id"         : {"type": "keyword"},
        "agence"          : {"type": "keyword"},
        "application"     : {"type": "keyword"},
        "processus"       : {"type": "keyword"},
        "action"          : {"type": "keyword"},
        "duree_action_sec": {"type": "float"},
        "success"         : {"type": "boolean"},
        "error_code"      : {"type": "keyword"},
        "field_modified"  : {"type": "keyword"},
        "screen"          : {"type": "keyword"},
        "log_text_brut"   : {"type": "text"},
        "log_text_lemme"  : {"type": "text"},
        "heure"           : {"type": "integer"}
    }
})
logger.info(f"Index '{INDEX}' créé avec mapping.")

# Charger le CSV
df = pd.read_csv(CSV_FILE, sep=";")
df['success']        = df['success'].astype(str).str.strip().map({"True": True, "False": False})
df['timestamp']      = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%dT%H:%M:%S')
df['error_code']     = df['error_code'].fillna("NONE")
df['field_modified'] = df['field_modified'].fillna("NONE")
df['heure']          = pd.to_datetime(df['timestamp']).dt.hour

# Texte brut pour NLP
df['log_text_brut'] = df.apply(lambda r:
    f"action {r['action']} processus {r['processus']} "
    f"application {r['application']} agence {r['agence']} "
    f"erreur {r['error_code']} durée {int(r['duree_action_sec'])} secondes "
    f"{'échec' if not r['success'] else 'succès'}",
    axis=1
)

# Indexation en bulk
docs = [{"_index": INDEX, "_source": row.to_dict()} for _, row in df.iterrows()]
ok, errors = helpers.bulk(es, docs, raise_on_error=False)
logger.info(f"✅ {ok} documents indexés dans '{INDEX}'. Erreurs: {len(errors)}")
