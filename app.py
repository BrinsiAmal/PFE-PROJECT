from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
from elasticsearch import Elasticsearch
from datetime import datetime
import json, os, csv
from collections import defaultdict
import queue

app = Flask(__name__)
CORS(app)
ES_HOST = os.environ.get('ES_HOST', 'http://localhost:9200')
es = Elasticsearch([ES_HOST])

# Queue pour anomalies et décisions temps réel
anomalies_queue = queue.Queue(maxsize=1000)
decisions_queue = queue.Queue(maxsize=1000)

# ═══════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════

def es_ok():
    try:    return es.ping()
    except: return False

def load_json(path):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def get_csv():
    path = 'attijari_bank_logs_10000.csv'
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f, delimiter=';'):
            row['success'] = row.get('success', '').strip() == 'True'
            try:    row['duree_action_sec'] = float(row.get('duree_action_sec') or 0)
            except: row['duree_action_sec'] = 0.0
            row['error_code'] = row.get('error_code', '') or 'NONE'
            ts = row.get('timestamp', '')
            try:    row['heure'] = int(ts[11:13]) if len(ts) >= 13 else 0
            except: row['heure'] = 0
            rows.append(row)
    return rows

# ═══════════════════════════════════════════
# PAGE
# ═══════════════════════════════════════════

@app.route('/')
def index():
    return render_template('index.html')

# ═══════════════════════════════════════════
# WEBHOOKS - Réception Consumer/n8n
# ═══════════════════════════════════════════

@app.route('/webhook/anomaly', methods=['POST'])
def webhook_anomaly():
    """Webhook reçoit les anomalies du CONSUMER"""
    try:
        data = request.get_json()
        
        if 'timestamp' not in data:
            data['timestamp'] = datetime.utcnow().isoformat() + 'Z'
        
        # Sauvegarder dans Elasticsearch
        if es_ok():
            es.index(index='attijari-anomalies', document=data)
        
        # Ajouter à queue pour SSE
        anomalies_queue.put(data)
        
        print(f"✅ [WEBHOOK] Anomalie reçue: {data.get('regle_id')} | {data.get('user_id')} | {data.get('taux_echec_pct')}%")
        
        return jsonify({
            'status': 'success',
            'message': 'Anomalie enregistrée',
            'id': data.get('regle_id')
        }), 201
    
    except Exception as e:
        print(f"❌ [WEBHOOK] Erreur anomalie: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 400

@app.route('/webhook/decision', methods=['POST'])
def webhook_decision():
    """Webhook reçoit les décisions du n8n"""
    try:
        data = request.get_json()
        
        if 'timestamp' not in data:
            data['timestamp'] = datetime.utcnow().isoformat() + 'Z'
        
        if es_ok():
            es.index(index='attijari-robots-logs', document=data)
        
        decisions_queue.put(data)
        
        print(f"✅ [WEBHOOK] Décision reçue: {data.get('robot')} | {data.get('regle_id')}")
        
        return jsonify({
            'status': 'success',
            'message': 'Décision enregistrée',
            'robot': data.get('robot')
        }), 201
    
    except Exception as e:
        print(f"❌ [WEBHOOK] Erreur décision: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 400

# ═══════════════════════════════════════════
# SSE - Anomalies Temps Réel
# ═══════════════════════════════════════════

@app.route('/api/anomalies/stream')
def anomalies_stream():
    """Stream SSE pour anomalies temps réel"""
    def generate():
        try:
            if es_ok():
                response = es.search(
                    index='attijari-anomalies',
                    size=50,
                    sort=[{'timestamp': {'order': 'desc'}}]
                )
                for hit in response['hits']['hits']:
                    anomaly = hit['_source']
                    yield f"data: {json.dumps(anomaly)}\n\n"
        except:
            pass
        
        while True:
            try:
                anomaly = anomalies_queue.get(timeout=30)
                yield f"data: {json.dumps(anomaly)}\n\n"
            except:
                yield f": keep-alive\n\n"
    
    return app.response_class(
        generate(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive'
        }
    )

@app.route('/api/decisions/stream')
def decisions_stream():
    """Stream SSE pour décisions temps réel"""
    def generate():
        try:
            if es_ok():
                response = es.search(
                    index='attijari-robots-logs',
                    size=50,
                    sort=[{'timestamp': {'order': 'desc'}}]
                )
                for hit in response['hits']['hits']:
                    decision = hit['_source']
                    yield f"data: {json.dumps(decision)}\n\n"
        except:
            pass
        
        while True:
            try:
                decision = decisions_queue.get(timeout=30)
                yield f"data: {json.dumps(decision)}\n\n"
            except:
                yield f": keep-alive\n\n"
    
    return app.response_class(
        generate(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no'
        }
    )

# ═══════════════════════════════════════════
# API - Anomalies
# ═══════════════════════════════════════════

@app.route('/api/anomalies')
def api_anomalies():
    """Retourne toutes les anomalies"""
    if es_ok():
        try:
            response = es.search(
                index='attijari-anomalies',
                size=1000,
                sort=[{'timestamp': {'order': 'desc'}}]
            )
            anomalies = [hit['_source'] for hit in response['hits']['hits']]
        except:
            anomalies = []
    else:
        p2 = load_json('resultats_phase2.json')
        anomalies = p2.get('anomalies', [])
    
    uc, ac = defaultdict(int), defaultdict(int)
    for a in anomalies:
        uc[a.get('user_id', 'N/A')] += 1
        ac[a.get('agence', 'N/A')] += 1
    
    return jsonify({
        'total': len(anomalies),
        'critiques': len([a for a in anomalies if a.get('taux_echec_pct', 0) > 75]),
        'hautes': len([a for a in anomalies if 50 <= a.get('taux_echec_pct', 0) <= 75]),
        'top_users': [{'user': u, 'count': c} for u, c in sorted(uc.items(), key=lambda x: -x[1])[:10]],
        'top_agences': [{'agence': a, 'count': c} for a, c in sorted(ac.items(), key=lambda x: -x[1])[:10]],
        'liste': sorted(anomalies, key=lambda x: x.get('timestamp', ''), reverse=True)[:100]
    })

@app.route('/api/decisions')
def api_decisions():
    """Retourne toutes les décisions"""
    if es_ok():
        try:
            response = es.search(
                index='attijari-robots-logs',
                size=1000,
                sort=[{'timestamp': {'order': 'desc'}}]
            )
            decisions = [hit['_source'] for hit in response['hits']['hits']]
        except:
            decisions = []
    else:
        p3 = load_json('decisions_phase3.json')
        decisions = p3.get('decisions', [])
    
    pt, pr, ps, pp = defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int)
    for d in decisions:
        pt[d.get('type_robot', 'AUTRE')] += 1
        pr[d.get('robot', 'N/A')] += 1
        ps[d.get('source', 'N/A')] += 1
        pp[str(d.get('priority', 0))] += 1
    
    return jsonify({
        'total': len(decisions),
        'par_type': [{'label': k, 'value': v} for k, v in pt.items()],
        'par_robot': [{'label': k, 'value': v} for k, v in sorted(pr.items(), key=lambda x: -x[1])],
        'par_source': [{'label': k, 'value': v} for k, v in ps.items()],
        'par_prio': [{'label': f'P{k}', 'value': v} for k, v in sorted(pp.items())],
        'liste': sorted(decisions, key=lambda x: (x.get('priority', 9), -x.get('nb_occurrences', 0)))
    })

# ═══════════════════════════════════════════
# STATUS & KPIs
# ═══════════════════════════════════════════

@app.route('/api/status')
def api_status():
    ok, indices = es_ok(), []
    if ok:
        try:
            cat = es.cat.indices(format='json')
            indices = [
                {'index': i['index'], 'docs': i.get('docs.count', '0'), 'size': i.get('store.size', '0')}
                for i in cat if i['index'] in ['attijari-anomalies', 'attijari-robots-logs']
            ]
        except:
            pass    