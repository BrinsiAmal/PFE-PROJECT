"""
Application Flask — Attijari Bank
Elasticsearch 8.x — API sans body= (déprécié)
Fallback CSV si ES non disponible
"""
from flask import Flask, jsonify, render_template
from elasticsearch import Elasticsearch
import json, os, csv
from datetime import datetime
from collections import defaultdict

app     = Flask(__name__)
ES_HOST = os.environ.get('ES_HOST', 'http://localhost:9200')

# ✅ CORRIGÉ : ajout request_timeout + retry_on_timeout
es = Elasticsearch(
    ES_HOST,
    request_timeout=10,
    retry_on_timeout=True,
    max_retries=3
)

# ═══════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════

def es_ok():
    try:
        return es.ping()
    except Exception:
        return False

def load_json(path):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

# ── Cache CSV en mémoire
_CSV_CACHE = None

def get_csv():
    global _CSV_CACHE
    if _CSV_CACHE is not None:
        return _CSV_CACHE
    path = 'attijari_bank_logs_10000.csv'
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            row['success'] = row.get('success', '').strip() == 'True'
            try:    row['duree_action_sec'] = float(row.get('duree_action_sec') or 0)
            except: row['duree_action_sec'] = 0.0
            row['error_code'] = row.get('error_code', '') or 'NONE'
            ts = row.get('timestamp', '')
            try:    row['heure'] = int(ts[11:13]) if len(ts) >= 13 else 0
            except: row['heure'] = 0
            rows.append(row)
    _CSV_CACHE = rows
    return rows

# ═══════════════════════════════════════════
#  PAGE
# ═══════════════════════════════════════════

@app.route('/')
def index():
    return render_template('index.html')

# ═══════════════════════════════════════════
#  STATUS
# ═══════════════════════════════════════════

@app.route('/api/status')
def api_status():
    ok, indices = es_ok(), []
    if ok:
        try:
            cat     = es.cat.indices(format='json')
            indices = [
                {'index': i['index'], 'docs': i.get('docs.count', '0')}
                for i in cat if 'attijari' in i['index']
            ]
        except Exception as e:
            print(f'[status] {e}')
    return jsonify({
        'elasticsearch' : ok,
        'es_host'       : ES_HOST,
        'indices'       : indices,
        'csv_loaded'    : len(get_csv()),
        'timestamp'     : datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    })

# ═══════════════════════════════════════════
#  KPIs
# ═══════════════════════════════════════════

@app.route('/api/kpis')
def api_kpis():
    p2 = load_json('resultats_phase2.json')
    p3 = load_json('decisions_phase3.json')
    total = succes = robots_total = 0
    duree_moy = 0.0

    if es_ok():
        try:
            total = es.count(index='attijari-logs')['count']
            succes = es.count(
                index='attijari-logs',
                query={'term': {'success': True}}
            )['count']
            r = es.search(
                index='attijari-logs',
                size=0,
                aggs={'d': {'avg': {'field': 'duree_action_sec'}}}
            )
            duree_moy = round(r['aggregations']['d']['value'] or 0, 2)
            try:
                robots_total = es.count(index='attijari-robots-logs')['count']
            except Exception:
                robots_total = 0
        except Exception as e:
            print(f'[kpis ES] {e}')
            rows      = get_csv()
            total     = len(rows)
            succes    = sum(1 for r in rows if r['success'])
            durees    = [r['duree_action_sec'] for r in rows if r['duree_action_sec'] > 0]
            duree_moy = round(sum(durees) / len(durees), 2) if durees else 0.0
    else:
        rows      = get_csv()
        total     = len(rows)
        succes    = sum(1 for r in rows if r['success'])
        durees    = [r['duree_action_sec'] for r in rows if r['duree_action_sec'] > 0]
        duree_moy = round(sum(durees) / len(durees), 2) if durees else 0.0

    echecs    = total - succes
    decisions = p3.get('decisions', [])
    opps      = p2.get('opportunites', [])

    return jsonify({
        'total_logs'      : total,
        'succes'          : succes,
        'echecs'          : echecs,
        'taux_succes_pct' : round(succes / total * 100, 1) if total else 0,
        'taux_echec_pct'  : round(echecs / total * 100, 1) if total else 0,
        'duree_moy_sec'   : duree_moy,
        'robots_total'    : robots_total,
        'repetitions'     : len(p2.get('repetitions', [])),
        'erreurs'         : len(p2.get('erreurs', [])),
        'goulots'         : len(p2.get('goulots', [])),
        'anomalies'       : len(p2.get('anomalies', [])),
        'opportunites'    : len(opps),
        'decisions_rpa'   : len(decisions),
        'gain_min'        : sum(d.get('gain_temps_min', 0) for d in decisions),
        'gain_cout'       : round(sum(o.get('gain_cout_tnd', 0) for o in opps), 2),
    })

# ═══════════════════════════════════════════
#  AGENCES
# ═══════════════════════════════════════════

@app.route('/api/logs/agences')
def api_agences():
    if es_ok():
        try:
            res = es.search(
                index='attijari-logs',
                size=0,
                aggs={
                    'par_agence': {
                        'terms': {'field': 'agence', 'size': 20},
                        'aggs' : {'echecs': {'filter': {'term': {'success': False}}}}
                    }
                }
            )
            data = []
            for b in res['aggregations']['par_agence']['buckets']:
                t = b['doc_count']; e = b['echecs']['doc_count']
                data.append({
                    'agence'         : b['key'],
                    'total'          : t,
                    'echecs'         : e,
                    'taux_echec_pct' : round(e / t * 100, 1) if t else 0
                })
            return jsonify(sorted(data, key=lambda x: -x['taux_echec_pct']))
        except Exception as e:
            print(f'[agences ES] {e}')

    rows    = get_csv()
    agences = defaultdict(lambda: {'total': 0, 'echecs': 0})
    for r in rows:
        ag = r.get('agence', 'N/A')
        agences[ag]['total'] += 1
        if not r['success']:
            agences[ag]['echecs'] += 1
    data = [
        {'agence': ag, 'total': v['total'], 'echecs': v['echecs'],
         'taux_echec_pct': round(v['echecs'] / v['total'] * 100, 1) if v['total'] else 0}
        for ag, v in agences.items()
    ]
    return jsonify(sorted(data, key=lambda x: -x['taux_echec_pct']))

# ═══════════════════════════════════════════
#  HEURES
# ═══════════════════════════════════════════

@app.route('/api/logs/heures')
def api_heures():
    if es_ok():
        try:
            res = es.search(
                index='attijari-logs',
                size=0,
                aggs={
                    'par_heure': {
                        'terms': {'field': 'heure', 'size': 24, 'order': {'_key': 'asc'}},
                        'aggs' : {'echecs': {'filter': {'term': {'success': False}}}}
                    }
                }
            )
            return jsonify([
                {'heure': int(b['key']), 'total': b['doc_count'], 'echecs': b['echecs']['doc_count']}
                for b in res['aggregations']['par_heure']['buckets']
            ])
        except Exception as e:
            print(f'[heures ES] {e}')

    rows   = get_csv()
    heures = defaultdict(lambda: {'total': 0, 'echecs': 0})
    for r in rows:
        h = r.get('heure', 0)
        heures[h]['total'] += 1
        if not r['success']:
            heures[h]['echecs'] += 1
    return jsonify([
        {'heure': h, 'total': v['total'], 'echecs': v['echecs']}
        for h, v in sorted(heures.items())
    ])

# ═══════════════════════════════════════════
#  ERREURS
# ═══════════════════════════════════════════

@app.route('/api/logs/erreurs')
def api_erreurs():
    if es_ok():
        try:
            res = es.search(
                index='attijari-logs',
                size=0,
                query={'bool': {'must_not': [{'term': {'error_code': 'NONE'}}]}},
                aggs={'top_erreurs': {'terms': {'field': 'error_code', 'size': 10}}}
            )
            return jsonify([
                {'code': b['key'], 'count': b['doc_count']}
                for b in res['aggregations']['top_erreurs']['buckets']
            ])
        except Exception as e:
            print(f'[erreurs ES] {e}')

    rows   = get_csv()
    counts = defaultdict(int)
    for r in rows:
        ec = r.get('error_code', 'NONE')
        if ec and ec != 'NONE':
            counts[ec] += 1
    top = sorted(counts.items(), key=lambda x: -x[1])[:10]
    return jsonify([{'code': k, 'count': v} for k, v in top])

# ═══════════════════════════════════════════
#  ACTIONS ÉCHOUÉES
# ═══════════════════════════════════════════

@app.route('/api/logs/actions_echouees')
def api_actions():
    if es_ok():
        try:
            res = es.search(
                index='attijari-logs',
                size=0,
                query={'term': {'success': False}},
                aggs={'top_actions': {'terms': {'field': 'action', 'size': 10}}}
            )
            return jsonify([
                {'action': b['key'], 'count': b['doc_count']}
                for b in res['aggregations']['top_actions']['buckets']
            ])
        except Exception as e:
            print(f'[actions ES] {e}')

    rows   = get_csv()
    counts = defaultdict(int)
    for r in rows:
        if not r['success']:
            counts[r.get('action', 'N/A')] += 1
    top = sorted(counts.items(), key=lambda x: -x[1])[:10]
    return jsonify([{'action': k, 'count': v} for k, v in top])

# ═══════════════════════════════════════════
#  ANOMALIES
# ═══════════════════════════════════════════

@app.route('/api/anomalies')
def api_anomalies():
    p2        = load_json('resultats_phase2.json')
    anomalies = p2.get('anomalies', [])
    uc = defaultdict(int)
    ac = defaultdict(int)
    for a in anomalies:
        uc[a.get('user_id', 'N/A')] += 1
        ac[a.get('agence',  'N/A')] += 1
    return jsonify({
        'total'      : len(anomalies),
        'critiques'  : len([a for a in anomalies if a.get('impact') == 'CRITIQUE']),
        'hauts'      : len([a for a in anomalies if a.get('impact') == 'HAUT']),
        'top_users'  : [{'user': u, 'count': c} for u, c in sorted(uc.items(), key=lambda x: -x[1])[:10]],
        'top_agences': [{'agence': a, 'count': c} for a, c in sorted(ac.items(), key=lambda x: -x[1])[:10]],
        'liste'      : sorted(anomalies, key=lambda x: x.get('score_anomalie', 0))
    })

# ═══════════════════════════════════════════
#  DÉCISIONS
# ═══════════════════════════════════════════

@app.route('/api/decisions')
def api_decisions():
    p3        = load_json('decisions_phase3.json')
    decisions = p3.get('decisions', [])
    pt = defaultdict(int)
    pr = defaultdict(int)
    ps = defaultdict(int)
    pp = defaultdict(int)
    for d in decisions:
        pt[d.get('type_robot', 'AUTRE')] += 1
        pr[d.get('robot',      'N/A')]   += 1
        ps[d.get('source',     'N/A')]   += 1
        pp[str(d.get('priorite', 0))]    += 1
    return jsonify({
        'total'     : len(decisions),
        'par_type'  : [{'label': k, 'value': v} for k, v in pt.items()],
        'par_robot' : [{'label': k, 'value': v} for k, v in sorted(pr.items(), key=lambda x: -x[1])],
        'par_source': [{'label': k, 'value': v} for k, v in ps.items()],
        'par_prio'  : [{'label': f'Priorité {k}', 'value': v} for k, v in sorted(pp.items())],
        'liste'     : sorted(decisions, key=lambda x: (x.get('priorite', 9), -x.get('nb_occurrences', 0)))
    })

# ═══════════════════════════════════════════
#  OPPORTUNITÉS
# ═══════════════════════════════════════════

@app.route('/api/opportunites')
def api_opportunites():
    p2   = load_json('resultats_phase2.json')
    opps = p2.get('opportunites', [])
    pc   = defaultdict(int)
    for o in opps:
        pc[o.get('categorie', 'AUTRE')] += 1
    return jsonify({
        'total'     : len(opps),
        'gain_total': round(sum(o.get('gain_temps_min', 0) for o in opps), 1),
        'cout_total': round(sum(o.get('gain_cout_tnd',  0) for o in opps), 2),
        'roi_annuel': round(sum(o.get('gain_cout_tnd',  0) for o in opps) * 12, 0),
        'par_cat'   : [{'label': k, 'value': v} for k, v in pc.items()],
        'liste'     : opps
    })

# ═══════════════════════════════════════════
#  ALERTES
# ═══════════════════════════════════════════

@app.route('/api/alertes')
def api_alertes():
    p2        = load_json('resultats_phase2.json')
    p3        = load_json('decisions_phase3.json')
    anomalies = p2.get('anomalies', [])
    erreurs   = p2.get('erreurs',   [])
    decisions = p3.get('decisions', [])
    alertes   = []

    for a in anomalies:
        if a.get('impact') == 'CRITIQUE':
            alertes.append({
                'id'       : f"ALT-{len(alertes)+1:04d}",
                'niveau'   : 'CRITIQUE',
                'type'     : 'ANOMALIE',
                'titre'    : f"Comportement anormal — {a.get('user_id','N/A')}",
                'message'  : f"{a.get('user_id')} | {a.get('processus')} | {a.get('action')} | Score: {a.get('score_anomalie')}",
                'agence'   : a.get('agence', 'N/A'),
                'robot'    : 'RobotAlerteAnomalie',
                'timestamp': a.get('timestamp', datetime.now().isoformat())
            })

    for e in erreurs:
        if e.get('impact') == 'CRITIQUE':
            alertes.append({
                'id'       : f"ALT-{len(alertes)+1:04d}",
                'niveau'   : 'HAUTE',
                'type'     : 'ERREUR',
                'titre'    : f"Erreur critique — {e.get('error_code')} | {e.get('processus')}",
                'message'  : f"{e.get('description')} : {e.get('nb_occurrences')} occurrences",
                'agence'   : 'MULTI-AGENCE',
                'robot'    : f"RobotCorrection_{e.get('error_code')}",
                'timestamp': datetime.now().isoformat()
            })

    for d in decisions:
        if d.get('priorite') == 1 and d.get('type_robot') == 'ANOMALIE':
            alertes.append({
                'id'       : f"ALT-{len(alertes)+1:04d}",
                'niveau'   : 'HAUTE',
                'type'     : 'DECISION_RPA',
                'titre'    : f"Intervention urgente — {d.get('robot')}",
                'message'  : d.get('description', f"{d.get('robot')} sur {d.get('processus')}"),
                'agence'   : d.get('agence', 'N/A'),
                'robot'    : d.get('robot',  'N/A'),
                'timestamp': d.get('timestamp', datetime.now().isoformat())
            })

    return jsonify({
        'total'    : len(alertes),
        'critiques': len([a for a in alertes if a['niveau'] == 'CRITIQUE']),
        'hautes'   : len([a for a in alertes if a['niveau'] == 'HAUTE']),
        'liste'    : alertes
    })

# ═══════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════

if __name__ == '__main__':
    print("""
╔══════════════════════════════════════════════╗
║  🏦  Attijari Bank — Système RPA            ║
║  Flask + Elasticsearch 8.x                  ║
║  http://localhost:5000                      ║
╚══════════════════════════════════════════════╝
    """)
    print(f'  ES_HOST       : {ES_HOST}')
    ok = es_ok()
    print(f'  Elasticsearch : {"✅ Connecté" if ok else "⚠️  Non disponible (fallback CSV)"}')
    print(f'  CSV           : {len(get_csv())} lignes chargées\n')
    app.run(debug=True, host='0.0.0.0', port=5000)
