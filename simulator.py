import csv, random, time, os, json
from datetime import datetime
from elasticsearch import Elasticsearch
 
CSV_FILE = 'attijari_bank_logs_10000.csv'
ES_HOST  = os.environ.get('ES_HOST', 'http://localhost:9200')
INTERVAL = float(os.environ.get('SIM_INTERVAL', '1.0'))   # secondes entre chaque log
BURST    = int(os.environ.get('SIM_BURST', '1'))           # logs par intervalle
 
USERS     = [f'U{i:03d}' for i in range(1, 21)]
AGENCES   = ['Tunis-Centre','Sfax','Sousse','Bizerte','Nabeul','Monastir','Gabes','Kairouan','Medenine','Gafsa']
APPS      = ['CoreBanking','CRM','RiskManager','ComplianceApp']
PROCESSUS = ['VIREMENT','OUVERTURE_COMPTE','CREDIT','DEPOT','RETRAIT','CONSULTATION','CLOTURE','MISE_A_JOUR','VALIDATION','REPORTING']
ACTIONS   = ['LOGIN','LOGOUT','FILL_FIELD','SUBMIT_FORM','SESSION_EXPIRED','ERROR_VALIDATION','SEARCH','PRINT','UPLOAD','DOWNLOAD','APPROVE','REJECT']
ERRORS    = ['ERR_001','ERR_101','ERR_201','ERR_301','ERR_401','ERR_501','ERR_601','ERR_701','ERR_901']
FIELDS    = ['montant','iban','nom_client','date_naissance','adresse']
SCREENS   = ['login','dashboard','form_virement','form_credit','reporting','client_detail']
HEADERS   = ['timestamp','user_id','agence','application','processus','action',
             'duree_action_sec','success','error_code','field_modified','screen']
 
# ── Anomalies injectées aléatoirement pour rendre la démo réaliste
ANOMALY_USERS = ['U005','U012','U018']  # utilisateurs "suspects"
 
def gen_log():
    action   = random.choice(ACTIONS)
    user     = random.choice(USERS)
    anomaly  = user in ANOMALY_USERS
 
    # Augmenter taux d'échec pour utilisateurs suspects
    fail_prob = 0.65 if anomaly else 0.25
    has_error = random.random() < fail_prob
 
    error   = random.choice(ERRORS) if has_error else 'NONE'
    success = not has_error or random.random() > 0.8
 
    # Durée anormale pour goulots
    duree = round(random.uniform(25.0, 60.0) if anomaly and random.random() < 0.3
                  else random.uniform(0.5, 20.0), 2)
 
    return {
        'timestamp'       : datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
        'user_id'         : user,
        'agence'          : random.choice(AGENCES),
        'application'     : random.choice(APPS),
        'processus'       : random.choice(PROCESSUS),
        'action'          : action,
        'duree_action_sec': duree,
        'success'         : success,
        'error_code'      : error,
        'field_modified'  : random.choice(FIELDS) if action == 'FILL_FIELD' else 'NONE',
        'screen'          : random.choice(SCREENS),
    }
 
def init_csv():
    """Créer CSV avec headers si inexistant"""
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            csv.DictWriter(f, fieldnames=HEADERS, delimiter=';').writeheader()
        print(f'✅ {CSV_FILE} créé')
    else:
        # Compter lignes existantes
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            count = sum(1 for _ in f) - 1
        print(f'📋 {CSV_FILE} existant — {count} logs')
 
def connect_es():
    try:
        es = Elasticsearch(ES_HOST)
        if es.ping():
            print(f'✅ Elasticsearch connecté : {ES_HOST}')
            return es
    except Exception:
        pass
    print('⚠️  ES non disponible — écriture CSV seulement')
    return None
 
def write_log(row, es):
    # CSV
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
        csv.DictWriter(f, fieldnames=HEADERS, delimiter=';').writerow(row)
 
    # Elasticsearch (optionnel)
    if es:
        try:
            doc = {**row, 'heure': int(row['timestamp'][11:13])}
            es.index(index='attijari-logs', document=doc)
        except Exception:
            pass
 
def main():
    print("""
╔══════════════════════════════════════════════╗
║  🔄  Attijari Bank — Simulateur Logs        ║
║  Intervalle : {:.1f}s | Burst : {}             ║
╚══════════════════════════════════════════════╝
    """.format(INTERVAL, BURST))
 
    init_csv()
    es = connect_es()
 
    total = 0
    t0    = time.time()
    try:
        while True:
            for _ in range(BURST):
                row = gen_log()
                write_log(row, es)
                total += 1
 
            elapsed = time.time() - t0
            rate    = total / elapsed
            print(f'\r  ✍️  {total:6d} logs écrits | {rate:.1f} log/s | dernier: {row["user_id"]} {row["action"]} {"✅" if row["success"] else "❌"}', end='', flush=True)
            time.sleep(INTERVAL)
 
    except KeyboardInterrupt:
        print(f'\n\n✅ Simulation arrêtée — {total} logs générés')
 
if __name__ == '__main__':
    main()