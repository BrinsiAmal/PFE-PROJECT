"""
Module de boucle de feedback — Mesure d'impact des robots RPA
Compare les KPIs avant vs après exécution des robots
Stocke l'historique des décisions + ROI dans Elasticsearch
"""
import os, json, time, logging
from datetime import datetime, timedelta
from collections import defaultdict
from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)

ES_HOST = os.environ.get('ES_HOST', 'http://localhost:9200')
INDEX_IMPACT = 'attijari-robots-impact'
INDEX_LOGS = 'attijari-logs'
INDEX_ROBOTS = 'attijari-robots-logs'

class ImpactTracker:
    def __init__(self, es=None):
        self.es = es or Elasticsearch(ES_HOST)
        self._ensure_index()

    def _ensure_index(self):
        if not self.es.indices.exists(index=INDEX_IMPACT):
            self.es.indices.create(index=INDEX_IMPACT, mappings={
                "properties": {
                    "decision_id": {"type": "keyword"},
                    "regle_id": {"type": "keyword"},
                    "robot": {"type": "keyword"},
                    "processus": {"type": "keyword"},
                    "type_robot": {"type": "keyword"},
                    "statut": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "resolved_at": {"type": "date"},
                    "resolved": {"type": "boolean"},
                    "baseline": {
                        "properties": {
                            "taux_echec_pct": {"type": "float"},
                            "nb_occurrences": {"type": "integer"},
                            "duree_moy_sec": {"type": "float"},
                            "periode_heures": {"type": "integer"}
                        }
                    },
                    "impact": {
                        "properties": {
                            "taux_echec_avant": {"type": "float"},
                            "taux_echec_apres": {"type": "float"},
                            "reduction_echec_pct": {"type": "float"},
                            "temps_economise_min": {"type": "float"},
                            "cout_economise_tnd": {"type": "float"}
                        }
                    }
                }
            })
            logger.info(f"Index '{INDEX_IMPACT}' créé")

    def get_baseline(self, processus=None, user_id=None, error_code=None, heures=24):
        """Calcule les métriques avant intervention"""
        must = [{"range": {"timestamp": {"gte": f"now-{heures}h/h", "lte": "now"}}}]
        if processus: must.append({"term": {"processus": processus}})
        if user_id: must.append({"term": {"user_id": user_id}})
        if error_code: must.append({"term": {"error_code": error_code}})

        try:
            res = self.es.search(index=INDEX_LOGS, size=0, query={"bool": {"must": must}},
                aggs={
                    "taux_echec": {"avg": {"field": "success"}},
                    "nb_occurrences": {"value_count": {"field": "timestamp"}},
                    "duree_moy": {"avg": {"field": "duree_action_sec"}}
                })
            aggs = res.get("aggregations", {})
            taux_succes = float(aggs.get("taux_echec", {}).get("value") or 1.0)
            return {
                "taux_echec_pct": round((1 - taux_succes) * 100, 1),
                "nb_occurrences": int(aggs.get("nb_occurrences", {}).get("value", 0)),
                "duree_moy_sec": round(aggs.get("duree_moy", {}).get("value", 0) or 0, 1),
                "periode_heures": heures
            }
        except Exception as e:
            logger.warning(f"get_baseline error: {e}")
            return {"taux_echec_pct": 0, "nb_occurrences": 0, "duree_moy_sec": 0, "periode_heures": heures}

    def record_decision(self, decision):
        """Enregistre une décision avec baseline"""
        baseline = self.get_baseline(
            processus=decision.get("processus"),
            user_id=decision.get("user_id"),
            error_code=decision.get("error_code")
        )
        doc = {
            "decision_id": f"IMP-{int(time.time())}-{decision.get('regle_id', 'R000')}",
            "regle_id": decision.get("regle_id"),
            "robot": decision.get("robot"),
            "processus": decision.get("processus"),
            "type_robot": decision.get("type_robot"),
            "statut": "EXECUTING",
            "created_at": datetime.now().isoformat(),
            "resolved": False,
            "baseline": baseline,
            "impact": {},
            "source": decision.get("source", "n8n")
        }
        try:
            self.es.index(index=INDEX_IMPACT, document=doc, id=doc["decision_id"])
            logger.info(f"Décision enregistrée: {doc['decision_id']}")
        except Exception as e:
            logger.error(f"record_decision error: {e}")
        return doc["decision_id"]

    def resolve_decision(self, decision_id, gain_temps_min=0, gain_cout_tnd=0):
        """Mesure l'impact après exécution du robot"""
        try:
            doc = self.es.get(index=INDEX_IMPACT, id=decision_id).get("_source", {})
        except:
            logger.warning(f"Décision {decision_id} non trouvée")
            return None

        apres = self.get_baseline(
            processus=doc.get("processus"),
            heures=24
        )
        avant = doc.get("baseline", {})
        avant_taux = avant.get("taux_echec_pct", 0)
        apres_taux = apres.get("taux_echec_pct", 0)
        reduction = max(0, avant_taux - apres_taux)

        impact = {
            "taux_echec_avant": avant_taux,
            "taux_echec_apres": apres_taux,
            "reduction_echec_pct": round(reduction, 1),
            "temps_economise_min": gain_temps_min,
            "cout_economise_tnd": gain_cout_tnd,
            "roi_annuel_tnd": round(gain_cout_tnd * 12, 2)
        }

        try:
            self.es.update(index=INDEX_IMPACT, id=decision_id, body={
                "doc": {
                    "statut": "RESOLVED",
                    "resolved": True,
                    "resolved_at": datetime.now().isoformat(),
                    "impact": impact,
                    "after": apres
                }
            })
            logger.info(f"Impact enregistré pour {decision_id}: -{reduction}% échecs")
        except Exception as e:
            logger.error(f"resolve_decision error: {e}")

        return impact

    def get_summary(self):
        """Résumé global de l'impact"""
        try:
            res = self.es.search(index=INDEX_IMPACT, size=0,
                aggs={
                    "total": {"value_count": {"field": "decision_id"}},
                    "resolues": {"filter": {"term": {"resolved": True}}},
                    "par_type": {"terms": {"field": "type_robot", "size": 10}},
                    "par_regle": {"terms": {"field": "regle_id", "size": 10}},
                    "total_temps": {"sum": {"field": "impact.temps_economise_min"}},
                    "total_cout": {"sum": {"field": "impact.cout_economise_tnd"}},
                    "avg_reduction": {"avg": {"field": "impact.reduction_echec_pct"}}
                })
            aggs = res.get("aggregations", {})
            total = aggs.get("total", {}).get("value", 0)
            resolues = aggs.get("resolues", {}).get("doc_count", 0)
            return {
                "total_decisions": int(total),
                "resolues": int(resolues),
                "taux_resolution_pct": round(resolues / total * 100, 1) if total else 0,
                "par_type": [{"label": b["key"], "value": b["doc_count"]}
                            for b in aggs.get("par_type", {}).get("buckets", [])],
                "par_regle": [{"label": b["key"], "value": b["doc_count"]}
                            for b in aggs.get("par_regle", {}).get("buckets", [])],
                "temps_economise_min": round(aggs.get("total_temps", {}).get("value", 0) or 0, 1),
                "cout_economise_tnd": round(aggs.get("total_cout", {}).get("value", 0) or 0, 2),
                "reduction_moyenne_pct": round(aggs.get("avg_reduction", {}).get("value", 0) or 0, 1),
                "roi_annuel_tnd": round((aggs.get("total_cout", {}).get("value", 0) or 0) * 12, 2)
            }
        except Exception as e:
            logger.error(f"get_summary error: {e}")
            return {}

    def get_recent_decisions(self, limit=50):
        """Dernières décisions avec impact"""
        try:
            res = self.es.search(index=INDEX_IMPACT, size=limit,
                sort=[{"created_at": {"order": "desc"}}])
            return [
                {**h["_source"], "id": h["_id"]}
                for h in res.get("hits", {}).get("hits", [])
            ]
        except Exception as e:
            logger.error(f"get_recent_decisions error: {e}")
            return []

    def get_timeline(self, jours=7):
        """Données pour timeline des impacts"""
        depuis = (datetime.now() - timedelta(days=jours)).isoformat()
        try:
            res = self.es.search(index=INDEX_IMPACT, size=200,
                query={"range": {"created_at": {"gte": depuis}}},
                sort=[{"created_at": {"order": "asc"}}],
                _source=["created_at", "type_robot", "resolved", "impact.reduction_echec_pct"])
            return [h["_source"] for h in res.get("hits", {}).get("hits", [])]
        except Exception as e:
            logger.error(f"get_timeline error: {e}")
            return []

    def get_evolution(self, jours=7):
        """Évolution des KPIs sur N jours pour le dashboard"""
        data = []
        for i in range(jours - 1, -1, -1):
            jour = datetime.now() - timedelta(days=i)
            debut = jour.replace(hour=0, minute=0, second=0).isoformat()
            fin = jour.replace(hour=23, minute=59, second=59).isoformat()
            try:
                res = self.es.search(index=INDEX_LOGS, size=0,
                    query={"range": {"timestamp": {"gte": debut, "lte": fin}}},
                    aggs={"taux_succes": {"avg": {"field": "success"}}})
                taux = res.get("aggregations", {}).get("taux_succes", {}).get("value", 0) or 0
            except:
                taux = 0
            data.append({
                "date": jour.strftime("%Y-%m-%d"),
                "taux_succes_pct": round(taux * 100, 1)
            })
        return data
