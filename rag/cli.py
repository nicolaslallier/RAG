"""CLI `rag` : migrer, indexer, chercher, servir, exclusions, sources."""

import argparse
import sys
import time
from dataclasses import asdict
from pathlib import Path

from rag.banc.execution import ajouter_cli_banc, commande_banc
from rag.config import PUBLIC, TOUT, Reglages, charger_config, empreinte_indexation
from rag.db import connexion, migrer
from rag.graphe import maj_graphe
from rag.indexeur import CoffrePartielError, SourceSupprimee, indexer_source
from rag.modeles import modeles_depuis
from rag.recherche import Filtres, chercher
from rag.rerank import reranker_depuis
from rag.sources import ErreurSource, lecteur_pour
from rag.sources_gerees import coffret_depuis, sources_actives


def _indexer(conn, reglages, config, modeles, noms=None, complet=False, forcer=False) -> int:
    from rag.travailleur import VERROU
    if not conn.execute("SELECT pg_try_advisory_lock(%s) AS ok", (VERROU,)).fetchone()["ok"]:
        conn.commit()
        print("une passe d'indexation est en cours (travailleur) : réessayer plus tard",
              file=sys.stderr)
        return 3
    try:
        return _indexer_verrouille(conn, reglages, config, modeles, noms, complet, forcer)
    finally:
        conn.rollback()
        conn.execute("SELECT pg_advisory_unlock(%s)", (VERROU,))
        conn.commit()


def _indexer_verrouille(conn, reglages, config, modeles, noms, complet, forcer) -> int:
    code = 0
    actives = sources_actives(conn, config, coffret_depuis(reglages))
    conn.commit()
    for active in actives:
        src = active.conf
        if noms and src.nom not in noms:
            continue
        debut = time.monotonic()
        lecteur = lecteur_pour(src, reglages, active.jeton, active.utilisateur)
        try:
            b = indexer_source(conn, src, lecteur, config.indexation, modeles,
                               forcer=forcer, sans_repos=complet)
        except CoffrePartielError as e:
            print(f"ERREUR {e}", file=sys.stderr)
            return 2
        except SourceSupprimee:
            print(f"{src.nom} : source supprimée, ignorée", file=sys.stderr)
            continue
        except ErreurSource as e:  # un dépôt injoignable n'arrête pas les autres
            print(f"ERREUR {src.nom} : {e}", file=sys.stderr)
            code = 1
            continue
        print(f"{src.nom} : " + ", ".join(f"{v} {k}" for k, v in asdict(b).items())
              + f" ({time.monotonic() - debut:.0f} s)")
        print(f"{src.nom} : {maj_graphe(conn, src, lecteur, reglages)}")
    conn.execute("ANALYZE morceaux, documents, noeuds, liens")  # sinon l'index HNSW est ignoré
    conn.commit()
    return code


def _candidats(cands) -> None:
    for c in cands:
        print(f"  id:{c['id']}  {c['libelle']}  ({c['type']}, {c['source']}:{c['chemin']})")


def _graphe_cli(conn, a) -> int:
    from rag.graphe import chemin, expliquer
    niveaux = TOUT if a.prive else PUBLIC
    if a.cmd == "chemin":
        r = chemin(conn, a.de, a.vers, niveaux=niveaux)
        print(r["etat"])
        if r["etat"] in ("ambigu", "introuvable"):
            for cote in ("a", "b"):
                if len(r["candidats"][cote]) != 1:
                    print(f"{a.de if cote == 'a' else a.vers} :")
                    _candidats(r["candidats"][cote])
        for e in r["etapes"]:
            if lk := e["lien"]:
                print(f"    {'->' if lk['sens'] == 'sortant' else '<-'} {lk['type']} "
                      f"[{lk['confiance']}]")
            print(f"  {e['noeud']['libelle']}  ({e['noeud']['source']}:{e['noeud']['chemin']})")
        return 0
    r = expliquer(conn, a.nom, niveaux=niveaux)
    print(r["etat"])
    if r["etat"] != "ok":
        _candidats(r["candidats"])
        return 0
    n = r["noeud"]
    print(f"{n['libelle']} ({n['type']}) — {n['lien']}")
    if r["communaute"]:
        c = r["communaute"]
        print(f"communauté {c['id']} ({c['taille']}) : {', '.join(c['membres'])}")
    print(f"{r['n_voisins']} voisins :")
    for v in r["voisins"]:
        print(f"  {'->' if v['sens'] == 'sortant' else '<-'} {v['lien_type']} [{v['confiance']}]"
              f"  {v['libelle']}  ({v['source']}:{v['chemin']})")
    return 0


def _entretien_cli(conn, a, config) -> int:
    from rag.api import version_id
    from rag.entretien import doublons, liens_entrants, orphelines
    niveaux = TOUT if a.prive else PUBLIC
    if a.cmd == "doublons":
        v = version_id(conn, empreinte_indexation(config.indexation))
        for d in doublons(conn, v, a.seuil, niveaux=niveaux) if v else []:
            print(f"{d['similarite']:.3f}  {d['a']['cle']}  ==  {d['b']['cle']}")
    elif a.cmd == "orphelines":
        for o in orphelines(conn, niveaux=niveaux):
            print(o["cle"])
    else:
        r = liens_entrants(conn, a.cle, niveaux=niveaux)
        print(r["etat"])
        for e in r["entrants"]:
            print(f"  {e['cle']}")
    return 0


def _afficher_liste(r: dict) -> None:
    print(f"{r['mode']} : {r['total']} élément(s)")
    for e in r["elements"]:
        marque = "" if e["certitude"] == "tague" else "  (probable, non tagué)"
        print(f"  - {e['titre']}{marque}\n      {e['lien']}"
              + (f"\n      origine : {e['origine']}" if e["origine"] else ""))


def _afficher_reponse(r) -> None:
    print(r.texte + (f"\n(refus : {r.raison})" if r.refus else ""))
    for s in r.sources:
        if s["cite"] or r.refus:
            print(f"  [{s['n']}] {s['titre']} — {s['lien']}"
                  + (f" (origine : {s['origine']})" if s["origine"] else ""))


def _question_cli(conn, a, config, modeles, reglages, v) -> int:
    from rag.aiguillage import aiguiller
    from rag.lister import lister, taxonomie
    from rag.reponse import repondre
    from rag.rerank import reranker_depuis
    niveaux = TOUT if a.prive else PUBLIC
    rr = reranker_depuis(config, reglages)
    f = None
    if a.type or a.tag or a.source:
        f = Filtres(sources=a.source, tags=a.tag, type=a.type)
    mode = a.cmd if a.cmd in ("lister", "repondre") else a.mode
    debut = time.monotonic()
    dec = aiguiller(a.question, taxonomie(conn, niveaux=niveaux), modeles,
                    config.reponse.modele, mode, f)
    filtres = ", ".join(f"{k}={x}" for k, x in asdict(dec.filtres).items() if x)
    print(f"mode : {dec.mode} (par {dec.par}) ; filtres : {filtres or 'aucun'}")
    if dec.bonus_sources:
        print(f"bonus de source : {', '.join(dec.bonus_sources)}")
    if dec.mode == "lister":
        _afficher_liste(lister(conn, a.question, dec.filtres, config, modeles, v,
                               niveaux=niveaux, reranker=rr))
    else:
        _afficher_reponse(repondre(conn, a.question, config, modeles, v, niveaux=niveaux,
                                   filtres=dec.filtres, reranker=rr,
                                   bonus_sources=dec.bonus_sources))
    print(f"({time.monotonic() - debut:.1f} s)")
    return 0


def _exclusions_cli(conn, a) -> int:
    from rag.gestion import MotifInvalide, exporter, importer
    if a.action == "exporter":
        print(exporter(conn), end="")
        return 0
    try:
        texte = Path(a.fichier).read_text(encoding="utf-8")
    except UnicodeDecodeError:
        print("fichier illisible, rien d'importé : UTF-8 attendu", file=sys.stderr)
        return 1
    except OSError as e:
        print(f"fichier illisible, rien d'importé : {e.strerror or e}", file=sys.stderr)
        return 1
    try:
        r = importer(conn, texte)
    except MotifInvalide as e:
        print(f"fichier invalide, rien d'importé : {e}", file=sys.stderr)
        return 1
    print(f"{r['ajoutees']} ajoutée(s), {r['ignorees']} ignorée(s), "
          f"{r['retires']} document(s) retiré(s)", file=sys.stderr)
    return 0


def _sources_cli(conn, a, config) -> int:
    from rag.sources_gerees import SourceInvalide, exporter, importer
    if a.action == "exporter":
        print(exporter(conn), end="")
        return 0
    if a.action == "lister":
        for r in conn.execute("SELECT * FROM sources WHERE geree ORDER BY nom"):
            jeton = "jeton" if r["jeton_chiffre"] is not None else "—"
            print(f"{r['nom']}\t{r['emplacement']}\t{r['branche'] or 'main'}\t"
                  f"{r['visibilite']}\t{jeton}")
        return 0
    try:
        texte = Path(a.fichier).read_text(encoding="utf-8")
    except UnicodeDecodeError:
        print("fichier illisible, rien d'importé : UTF-8 attendu", file=sys.stderr)
        return 1
    except OSError as e:
        print(f"fichier illisible, rien d'importé : {e.strerror or e}", file=sys.stderr)
        return 1
    try:
        r = importer(conn, texte, noms_reserves={s.nom for s in config.sources})
    except SourceInvalide as e:
        print(f"fichier invalide, rien d'importé : {e}", file=sys.stderr)
        return 1
    print(f"{r['ajoutees']} ajoutée(s), {r['modifiees']} modifiée(s), "
          f"{r['inchangees']} inchangée(s)", file=sys.stderr)
    for n in r["niveaux"]:
        print(f"niveau changé : {n['nom']} ({n['de']} → {n['vers']})", file=sys.stderr)
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="rag")
    sp = p.add_subparsers(dest="cmd", required=True)
    sp.add_parser("migrer")
    pi = sp.add_parser("indexer")
    pi.add_argument("--source", action="append")
    pi.add_argument("--complet", action="store_true")
    pi.add_argument("--forcer", action="store_true")
    pc = sp.add_parser("chercher")
    pc.add_argument("question")
    pc.add_argument("--type")
    pc.add_argument("--tag", action="append")
    pc.add_argument("--source", action="append")
    pc.add_argument("--dossier")
    pc.add_argument("--limite", type=int, default=10)
    pc.add_argument("--prive", action="store_true")
    pg = sp.add_parser("graphe")
    pg.add_argument("--source", action="append")
    pch = sp.add_parser("chemin")
    pch.add_argument("de")
    pch.add_argument("vers")
    pch.add_argument("--prive", action="store_true")
    pex = sp.add_parser("expliquer")
    pex.add_argument("nom")
    pex.add_argument("--prive", action="store_true")
    pd = sp.add_parser("doublons")
    pd.add_argument("--seuil", type=float, default=0.92)
    pd.add_argument("--prive", action="store_true")
    po = sp.add_parser("orphelines")
    po.add_argument("--prive", action="store_true")
    ple = sp.add_parser("liens-entrants")
    ple.add_argument("cle")
    ple.add_argument("--prive", action="store_true")
    pgs = sp.add_parser("graphe-semantique")
    pgs.add_argument("--source")
    pgs.add_argument("--max", type=int)
    sp.add_parser("servir")
    pexc = sp.add_parser("exclusions").add_subparsers(dest="action", required=True)
    pexc.add_parser("exporter")
    pexc.add_parser("importer").add_argument("fichier")
    psrc = sp.add_parser("sources").add_subparsers(dest="action", required=True)
    psrc.add_parser("lister")
    psrc.add_parser("exporter")
    psrc.add_parser("importer").add_argument("fichier")
    sp.add_parser("travailleur")
    for nom in ("question", "lister", "repondre"):
        pq = sp.add_parser(nom)
        pq.add_argument("question")
        if nom == "question":
            pq.add_argument("--mode", choices=["lister", "repondre"])
        pq.add_argument("--type")
        pq.add_argument("--tag", action="append")
        pq.add_argument("--source", action="append")
        pq.add_argument("--prive", action="store_true")
    ajouter_cli_banc(sp)
    ps = sp.add_parser("banc-synthetique")
    ps.add_argument("--n", type=int, default=250)
    ps.add_argument("--graine", type=int, default=1)
    ps.add_argument("--source", action="append")
    ps.add_argument("--reprendre", action="store_true")
    ps.add_argument("--ecraser", action="store_true")
    a = p.parse_args(argv)

    reglages = Reglages()
    if a.cmd == "servir":
        import uvicorn

        from rag.api import create_app
        from rag.demarrage import verifier_reglages
        verifier_reglages(reglages, "api")
        uvicorn.run(create_app(reglages), host="0.0.0.0", port=8000,
                    access_log=False, proxy_headers=False)
        return 0
    config = charger_config(reglages.config)
    if a.cmd == "travailleur":
        import logging

        from rag.travailleur import boucle
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
        from rag.demarrage import attendre_base, verifier_reglages
        verifier_reglages(reglages, "travailleur")
        # Postgres pas encore prêt : attendre plutôt que boucler en redémarrage.
        attendre_base(lambda: connexion(reglages.database_url), url=reglages.database_url).close()
        boucle(reglages, config, modeles_depuis(reglages))
        return 0
    if a.cmd == "migrer":  # au démarrage du conteneur : attendre Postgres plutôt que redémarrer
        from rag.demarrage import attendre_base
        ouvrir = attendre_base(lambda: connexion(reglages.database_url),
                               url=reglages.database_url)
    else:
        ouvrir = connexion(reglages.database_url)
    with ouvrir as conn:
        if a.cmd == "migrer":
            print("\n".join(migrer(conn)) or "rien à migrer")
            if reglages.sources_amorce:
                from rag.sources_gerees import amorcer
                print(f"{amorcer(conn, reglages.sources_amorce)} source(s) git amorcée(s)")
            return 0
        if a.cmd == "exclusions":
            return _exclusions_cli(conn, a)
        if a.cmd == "sources":
            return _sources_cli(conn, a, config)
        modeles = modeles_depuis(reglages)
        if a.cmd == "indexer":
            return _indexer(conn, reglages, config, modeles, a.source, a.complet, a.forcer)
        if a.cmd in ("doublons", "orphelines", "liens-entrants"):
            return _entretien_cli(conn, a, config)
        if a.cmd in ("chemin", "expliquer"):
            return _graphe_cli(conn, a)
        if a.cmd == "graphe":
            for active in sources_actives(conn, config, coffret_depuis(reglages)):
                src = active.conf
                if not a.source or src.nom in a.source:
                    print(f"{src.nom} : "
                          + maj_graphe(conn, src, lecteur_pour(src, reglages, active.jeton,
                                                               active.utilisateur),
                                       reglages, forcer=True))
                    if src.type == "obsidian":
                        from rag.graphe import charger_wikiliens
                        for chemin, cible in getattr(charger_wikiliens, "non_resolues", []):
                            print(f"  non résolu : [[{cible}]] dans {chemin}")
            return 0
        if a.cmd == "graphe-semantique":
            from rag.semantique import passe_semantique
            debut = time.monotonic()
            b = passe_semantique(conn, config, modeles, a.source, a.max)
            print(", ".join(f"{v} {k}" for k, v in b.items())
                  + f" ({time.monotonic() - debut:.0f} s)")
            return 0
        if a.cmd == "banc":
            return commande_banc(a, conn, reglages, config, modeles, _indexer)
        if a.cmd == "banc-synthetique":
            from rag.banc.synthetique import commande_synthetique
            a.source = a.source or ["coffre-maison"]
            return commande_synthetique(a, conn, config, modeles)
        from rag.api import version_id
        v = version_id(conn, empreinte_indexation(config.indexation))
        if v is None:
            print("aucun index pour cette config : lancer `rag indexer`", file=sys.stderr)
            return 1
        if a.cmd in ("question", "lister", "repondre"):
            return _question_cli(conn, a, config, modeles, reglages, v)
        f = Filtres(sources=a.source, dossier=a.dossier, tags=a.tag, type=a.type)
        for i, r in enumerate(chercher(conn, a.question, config.recherche, modeles, v,
                                       config.indexation.modele_embedding,
                                       niveaux=TOUT if a.prive else PUBLIC, filtres=f,
                                       limite=a.limite,
                                       reranker=reranker_depuis(config, reglages)), 1):
            print(f"{i:>2}  {r.score:.4f}  cos={r.cosinus:.3f}  {r.cle}  [{r.fil}]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
