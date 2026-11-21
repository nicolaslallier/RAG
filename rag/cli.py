"""CLI `rag` : migrer, indexer, chercher, servir."""

import argparse
import sys
import time
from dataclasses import asdict

from rag.banc.execution import ajouter_cli_banc, commande_banc
from rag.config import PUBLIC, TOUT, Reglages, charger_config, empreinte_indexation
from rag.db import connexion, migrer
from rag.graphe import maj_graphe
from rag.indexeur import CoffrePartielError, indexer_source
from rag.modeles import modeles_depuis
from rag.recherche import Filtres, chercher
from rag.sources import ErreurSource, lecteur_pour


def _indexer(conn, reglages, config, modeles, noms=None, complet=False, forcer=False) -> int:
    code = 0
    for src in config.sources:
        if noms and src.nom not in noms:
            continue
        debut = time.monotonic()
        lecteur = lecteur_pour(src, reglages)
        try:
            b = indexer_source(conn, src, lecteur, config.indexation, modeles,
                               forcer=forcer, sans_repos=complet)
        except CoffrePartielError as e:
            print(f"ERREUR {e}", file=sys.stderr)
            return 2
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
    ajouter_cli_banc(sp)
    ps = sp.add_parser("banc-synthetique")
    ps.add_argument("--n", type=int, default=250)
    ps.add_argument("--graine", type=int, default=1)
    ps.add_argument("--source", action="append")
    a = p.parse_args(argv)

    reglages = Reglages()
    if a.cmd == "servir":
        import uvicorn

        from rag.api import create_app
        uvicorn.run(create_app(reglages), host="0.0.0.0", port=8000)
        return 0
    config = charger_config(reglages.config)
    with connexion(reglages.database_url) as conn:
        if a.cmd == "migrer":
            print("\n".join(migrer(conn)) or "rien à migrer")
            return 0
        modeles = modeles_depuis(reglages)
        if a.cmd == "indexer":
            return _indexer(conn, reglages, config, modeles, a.source, a.complet, a.forcer)
        if a.cmd in ("doublons", "orphelines", "liens-entrants"):
            return _entretien_cli(conn, a, config)
        if a.cmd in ("chemin", "expliquer"):
            return _graphe_cli(conn, a)
        if a.cmd == "graphe":
            for src in config.sources:
                if not a.source or src.nom in a.source:
                    print(f"{src.nom} : "
                          + maj_graphe(conn, src, lecteur_pour(src, reglages), reglages,
                                       forcer=True))
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
            return commande_synthetique(a, conn, config, modeles)
        from rag.api import version_id
        v = version_id(conn, empreinte_indexation(config.indexation))
        if v is None:
            print("aucun index pour cette config : lancer `rag indexer`", file=sys.stderr)
            return 1
        f = Filtres(sources=a.source, dossier=a.dossier, tags=a.tag, type=a.type)
        for i, r in enumerate(chercher(conn, a.question, config.recherche, modeles, v,
                                       config.indexation.modele_embedding,
                                       niveaux=TOUT if a.prive else PUBLIC, filtres=f,
                                       limite=a.limite), 1):
            print(f"{i:>2}  {r.score:.4f}  cos={r.cosinus:.3f}  {r.cle}  [{r.fil}]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
