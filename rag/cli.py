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
    return code


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
    sp.add_parser("servir")
    ajouter_cli_banc(sp)
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
        if a.cmd == "graphe":
            for src in config.sources:
                if not a.source or src.nom in a.source:
                    print(f"{src.nom} : "
                          + maj_graphe(conn, src, lecteur_pour(src, reglages), reglages,
                                       forcer=True))
            return 0
        if a.cmd == "banc":
            return commande_banc(a, conn, reglages, config, modeles, _indexer)
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
