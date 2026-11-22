import json
import logging
from pathlib import Path
from urllib.parse import quote

import pytest
import yaml
from fastapi.testclient import TestClient

from rag.api import create_app
from rag.config import Reglages
from tests.conftest import URL

HX = {"HX-Request": "true"}


@pytest.fixture
def cfg(tmp_path, index):
    d = yaml.safe_load(Path("config/rag.yaml").read_text())
    d["sources"] = [{"nom": "coffre-maison", "type": "obsidian", "visibilite": "public-maison",
                     "emplacement": str(index.coffre)}]
    f = tmp_path / "rag.yaml"
    f.write_text(yaml.safe_dump(d))
    return f


def faire(cfg, faux_modeles, **k):
    r = Reglages(database_url=URL, litellm_url="http://x", jeton="pub", config=cfg, **k)
    return TestClient(create_app(r, faux_modeles))


@pytest.fixture
def client(cfg, faux_modeles):
    return faire(cfg, faux_modeles, jeton_prive="priv")


def connecter(client, jeton):
    client.cookies.clear()
    r = client.post("/ui/connexion", data={"jeton": jeton}, follow_redirects=False)
    assert r.status_code == 303, r.text
    return client


def test_racine_redirige_vers_ui(client):
    r = client.get("/", follow_redirects=False)
    assert r.status_code == 307 and r.headers["location"] == "/ui/"


def test_ui_sans_cookie_redirige_vers_connexion(client):
    r = client.get("/ui/", follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/ui/connexion"
    client.cookies.set("rag_session", "mauvais", path="/ui")
    r = client.get("/ui/", follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/ui/connexion"


def test_page_de_connexion(client):
    r = client.get("/ui/connexion")
    assert r.status_code == 200 and 'name="jeton"' in r.text
    assert "/ui/statique/htmx.min.js" in r.text and "/ui/statique/rag.css" in r.text


def test_statique_sans_cookie(client):
    r = client.get("/ui/statique/htmx.min.js")
    assert r.status_code == 200 and r.text.startswith("var htmx=")
    assert client.get("/ui/statique/rag.css").status_code == 200


def test_connexion_pose_un_cookie_httponly_strict(client):
    r = client.post("/ui/connexion", data={"jeton": "priv"}, follow_redirects=False)
    c = r.headers["set-cookie"]
    assert r.status_code == 303 and r.headers["location"] == "/ui/"
    assert "rag_session=" in c and "HttpOnly" in c
    assert "SameSite=strict" in c.replace("Strict", "strict")
    assert "Path=/ui" in c and "Secure" not in c


def test_connexion_refusee(client):
    for jeton in ("mauvais", "", "   "):
        r = client.post("/ui/connexion", data={"jeton": jeton}, follow_redirects=False)
        assert r.status_code == 401 and "Jeton refusé." in r.text
        assert "set-cookie" not in r.headers and 'name="jeton"' in r.text


def test_deconnexion_efface_le_cookie(client):
    connecter(client, "pub")
    r = client.post("/ui/deconnexion", follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/ui/connexion"
    assert 'rag_session=""' in r.headers["set-cookie"] and "Path=/ui" in r.headers["set-cookie"]
    assert client.get("/ui/", follow_redirects=False).status_code == 303


def test_le_jeton_n_apparait_jamais_dans_la_page(cfg, faux_modeles):
    # jeton distinctif : « priv » est aussi un préfixe de « prive » et de « privé »
    c = faire(cfg, faux_modeles, jeton_prive="Zq8-secret-jeton")
    connecter(c, "Zq8-secret-jeton")
    r = c.get("/ui/")
    assert r.status_code == 200 and "coffre-maison" in r.text
    assert "Zq8-secret-jeton" not in r.text
    r = c.post("/ui/sources/coffre-maison/reindexer", headers=HX)
    assert r.status_code == 200 and "Zq8-secret-jeton" not in r.text


def test_sources_affiche_les_comptes(client):
    r = connecter(client, "pub").get("/ui/")
    assert "coffre-maison" in r.text and "obsidian" in r.text and ">6<" in r.text


def test_sources_affiche_la_passe_en_cours(client, bd):
    assert "Aucune passe en cours." in connecter(client, "pub").get("/ui/").text
    bd.execute("INSERT INTO passes (genre, debut) VALUES ('demandee', '2026-11-22 10:15+00')")
    bd.commit()
    r = client.get("/ui/")
    assert "Passe en cours : demandee, depuis le 2026-11-22" in r.text


def test_sources_public_ne_voit_pas_prive(client, bd):
    bd.execute("UPDATE sources SET visibilite = 'prive'")
    bd.commit()
    assert "coffre-maison" not in connecter(client, "pub").get("/ui/").text
    assert "coffre-maison" in connecter(client, "priv").get("/ui/").text


def test_boutons_absents_pour_le_jeton_public(client):
    assert "Réindexer" not in connecter(client, "pub").get("/ui/").text
    assert "Réindexer" in connecter(client, "priv").get("/ui/").text


def test_reindexer_exige_l_ecrivain(client, bd):
    url = "/ui/sources/coffre-maison/reindexer"
    r = client.post(url, headers=HX, follow_redirects=False)  # sans cookie : htmx ne suit pas de 303
    assert r.status_code == 401 and r.headers["hx-redirect"] == "/ui/connexion"
    assert connecter(client, "pub").post(url, headers=HX).status_code == 403
    connecter(client, "priv")
    assert client.post(url).status_code == 403
    assert client.post(url, headers={"HX-Request": "false"}).status_code == 403
    assert client.post(url, headers=HX | {"Origin": "http://ailleurs.exemple"}).status_code == 403
    assert client.post(url, headers=HX | {"Origin": "null"}).status_code == 403
    assert bd.execute("SELECT demande_le FROM sources").fetchone()["demande_le"] is None
    bd.rollback()
    r = client.post(url, headers=HX | {"Origin": "http://testserver"})
    assert r.status_code == 200 and r.text.lstrip().startswith("<tr")
    assert "coffre-maison" in r.text and "en attente" in r.text
    assert bd.execute("SELECT demande_le FROM sources").fetchone()["demande_le"] is not None
    bd.rollback()
    assert client.post(url, headers=HX).status_code == 200  # Origin absent : accepté


def test_reindexer_source_inconnue(client):
    connecter(client, "priv")
    assert client.post("/ui/sources/inconnue/reindexer", headers=HX).status_code == 404


def test_sans_jeton_prive_l_interface_est_en_lecture_seule(cfg, faux_modeles):
    c = faire(cfg, faux_modeles)
    assert c.post("/ui/connexion", data={"jeton": "priv"},
                  follow_redirects=False).status_code == 401
    connecter(c, "pub")
    assert "Réindexer" not in c.get("/ui/").text
    assert c.post("/ui/sources/coffre-maison/reindexer", headers=HX).status_code == 403


def test_jeton_absent_des_journaux(cfg, faux_modeles, caplog):
    caplog.set_level(logging.DEBUG)
    c = faire(cfg, faux_modeles, jeton_prive="Zq8-secret-jeton")
    connecter(c, "Zq8-secret-jeton")
    c.get("/ui/")
    c.post("/ui/sources/coffre-maison/reindexer", headers=HX)
    c.cookies.clear()
    c.post("/ui/connexion", data={"jeton": "Zq8-secret-jeton-faux"})
    assert caplog.text and "Zq8-secret-jeton" not in caplog.text


# --- Fichiers d'une source et détail ---------------------------------------------------------

PATE = "02 - Domaines/Cuisine/Recettes/Pâté chinois.md"
BIZARRE = "Notes/C# et ? test.md"


def lien(source, chemin):
    return f"/ui/sources/{source}/fichier?chemin={quote(chemin, safe='')}"


def test_htmx_sans_session_401_et_redirection(client):
    r = client.get("/ui/sources/coffre-maison", headers=HX, follow_redirects=False)
    assert r.status_code == 401 and r.headers["hx-redirect"] == "/ui/connexion"
    assert "<html" not in r.text
    r = client.get("/ui/sources/coffre-maison", follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/ui/connexion"
    assert "hx-redirect" not in r.headers


def test_no_store_sur_page_et_fragment(client, index):
    connecter(client, "pub")
    page = client.get("/ui/sources/coffre-maison")
    frag = client.get("/ui/sources/coffre-maison", headers=HX)
    assert page.status_code == frag.status_code == 200
    assert page.headers["cache-control"] == frag.headers["cache-control"] == "no-store"
    assert client.get("/ui/", follow_redirects=False).headers["cache-control"] == "no-store"
    assert "no-store" not in client.get("/ui/statique/rag.css").headers.get("cache-control", "")


def test_fichiers_pagines(client, index, monkeypatch):
    monkeypatch.setattr("rag.ui.PAR_PAGE", 4)
    connecter(client, "pub")
    p1 = client.get("/ui/sources/coffre-maison")
    p2 = client.get("/ui/sources/coffre-maison?page=2")
    assert p1.status_code == p2.status_code == 200
    assert "Kubernetes.md" in p1.text and "Accueil.md" not in p1.text
    assert "Accueil.md" in p2.text and "Kubernetes.md" not in p2.text
    assert "page 2 sur 2" in p2.text
    assert "page=2" in p1.text and "<html" in p1.text


def test_fichiers_filtre_exclus_auto_avec_raison(client, index_code):
    connecter(client, "pub")
    r = client.get("/ui/sources/depot-test?etat=exclu_auto")
    assert r.status_code == 200
    assert "logo.png" in r.text and "uv.lock" in r.text and "exclu :" in r.text
    assert "a.py" not in r.text
    assert "CLE=xyz" not in r.text and "CLE=xyz" not in client.get("/ui/sources/depot-test").text


def test_fichiers_filtre_contient_en_fragment(client, index):
    connecter(client, "pub")
    r = client.get("/ui/sources/coffre-maison?contient=kube", headers=HX)
    assert r.status_code == 200 and "<html" not in r.text and "<body" not in r.text
    assert "Kubernetes.md" in r.text and "Grafana.md" not in r.text


def test_detail_par_le_lien_de_la_liste(client, index):
    connecter(client, "pub")
    liste = client.get("/ui/sources/coffre-maison").text
    href = lien("coffre-maison", PATE)
    assert f'href="{href}"' in liste
    r = client.get(href)
    assert r.status_code == 200 and "Pâté chinois" in r.text and "Boeuf haché" in r.text
    assert r.headers["cache-control"] == "no-store"


def test_detail_affiche_le_frontmatter_echappe(client, index, bd):
    fm = {"type": "recette", "tags": ["recette", "boeuf"], "note": "<b>gras</b>", "portions": 4}
    bd.execute("UPDATE documents SET frontmatter = %s WHERE chemin = %s", (json.dumps(fm), PATE))
    bd.commit()
    connecter(client, "pub")
    r = client.get(lien("coffre-maison", PATE))
    assert r.status_code == 200 and "<dt>Frontmatter</dt>" in r.text
    assert "<code>tags</code> : recette, boeuf" in r.text
    assert "<code>note</code> : &lt;b&gt;gras&lt;/b&gt;" in r.text and "<b>gras" not in r.text
    assert "<code>portions</code> : 4" in r.text


def test_detail_chemin_avec_diese_et_interrogation(client, index, bd, faux_modeles):
    from pathlib import Path

    from rag.config import SourceConf, charger_config
    from rag.indexeur import indexer_source
    from rag.sources import LecteurDossier

    (index.coffre / BIZARRE).parent.mkdir(parents=True, exist_ok=True)
    (index.coffre / BIZARRE).write_text("# Dièse\n\nContenu du fichier bizarre.\n")
    src = SourceConf(visibilite="public-maison", nom="coffre-maison", type="obsidian",
                     emplacement="x")
    indexer_source(bd, src, LecteurDossier(index.coffre),
                   charger_config(Path("config/rag.yaml")).indexation, faux_modeles)
    connecter(client, "pub")
    href = lien("coffre-maison", BIZARRE)
    assert "%23" in href and "%3F" in href
    assert f'href="{href}"' in client.get("/ui/sources/coffre-maison").text
    r = client.get(href)
    assert r.status_code == 200 and "Contenu du fichier bizarre." in r.text


def test_source_privee_introuvable_avec_cookie_public(client, index, bd):
    bd.execute("UPDATE sources SET visibilite = 'prive'")
    bd.commit()
    connecter(client, "pub")
    for url in ("/ui/sources/coffre-maison", lien("coffre-maison", PATE),
                "/ui/sources/absente"):
        r = client.get(url)
        assert r.status_code == 404 and "Source ou fichier introuvable." in r.text
    assert connecter(client, "priv").get("/ui/sources/coffre-maison").status_code == 200


def test_detail_document_absent_404(client, index):
    connecter(client, "pub")
    r = client.get(lien("coffre-maison", "n-existe-pas.md"))
    assert r.status_code == 404 and "Source ou fichier introuvable." in r.text


def test_bouton_exclure_seulement_pour_l_ecrivain(client, index):
    pub = connecter(client, "pub").get("/ui/sources/coffre-maison").text
    assert "Exclure" not in pub
    priv = connecter(client, "priv").get("/ui/sources/coffre-maison").text
    assert 'hx-post="/ui/exclusions/apercu"' in priv and "Exclure" in priv
    assert """hx-vals='{"motif": "Accueil.md", "origine": "fichiers", "source": "coffre-maison"}'""" in priv


def test_bouton_exclure_absent_des_lignes_deja_exclues(client, index_code):
    r = connecter(client, "priv").get("/ui/sources/depot-test")
    assert '"motif": "a.py"' in r.text
    assert '"motif": "uv.lock"' not in r.text and '"motif": "logo.png"' not in r.text


def test_page_au_dela_de_la_derniere_ramenee_a_la_derniere(client, index, monkeypatch):
    monkeypatch.setattr("rag.ui.PAR_PAGE", 4)
    connecter(client, "pub")
    for page in (2, 9, 10**30):
        r = client.get(f"/ui/sources/coffre-maison?page={page}")
        assert r.status_code == 200 and "Accueil.md" in r.text and "page 2 sur 2" in r.text


def test_erreur_de_validation_sous_ui_sans_cache(client):
    connecter(client, "pub")
    r = client.get("/ui/sources/coffre-maison?page=abc")
    assert r.status_code == 422 and r.headers["cache-control"] == "no-store"


# --- Exclusions ------------------------------------------------------------------------------

@pytest.fixture
def client_priv(cfg, faux_modeles):
    return connecter(faire(cfg, faux_modeles, jeton_prive="priv"), "priv")


@pytest.fixture
def client_pub(cfg, faux_modeles):
    return connecter(faire(cfg, faux_modeles, jeton_prive="priv"), "pub")


DOMAINES = "02 - Domaines/**"


def chemins_indexes(bd):
    r = [d["chemin"] for d in bd.execute("SELECT chemin FROM documents ORDER BY chemin")]
    bd.rollback()
    return r


def exclusions_en_base(bd):
    r = [(e["source"], e["motif"], e["raison"]) for e in bd.execute(
        "SELECT source, motif, raison FROM exclusions_manuelles ORDER BY source, motif")]
    bd.rollback()
    return r


def test_ecran_exclusions_et_menu(client_priv):
    r = client_priv.get("/ui/exclusions")
    assert r.status_code == 200 and r.headers["cache-control"] == "no-store"
    assert "Aucune exclusion" in r.text
    assert '<option value="coffre-maison">' in r.text
    assert 'hx-post="/ui/exclusions/apercu"' in r.text
    assert 'href="/ui/exclusions/export"' in r.text and 'hx-post="/ui/exclusions/import"' in r.text
    assert 'hx-encoding="multipart/form-data"' in r.text
    for page in (r.text, client_priv.get("/ui/").text):
        assert 'href="/ui/"' in page and 'href="/ui/exclusions"' in page
        assert "Déconnexion" in page


def test_ecran_exclusions_en_lecture_pour_le_jeton_public(client_pub):
    r = client_pub.get("/ui/exclusions")
    assert r.status_code == 200
    assert "/ui/exclusions/apercu" not in r.text and "/ui/exclusions/import" not in r.text
    assert "/ui/exclusions/export" not in r.text and "Annuler" not in r.text


def test_apercu_puis_exclusion(client_priv, bd):
    r = client_priv.post("/ui/exclusions/apercu", headers=HX,
                         data={"source": "coffre-maison", "motif": DOMAINES, "raison": "cuisine"})
    assert r.status_code == 200 and r.headers["cache-control"] == "no-store"
    assert "<html" not in r.text
    assert "2 documents seront retirés de l'index" in r.text
    assert "Pâté chinois.md" in r.text and "Confirmer" in r.text
    assert 'name="confirme_gros"' not in r.text
    assert len(chemins_indexes(bd)) == 6  # l'aperçu n'écrit rien
    r = client_priv.post("/ui/exclusions", headers=HX,
                         data={"source": "coffre-maison", "motif": DOMAINES, "raison": "cuisine"})
    assert r.status_code == 200 and "2 documents retirés." in r.text
    assert 'id="liste-exclusions"' in r.text and 'hx-swap-oob="true"' in r.text
    assert DOMAINES in r.text and "cuisine" in r.text
    assert not [c for c in chemins_indexes(bd) if c.startswith("02 - Domaines/")]
    assert exclusions_en_base(bd) == [("coffre-maison", DOMAINES, "cuisine")]
    liste = client_priv.get("/ui/exclusions").text
    assert DOMAINES in liste and "cuisine" in liste and "Annuler" in liste


def test_apercu_depuis_le_bouton_de_la_liste_des_fichiers(client_priv):
    r = client_priv.post("/ui/exclusions/apercu", headers=HX,
                         data={"source": "coffre-maison", "motif": "Accueil.md"})
    assert r.status_code == 200 and "1 document sera retiré de l'index" in r.text
    assert 'name="raison"' in r.text and 'value="Accueil.md"' in r.text


def test_exclusion_depuis_les_fichiers_rafraichit_la_page(client_priv, bd):
    r = client_priv.post("/ui/exclusions/apercu", headers=HX,
                         data={"source": "coffre-maison", "motif": "Accueil.md",
                               "origine": "fichiers"})
    assert r.status_code == 200 and 'name="origine" value="fichiers"' in r.text
    r = client_priv.post("/ui/exclusions", headers=HX,
                         data={"source": "coffre-maison", "motif": "Accueil.md",
                               "origine": "fichiers"})
    assert r.status_code == 200 and r.headers["hx-refresh"] == "true"
    assert "1 document retiré." in r.text and 'id="liste-exclusions"' not in r.text
    assert "Accueil.md" not in chemins_indexes(bd)


def test_exclusion_depuis_les_exclusions_garde_la_liste_hors_bande(client_priv):
    r = client_priv.post("/ui/exclusions/apercu", headers=HX,
                         data={"source": "coffre-maison", "motif": "Accueil.md"})
    assert 'name="origine" value="exclusions"' in r.text
    r = client_priv.post("/ui/exclusions", headers=HX,
                         data={"source": "coffre-maison", "motif": "Accueil.md",
                               "origine": "exclusions"})
    assert "hx-refresh" not in r.headers and 'id="liste-exclusions"' in r.text


def test_gros_motif_exige_la_seconde_confirmation(client_priv, bd):
    sid = bd.execute("SELECT id FROM sources WHERE nom = 'coffre-maison'").fetchone()["id"]
    for i in range(101):
        bd.execute("INSERT INTO documents (source_id, chemin, titre, titre_norm, signature, sha256)"
                   " VALUES (%s, %s, 'x', 'x', 's', 'h')", (sid, f"factices/{i:03}.md"))
    bd.commit()
    form = {"source": "coffre-maison", "motif": "*"}
    r = client_priv.post("/ui/exclusions/apercu", headers=HX, data=form)
    assert r.status_code == 200 and "107 documents seront retirés de l'index" in r.text
    assert 'name="confirme_gros"' in r.text and "Je confirme retirer 107 documents" in r.text
    assert r.text.count("<li>") == 50
    r = client_priv.post("/ui/exclusions", headers=HX, data=form)
    assert r.status_code == 400 and r.headers["cache-control"] == "no-store"
    assert len(chemins_indexes(bd)) == 107 and exclusions_en_base(bd) == []
    r = client_priv.post("/ui/exclusions", headers=HX, data=form | {"confirme_gros": "1"})
    assert r.status_code == 200 and "107 documents retirés." in r.text
    assert chemins_indexes(bd) == []


def test_exclusion_motif_invalide_et_source_inconnue(client_priv, bd):
    r = client_priv.post("/ui/exclusions/apercu", headers=HX,
                         data={"source": "coffre-maison", "motif": "x" * 1001})
    assert r.status_code == 400 and "1000 caractères" in r.text
    r = client_priv.post("/ui/exclusions", headers=HX,
                         data={"source": "coffre-maison", "motif": "a", "raison": "r" * 501})
    assert r.status_code == 400 and "500 caractères" in r.text
    for url in ("/ui/exclusions/apercu", "/ui/exclusions"):
        r = client_priv.post(url, headers=HX, data={"source": "absente", "motif": "*"})
        assert r.status_code == 404
    assert exclusions_en_base(bd) == []


def test_annuler(client_priv, bd):
    client_priv.post("/ui/exclusions", headers=HX,
                     data={"source": "coffre-maison", "motif": "Accueil.md"})
    assert bd.execute("SELECT demande_le FROM sources").fetchone()["demande_le"] is None
    bd.rollback()
    r = client_priv.post("/ui/exclusions/annuler", headers=HX,
                         data={"source": "coffre-maison", "motif": "Accueil.md"})
    assert r.status_code == 200 and r.headers["cache-control"] == "no-store"
    assert "Exclusion annulée : le fichier reviendra à la prochaine passe." in r.text
    assert 'id="liste-exclusions"' in r.text and "Accueil.md" not in r.text
    assert exclusions_en_base(bd) == []
    assert bd.execute("SELECT demande_le FROM sources").fetchone()["demande_le"] is not None
    bd.rollback()
    assert "Accueil.md" not in client_priv.get("/ui/exclusions").text


def test_ecritures_refusees_au_jeton_public(client_pub, bd):
    form = {"source": "coffre-maison", "motif": "Accueil.md"}
    for url in ("/ui/exclusions/apercu", "/ui/exclusions", "/ui/exclusions/annuler"):
        assert client_pub.post(url, headers=HX, data=form).status_code == 403
    r = client_pub.post("/ui/exclusions/import", headers=HX,
                        files={"fichier": ("e.yaml", b"- {source: a, motif: b}\n", "text/yaml")})
    assert r.status_code == 403
    assert client_pub.get("/ui/exclusions/export").status_code == 403
    assert exclusions_en_base(bd) == [] and len(chemins_indexes(bd)) == 6


def test_ecritures_exigent_htmx_et_la_meme_origine(client_priv, bd):
    form = {"source": "coffre-maison", "motif": "Accueil.md"}
    assert client_priv.post("/ui/exclusions", data=form).status_code == 403
    assert client_priv.post("/ui/exclusions", data=form,
                            headers=HX | {"Origin": "http://ailleurs.exemple"}).status_code == 403
    r = client_priv.post("/ui/exclusions/import",
                         files={"fichier": ("e.yaml", b"- {source: a, motif: b}\n", "text/yaml")})
    assert r.status_code == 403
    assert exclusions_en_base(bd) == []


def test_export_import_par_l_interface(client_priv, bd):
    client_priv.post("/ui/exclusions", headers=HX,
                     data={"source": "coffre-maison", "motif": "Accueil.md", "raison": "bruit"})
    r = client_priv.get("/ui/exclusions/export")
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/yaml")
    assert r.headers["content-disposition"] == 'attachment; filename="exclusions-rag.yaml"'
    assert r.headers["cache-control"] == "no-store"
    assert yaml.safe_load(r.text) == [{"source": "coffre-maison", "motif": "Accueil.md",
                                       "raison": "bruit"}]
    r = client_priv.post("/ui/exclusions/import", headers=HX,
                         files={"fichier": ("exclusions-rag.yaml", r.content, "text/yaml")})
    assert r.status_code == 200 and "0 ajoutées, 1 déjà présentes, 0 document retiré." in r.text
    assert 'id="liste-exclusions"' in r.text


def test_import_applique_et_liste(client_priv, bd):
    texte = "- {source: coffre-maison, motif: Accueil.md}\n- {source: future, motif: '*.csv'}\n"
    r = client_priv.post("/ui/exclusions/import", headers=HX,
                         files={"fichier": ("e.yaml", texte.encode(), "text/yaml")})
    assert r.status_code == 200 and "2 ajoutées, 0 déjà présentes, 1 document retiré." in r.text
    assert "Accueil.md" not in chemins_indexes(bd)
    assert "future" in client_priv.get("/ui/exclusions").text  # source inconnue : écrivain


def test_import_malforme(client_priv, bd):
    for contenu in (b"pas une liste", b"\xff\xfe pas de l'UTF-8"):
        r = client_priv.post("/ui/exclusions/import", headers=HX,
                             files={"fichier": ("e.yaml", contenu, "text/yaml")})
        assert r.status_code == 400 and r.headers["cache-control"] == "no-store"
    assert "une liste d&#39;exclusions est attendue" in client_priv.post(
        "/ui/exclusions/import", headers=HX,
        files={"fichier": ("e.yaml", b"pas une liste", "text/yaml")}).text
    assert client_priv.post("/ui/exclusions/import", headers=HX, data={}).status_code == 400
    assert exclusions_en_base(bd) == []


def test_import_trop_gros(client_priv, bd):
    mo = 1024 * 1024
    juste = b"[]\n" + b"#" * (mo - 4) + b"\n"
    assert len(juste) == mo
    r = client_priv.post("/ui/exclusions/import", headers=HX,
                         files={"fichier": ("e.yaml", juste, "text/yaml")})
    assert r.status_code == 200 and "0 ajoutées, 0 déjà présentes, 0 document retiré." in r.text
    r = client_priv.post("/ui/exclusions/import", headers=HX,
                         files={"fichier": ("e.yaml", juste + b"#", "text/yaml")})
    assert r.status_code == 413 and r.headers["cache-control"] == "no-store"
    r = client_priv.post("/ui/exclusions/import", headers=HX,
                         files={"fichier": ("e.yaml", b"#" * (2 * mo), "text/yaml")})
    assert r.status_code == 413
    assert exclusions_en_base(bd) == []


def test_exclusions_visibles_selon_le_jeton(client_priv, client_pub, bd):
    bd.execute("INSERT INTO exclusions_manuelles (source, motif) VALUES "
               "('coffre-maison', 'Accueil.md'), ('future', 'plus-tard.md')")
    bd.commit()
    pub = client_pub.get("/ui/exclusions").text
    assert "Accueil.md" in pub and "plus-tard.md" not in pub and "future" not in pub
    priv = client_priv.get("/ui/exclusions").text
    assert "Accueil.md" in priv and "plus-tard.md" in priv
    bd.execute("UPDATE sources SET visibilite = 'prive'")
    bd.commit()
    pub = client_pub.get("/ui/exclusions").text
    assert "Accueil.md" not in pub and "coffre-maison" not in pub
