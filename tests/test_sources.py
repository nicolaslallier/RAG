import re
import unicodedata
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rag.sources import LecteurDossier, LecteurS3, ListagePartielError
from tests.conftest import FauxS3

T = datetime(2026, 11, 1, tzinfo=UTC)


def obj(cle, etag='"e"'):
    return {"Key": cle, "ETag": etag, "LastModified": T, "Size": 1}


def test_dossier_liste_en_nfc_et_lit(tmp_path):
    nfd = unicodedata.normalize("NFD", "Modèles/é.md")
    (tmp_path / nfd).parent.mkdir()
    (tmp_path / nfd).write_text("x")
    e = LecteurDossier(tmp_path).lister()
    assert [x.chemin for x in e] == [unicodedata.normalize("NFC", "Modèles/é.md")]
    assert LecteurDossier(tmp_path).lire(e[0].chemin) == b"x"


def test_dossier_ignore_git(tmp_path):
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git/HEAD").write_text("ref")
    (tmp_path / "a.md").write_text("a")
    assert [x.chemin for x in LecteurDossier(tmp_path).lister()] == ["a.md"]


def test_s3_pagine_et_retire_le_prefixe():
    faux = FauxS3([
        {"Contents": [obj("coffre/a.md"), obj("coffre/b/")], "IsTruncated": True,
         "NextContinuationToken": "t"},
        {"Contents": [obj("coffre/b/c.md")], "IsTruncated": False},
    ])
    assert [e.chemin for e in LecteurS3(faux, "obsidian", "coffre/").lister()] == ["a.md", "b/c.md"]
    assert faux.appels_list == 2 and faux.args[1]["ContinuationToken"] == "t"


def test_s3_signature_est_l_etag_sans_guillemets():
    faux = FauxS3([{"Contents": [obj("a.md", '"abc"')]}])
    assert LecteurS3(faux, "obsidian").lister()[0].signature == "abc"


def test_s3_listage_tronque_sans_jeton_leve():
    faux = FauxS3([{"Contents": [obj("a.md")], "IsTruncated": True}])
    with pytest.raises(ListagePartielError):
        LecteurS3(faux, "obsidian").lister()


def test_s3_lit_une_cle_nfd_par_son_chemin_nfc():
    cle = unicodedata.normalize("NFD", "p/Modèles.md")
    faux = FauxS3([{"Contents": [obj(cle)]}], {cle: b"contenu"})
    lec = LecteurS3(faux, "obsidian", "p/")
    (e,) = lec.lister()
    assert e.chemin == unicodedata.normalize("NFC", "Modèles.md")
    assert lec.lire(e.chemin) == b"contenu"


def test_aucune_ecriture_s3_dans_le_depot():
    src = "".join(p.read_text() for p in Path("rag").rglob("*.py"))
    assert not re.search(r"put_object|delete_object|copy_object|upload_file", src)


@pytest.mark.parametrize("brut,attendu", [("coffre", "coffre/"), ("coffre/", "coffre/"),
                                          ("", ""), ("/", "")])
def test_prefixe_du_coffre_normalise(brut, attendu):
    from rag.config import Reglages, SourceConf
    from rag.sources import lecteur_pour
    r = Reglages(database_url="x", litellm_url="x", coffre_prefixe=brut)
    src = SourceConf(nom="c", type="obsidian", emplacement="s3")
    assert lecteur_pour(src, r).prefixe == attendu
