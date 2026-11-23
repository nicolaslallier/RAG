"""Scripts de déploiement contre un faux OpenBao / Portainer en HTTPS local."""

import json
import os
import shutil
import ssl
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest

J1, J2, J3 = "1" * 40, "2" * 40, "3" * 40
COFFRE = {"RAG_DB_PASSWORD": "Abc123", "RAG_LITELLM_CLE": "sk-essai", "RAG_JETON": J1,
          "RAG_JETON_REINDEXER": J2, "RAG_GITHUB_JETON": "gh-essai", "RAG_S3_SECRET_KEY": "s3-essai"}
BAO, CLE_API = "bao-jeton-essai", "portainer-cle-essai"


def SANS(cle):
    return {k: v for k, v in COFFRE.items() if k != cle}


def AVEC(**k):
    return {**COFFRE, **k}


class _Faux(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _repondre(self, corps, code=200):
        b = json.dumps(corps).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def _traiter(self):
        s = self.server.etat
        n = int(self.headers.get("Content-Length") or 0)
        brut = self.rfile.read(n) if n else b""
        chemin = self.path
        if chemin.startswith("/v1/"):
            if self.headers.get("X-Vault-Token") != BAO:
                return self._repondre({}, 403)
            if chemin == "/v1/auth/token/renew-self":
                return self._repondre({})
            return self._repondre({"data": {"data": s.coffre}})
        if self.headers.get("X-API-Key") != CLE_API:
            return self._repondre({}, 401)
        s.requetes.append(SimpleNamespace(methode=self.command, chemin=chemin,
                                          json=json.loads(brut) if brut else None))
        if self.command == "GET" and chemin == "/api/stacks":
            return self._repondre(s.piles)
        return self._repondre({"Id": 7})

    do_GET = do_POST = do_PUT = _traiter


@pytest.fixture(scope="module")
def certificat(tmp_path_factory):
    d = tmp_path_factory.mktemp("ca")
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                    "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost",
                    "-keyout", d / "k.pem", "-out", d / "c.pem"], check=True, capture_output=True)
    return d


@pytest.fixture
def faux(certificat):
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _Faux)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certificat / "c.pem", certificat / "k.pem")
    srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    srv.etat = SimpleNamespace(coffre=dict(COFFRE), piles=[], requetes=[],
                               url=f"https://localhost:{srv.server_address[1]}",
                               ca=str(certificat / "c.pem"))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield srv.etat
    srv.shutdown()


def lancer(script, *args, env):
    base = {"PATH": os.environ["PATH"], "HOME": os.environ.get("HOME", "/tmp")}
    return subprocess.run([script, *args], env={**base, **env}, capture_output=True, text=True, check=False,
                          timeout=60)


def env_rendu(faux, tmp_path):
    return {"RAG_STACK_ENV": str(tmp_path / "rag.env"), "BAO_TOKEN": BAO, "VAULT_URL": faux.url,
            "VAULT_CA_CERT": faux.ca}


def env_up(faux, tmp_path):
    f = tmp_path / "rag.env"
    f.write_text("".join(f"{k}={v}\n" for k, v in COFFRE.items()) + "IMAGE_TAG=sha-abc1234\n")
    return {"RAG_STACK_ENV": str(f), "PORTAINER_API_KEY": CLE_API, "PORTAINER_URL": faux.url,
            "PORTAINER_CA_CERT": faux.ca, "PORTAINER_ENDPOINT_ID": "3"}


def test_rendu_ecrit_les_cles_et_rien_sur_la_sortie(faux, tmp_path):
    r = lancer("scripts/vault-render-env.sh", env=env_rendu(faux, tmp_path))
    f = tmp_path / "rag.env"
    assert r.returncode == 0, r.stderr
    lignes = f.read_text().splitlines()
    assert "RAG_JETON=" + J1 in lignes and "RAG_JETON_PRIVE=" in lignes
    assert "RAG_COFFRE_PREFIXE=" in lignes and len(lignes) == 9 and "RAG_CLE_SECRETS=" in lignes
    assert all(v not in r.stdout + r.stderr for v in COFFRE.values())
    assert oct(f.stat().st_mode)[-3:] == "600"


def test_rendu_masque_les_valeurs_dans_actions(faux, tmp_path):
    r = lancer("scripts/vault-render-env.sh", env={**env_rendu(faux, tmp_path),
                                                    "GITHUB_ACTIONS": "true"})
    assert r.returncode == 0 and f"::add-mask::{J1}" in r.stdout


@pytest.mark.parametrize("coffre,attendu", [
    (SANS("RAG_S3_SECRET_KEY"), "RAG_S3_SECRET_KEY"),
    (AVEC(RAG_JETON="court"), "RAG_JETON"),
    (AVEC(RAG_JETON_REINDEXER=J1), "RAG_JETON_REINDEXER"),
    (AVEC(RAG_JETON_PRIVE=J2), "RAG_JETON_PRIVE"),
    (AVEC(RAG_JETON_PRIVE="court"), "RAG_JETON_PRIVE"),
    (AVEC(RAG_DB_PASSWORD="a/b"), "RAG_DB_PASSWORD"),
    (AVEC(RAG_JETON=J1 + "\nX=1"), "retour à la ligne"),
])
def test_rendu_refuse(faux, tmp_path, coffre, attendu):
    faux.coffre = coffre
    r = lancer("scripts/vault-render-env.sh", env=env_rendu(faux, tmp_path))
    assert r.returncode != 0 and attendu in r.stderr
    assert J1 not in r.stdout + r.stderr and "a/b" not in r.stderr
    assert not (tmp_path / "rag.env").exists()


def test_rendu_accepte_un_jeton_prive(faux, tmp_path):
    faux.coffre = AVEC(RAG_JETON_PRIVE=J3)
    assert lancer("scripts/vault-render-env.sh", env=env_rendu(faux, tmp_path)).returncode == 0


def test_cree_puis_met_a_jour_la_pile(faux, tmp_path):
    env = env_up(faux, tmp_path)
    r = lancer("scripts/portainer-stack.sh", "up", env=env)
    assert r.returncode == 0, r.stderr
    c = faux.requetes[-1]
    assert c.methode == "POST" and c.chemin == "/api/stacks/create/standalone/string?endpointId=3"
    assert c.json["Name"] == "rag" and "rag-indexeur" in c.json["StackFileContent"]
    assert {"name": "RAG_JETON", "value": J1} in c.json["Env"]
    faux.piles = [{"Id": 7, "Name": "rag", "EndpointId": 3, "Status": 2}]
    r = lancer("scripts/portainer-stack.sh", "up", env=env)
    assert r.returncode == 0, r.stderr
    demarrage, m = faux.requetes[-2:]
    assert (demarrage.methode, demarrage.chemin) == ("POST", "/api/stacks/7/start?endpointId=3")
    assert (m.methode, m.chemin) == ("PUT", "/api/stacks/7?endpointId=3")
    assert m.json["PullImage"] is True and m.json["Prune"] is True
    assert m.json["StackFileContent"] == Path("deploy/portainer-stack.yml").read_text()
    assert all(v not in r.stdout + r.stderr for v in (CLE_API, J1))


def test_up_refuse_une_variable_obligatoire_vide(faux, tmp_path):
    env = env_up(faux, tmp_path)
    f = Path(env["RAG_STACK_ENV"])
    f.write_text(f.read_text().replace("IMAGE_TAG=sha-abc1234", "IMAGE_TAG="))
    r = lancer("scripts/portainer-stack.sh", "up", env=env)
    assert r.returncode != 0 and "IMAGE_TAG" in r.stderr and faux.requetes == []


def test_up_accepte_un_jeton_github_vide(faux, tmp_path):
    env = env_up(faux, tmp_path)
    f = Path(env["RAG_STACK_ENV"])
    lignes = [ligne for ligne in f.read_text().splitlines()
              if not ligne.startswith("RAG_GITHUB_JETON=")]
    f.write_text("\n".join([*lignes, "RAG_GITHUB_JETON="]) + "\n")
    r = lancer("scripts/portainer-stack.sh", "up", env=env)
    assert r.returncode == 0, r.stderr


def test_aucun_secret_en_argv():
    for s in ("scripts/vault-render-env.sh", "scripts/portainer-stack.sh"):
        t = Path(s).read_text()
        assert '-H "X-Vault-Token' not in t and '-H "X-API-Key' not in t


@pytest.mark.skipif(not shutil.which("shellcheck"), reason="shellcheck absent (la CI le lance)")
def test_shellcheck():
    assert subprocess.run(["shellcheck", "scripts/vault-render-env.sh",
                           "scripts/portainer-stack.sh"], check=False).returncode == 0


def test_rendu_jeton_github_et_cle_secrets_facultatifs(faux, tmp_path):
    faux.coffre = AVEC(RAG_GITHUB_JETON="")
    r = lancer("scripts/vault-render-env.sh", env=env_rendu(faux, tmp_path))
    assert r.returncode == 0, r.stderr
    lignes = (tmp_path / "rag.env").read_text().splitlines()
    assert "RAG_GITHUB_JETON=" in lignes and "RAG_CLE_SECRETS=" in lignes
