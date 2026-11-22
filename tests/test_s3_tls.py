"""boto3 joint le S3 de l'Infra en TLS avec la CA maison (sans keyUsage, comme la vraie)."""

import os
import ssl
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest

from rag.sources import client_s3

LISTE = (b'<?xml version="1.0" encoding="UTF-8"?><ListBucketResult '
         b'xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>obsidian</Name>'
         b"<KeyCount>0</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated>"
         b"</ListBucketResult>")


class _S3(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(LISTE)))
        self.end_headers()
        self.wfile.write(LISTE)


def _ssl(*args, d):
    subprocess.run(["openssl", *args], cwd=d, check=True, capture_output=True)


@pytest.fixture(scope="module")
def faux_s3(tmp_path_factory):
    d = tmp_path_factory.mktemp("ca-maison")
    # CA comme celle de l'Infra : basicConstraints CA:TRUE critique, pas de keyUsage.
    _ssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=CA essai",
         "-addext", "basicConstraints=critical,CA:TRUE", "-keyout", "ca.key", "-out", "ca.crt", d=d)
    _ssl("req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost", "-keyout", "s.key",
         "-out", "s.csr", d=d)
    (d / "ext").write_text("subjectAltName=DNS:localhost\n")
    _ssl("x509", "-req", "-in", "s.csr", "-CA", "ca.crt", "-CAkey", "ca.key", "-CAcreateserial",
         "-days", "1", "-extfile", "ext", "-out", "s.crt", d=d)
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _S3)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(d / "s.crt", d / "s.key")
    srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield SimpleNamespace(url=f"https://localhost:{srv.server_address[1]}", ca=str(d / "ca.crt"))
    srv.shutdown()


def _reglages(url):
    return SimpleNamespace(s3_endpoint=url, s3_access_key="rag", s3_secret_key="x")


def test_s3_en_tls_avec_la_ca_maison(faux_s3, monkeypatch):
    monkeypatch.setenv("AWS_CA_BUNDLE", faux_s3.ca)
    r = client_s3(_reglages(faux_s3.url)).list_objects_v2(Bucket="obsidian", MaxKeys=1)
    assert r["KeyCount"] == 0


def test_s3_refuse_sans_la_ca(faux_s3, monkeypatch):
    monkeypatch.delenv("AWS_CA_BUNDLE", raising=False)
    from botocore.exceptions import SSLError
    with pytest.raises(SSLError):
        client_s3(_reglages(faux_s3.url)).list_objects_v2(Bucket="obsidian", MaxKeys=1)


@pytest.mark.vivant
@pytest.mark.skipif(not os.environ.get("RAG_S3_SECRET_KEY"), reason="RAG_S3_SECRET_KEY absent")
def test_vrai_s3_de_l_infra():
    from rag.config import Reglages
    r = client_s3(Reglages()).list_objects_v2(Bucket="obsidian", MaxKeys=1)
    assert r["ResponseMetadata"]["HTTPStatusCode"] == 200
