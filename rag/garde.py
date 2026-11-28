"""Gardes HTTP posées devant toute l'application : plafond du corps, en-têtes de sécurité."""

from starlette.responses import JSONResponse

CORPS_MAX = 64 * 1024


async def _trop_gros(scope, receive, send) -> None:
    await JSONResponse({"detail": "corps de requête trop gros"}, status_code=413,
                       headers={"Connection": "close"})(scope, receive, send)


class LimiteCorps:
    """Refuse un corps trop gros avant qu'il soit lu : FastAPI lit le corps avant les
    dépendances, donc avant le jeton. `exceptions` : chemin exact -> plafond propre ; ces
    routes vérifient elles-mêmes Content-Length (message lisible), seul le flux y est plafonné.

    Sur un flux qui dépasse, la 413 part d'ici et l'application reçoit `http.disconnect` :
    une exception levée dans `receive` deviendrait une 400 chez FastAPI."""

    def __init__(self, app, plafond: int = CORPS_MAX, exceptions: dict[str, int] | None = None):
        self.app, self.plafond, self.exceptions = app, plafond, exceptions or {}

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        chemin = scope["path"]
        plafond = self.exceptions.get(chemin, self.plafond)
        if chemin not in self.exceptions:
            longueur = dict(scope["headers"]).get(b"content-length", b"")
            if longueur.isdigit() and (len(longueur) > 12 or int(longueur) > plafond):
                await _trop_gros(scope, receive, send)
                return
        recu, refuse, commence = 0, False, False

        async def recevoir():
            nonlocal recu, refuse
            if refuse:
                return {"type": "http.disconnect"}
            message = await receive()
            if message["type"] == "http.request":
                recu += len(message.get("body", b""))
                if recu > plafond:
                    refuse = True
                    if not commence:
                        await _trop_gros(scope, receive, send)
                    return {"type": "http.disconnect"}
            return message

        async def envoyer(message):
            nonlocal commence
            if refuse:  # la 413 est partie : ce que l'application répond ensuite est jeté
                return
            commence = commence or message["type"] == "http.response.start"
            await send(message)

        try:
            await self.app(scope, recevoir, envoyer)
        except Exception:
            if not refuse:  # après un refus, l'application échoue sur la déconnexion simulée
                raise


ENTETES_SECURITE = (
    (b"x-frame-options", b"DENY"),
    (b"content-security-policy", (b"default-src 'self'; frame-ancestors 'none'; "
                                  b"base-uri 'none'; form-action 'self'; object-src 'none'")),
    (b"x-content-type-options", b"nosniff"),
    (b"referrer-policy", b"same-origin"),
)


class EntetesSecurite:
    """Pose les en-têtes de sécurité sur toute réponse HTTP, sans écraser un en-tête déjà là."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def envoyer(message):
            if message["type"] == "http.response.start":
                entetes = list(message.get("headers", []))
                presents = {nom.lower() for nom, _ in entetes}
                entetes += [(n, v) for n, v in ENTETES_SECURITE if n not in presents]
                message = {**message, "headers": entetes}
            await send(message)

        await self.app(scope, receive, envoyer)
