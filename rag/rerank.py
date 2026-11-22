"""Reranker cross-encoder (bge-reranker-v2-m3), chargé au premier appel : torch reste hors de
l'import tant que le rerank n'est pas actif."""

from pathlib import Path


class Reranker:
    def __init__(self, modele: str, cache: Path | None = None):
        self.modele, self.cache, self._ce = modele, cache, None

    def noter(self, question: str, textes: list[str]) -> list[float]:
        if not textes:
            return []
        if self._ce is None:
            from sentence_transformers import CrossEncoder
            cache = str(self.cache) if self.cache else None
            try:  # modèle en cache : aucun appel au Hub (équivaut à HF_HUB_OFFLINE=1)
                self._ce = CrossEncoder(self.modele, max_length=512, cache_folder=cache,
                                        local_files_only=True)
            except OSError:  # premier démarrage : téléchargement unique
                self._ce = CrossEncoder(self.modele, max_length=512, cache_folder=cache)
        # une seule sortie : sentence-transformers applique déjà la sigmoïde (scores 0..1)
        return [float(x) for x in self._ce.predict([(question, t) for t in textes])]


def reranker_depuis(config, reglages) -> Reranker | None:
    rr = config.recherche.rerank
    return Reranker(rr.modele, Path(reglages.donnees_dir) / "modeles") if rr.actif else None
