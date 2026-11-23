#!/usr/bin/env bash
# Écrit dans $RAG_STACK_ENV (NOM=valeur) les secrets de la pile « rag » lus dans OpenBao
# (KV v2, VAULT_SECRET_PATH, défaut infra/data/rag). Variables : BAO_TOKEN, VAULT_URL,
# VAULT_CA_CERT. Jamais de valeur sur la sortie.
set -euo pipefail
umask 077

OUT="${RAG_STACK_ENV:?RAG_STACK_ENV absent}"
VAULT_URL="${VAULT_URL:-https://vault.infra.famillelallier.net}"
VAULT_SECRET_PATH="${VAULT_SECRET_PATH:-infra/data/rag}"
# Variable de la pile = clé du coffre (même nom). tests/test_pile.py vérifie que cette liste
# est exactement celle des ${…} de deploy/portainer-stack.yml.
CLES="RAG_DB_PASSWORD RAG_LITELLM_CLE RAG_JETON RAG_JETON_PRIVE RAG_JETON_REINDEXER RAG_GITHUB_JETON RAG_CLE_SECRETS RAG_S3_SECRET_KEY RAG_COFFRE_PREFIXE"
# Absentes, elles sont écrites vides : Portainer réécrit toute la liste Env.
FACULTATIVES=" RAG_JETON_PRIVE RAG_GITHUB_JETON RAG_CLE_SECRETS RAG_COFFRE_PREFIXE "

die() { echo "::error::vault-render-env.sh : $*" >&2; exit 1; }
BAO_TOKEN="${BAO_TOKEN//[[:space:]]/}"
[ -n "$BAO_TOKEN" ] || die "BAO_TOKEN absent"
case "$VAULT_URL" in https://*) ;; *) die "VAULT_URL doit être en https://" ;; esac
[ -r "${VAULT_CA_CERT:-}" ] || die "VAULT_CA_CERT illisible : ${VAULT_CA_CERT:-}"
command -v jq >/dev/null || die "jq est requis"

# Jeton périodique : le renouveler à chaque déploiement. En cas d'échec on continue,
# la lecture ci-dessous échouera bruyamment si le jeton est mort.
printf 'header = "X-Vault-Token: %s"\n' "$BAO_TOKEN" \
  | curl -fsS -o /dev/null --connect-timeout 10 --max-time 120 --cacert "$VAULT_CA_CERT" -K - \
    -X POST "$VAULT_URL/v1/auth/token/renew-self" \
  || echo "::warning::renouvellement du jeton OpenBao impossible" >&2

# Le jeton passe par un fichier de config curl lu sur stdin, jamais par argv.
secret="$(printf 'header = "X-Vault-Token: %s"\n' "$BAO_TOKEN" \
  | curl -fsS --connect-timeout 10 --max-time 120 --cacert "$VAULT_CA_CERT" -K - "$VAULT_URL/v1/$VAULT_SECRET_PATH" \
  | jq -c .data.data)" || die "coffre illisible (scellé ? jeton expiré ?)"

# Tout est lu et vérifié avant d'écrire : un refus ne laisse pas de fichier à moitié rempli.
for key in $CLES; do
  if ! v="$(jq -er --arg k "$key" '.[$k] | select(. != null and . != "")' <<<"$secret")"; then
    case "$FACULTATIVES" in *" $key "*) ;; *) die "clé $key absente ou vide dans $VAULT_SECRET_PATH" ;; esac
    v=""
  fi
  # Un retour à la ligne injecterait une autre variable dans le fichier.
  case "$v" in *$'\n'* | *$'\r'*) die "$key contient un retour à la ligne" ;; esac
  # Avant toute autre sortie : le journal Actions masque la valeur.
  [ "${GITHUB_ACTIONS:-}" != true ] || [ -z "$v" ] || printf '::add-mask::%s\n' "$v"
  printf -v "$key" '%s' "$v"   # la variable porte le nom de la clé
done

# Un jeton court se devine ; deux jetons égaux donneraient la réindexation au lecteur.
for key in RAG_JETON RAG_JETON_REINDEXER RAG_JETON_PRIVE; do
  [ "$key" = RAG_JETON_PRIVE ] && [ -z "$RAG_JETON_PRIVE" ] && continue
  v="${!key}"
  [ "${#v}" -ge 32 ] || die "$key trop court (32 caractères au moins)"
done
[ "$RAG_JETON" != "$RAG_JETON_REINDEXER" ] || die "RAG_JETON_REINDEXER égal à RAG_JETON"
if [ -n "$RAG_JETON_PRIVE" ]; then
  [ "$RAG_JETON_PRIVE" != "$RAG_JETON" ] || die "RAG_JETON_PRIVE égal à RAG_JETON"
  [ "$RAG_JETON_PRIVE" != "$RAG_JETON_REINDEXER" ] || die "RAG_JETON_PRIVE égal à RAG_JETON_REINDEXER"
fi
# Il entre tel quel dans l'URL postgresql:// de la pile.
[[ "$RAG_DB_PASSWORD" =~ ^[A-Za-z0-9]+$ ]] || die "RAG_DB_PASSWORD doit être fait de lettres et de chiffres seulement"

: > "$OUT"
chmod 600 "$OUT"
for key in $CLES; do
  printf '%s=%s\n' "$key" "${!key}" >> "$OUT"
  echo "  $key <- $VAULT_SECRET_PATH:$key"
done
