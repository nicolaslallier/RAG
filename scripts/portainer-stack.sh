#!/usr/bin/env bash
# Crée la pile « rag » dans le Portainer de l'Infra, environnement de Colima (Mac Studio), ou la met à jour (image retirée de nouveau).
# Le dépôt est privé : Portainer ne clone rien, le fichier de pile lui est envoyé comme texte.
# L'API de Portainer réécrit TOUTE la liste Env à chaque déploiement : on renvoie donc chaque
# variable à chaque `up`, depuis $RAG_STACK_ENV.
#
# Usage : scripts/portainer-stack.sh up
# Variables : PORTAINER_URL, PORTAINER_API_KEY, PORTAINER_CA_CERT, RAG_STACK_ENV,
#   PORTAINER_ENDPOINT_ID (obligatoire).
set -euo pipefail
cd "$(dirname "$0")/.."

STACK=rag
COMPOSE_FILE=deploy/portainer-stack.yml
REQUIRED="RAG_DB_PASSWORD RAG_LITELLM_CLE RAG_JETON RAG_JETON_REINDEXER RAG_S3_SECRET_KEY IMAGE_TAG"

die() { echo "::error::portainer-stack.sh : $*" >&2; exit 1; }
[ "${1:-}" = up ] || die "usage : scripts/portainer-stack.sh up"

PORTAINER_URL="${PORTAINER_URL:?PORTAINER_URL absent}"
case "$PORTAINER_URL" in https://*) ;; *) die "PORTAINER_URL doit être en https://" ;; esac
PORTAINER_URL="${PORTAINER_URL%/}"
[ -r "${PORTAINER_CA_CERT:-}" ] || die "PORTAINER_CA_CERT illisible : ${PORTAINER_CA_CERT:-}"
[ -n "${PORTAINER_API_KEY:-}" ] || die "PORTAINER_API_KEY absent"
[ -n "${PORTAINER_ENDPOINT_ID:-}" ] || die "PORTAINER_ENDPOINT_ID absent"
[ -r "${RAG_STACK_ENV:-}" ] || die "RAG_STACK_ENV illisible : ${RAG_STACK_ENV:-}"
[ -r "$COMPOSE_FILE" ] || die "$COMPOSE_FILE illisible"
command -v jq >/dev/null || die "jq est requis"

# Fichier NOM=valeur -> [{name,value}]. Rien n'est interprété (un secret peut contenir `$` ou `#`).
env="$(jq -Rn '[inputs | select(test("^[A-Za-z_][A-Za-z0-9_]*=")) | capture("^(?<name>[^=]+)=(?<value>.*)$")]' < "$RAG_STACK_ENV")"
for n in $REQUIRED; do
  jq -e --arg n "$n" 'any(.[]; .name == $n and .value != "")' <<<"$env" >/dev/null || die "$n absent de la pile"
done

# La clé passe à curl par un fichier de config (mode 600), jamais par argv.
CURL_CFG="$(umask 077 && mktemp)"
trap 'rm -f "$CURL_CFG"' EXIT
# Un secret collé garde souvent un saut de ligne : il casserait l'en-tête.
PORTAINER_API_KEY="${PORTAINER_API_KEY//[[:space:]]/}"
printf 'header = "X-API-Key: %s"\n' "$PORTAINER_API_KEY" > "$CURL_CFG"

api() { # <méthode> <chemin> [corps-json]
  local out
  out="$(printf '%s' "${3:-}" | curl -sS --connect-timeout 10 --max-time 300 --cacert "$PORTAINER_CA_CERT" -K "$CURL_CFG" --fail-with-body \
    -X "$1" -H "Content-Type: application/json" --data-binary @- "$PORTAINER_URL/api$2" 2>&1)" \
    || { printf '%s\n' "${out//"$PORTAINER_API_KEY"/***}" >&2; die "$1 $2 a échoué"; }
  printf '%s' "$out"
}

eid="$PORTAINER_ENDPOINT_ID"
stack="$(api GET /stacks | jq -c --arg n "$STACK" --argjson e "$eid" 'first(.[] | select(.Name == $n and .EndpointId == $e)) // empty')"
sid=""
[ -z "$stack" ] || sid="$(jq -r .Id <<<"$stack")"

# Aucun secret en argv : l'environnement entre dans jq par stdin, le fichier par --rawfile.
if [ -z "$sid" ]; then
  body="$(jq -c --arg name "$STACK" --rawfile pile "$COMPOSE_FILE" \
    '{Name: $name, StackFileContent: $pile, Env: .}' <<<"$env")"
  api POST "/stacks/create/standalone/string?endpointId=$eid" "$body" > /dev/null
  echo "portainer-stack.sh : pile '$STACK' créée"
else
  # Status 2 = arrêtée : on la relance avant de la mettre à jour.
  if [ "$(jq -r .Status <<<"$stack")" = 2 ]; then
    api POST "/stacks/$sid/start?endpointId=$eid" > /dev/null
  fi
  body="$(jq -c --rawfile pile "$COMPOSE_FILE" \
    '{StackFileContent: $pile, Env: ., Prune: true, PullImage: true}' <<<"$env")"
  api PUT "/stacks/$sid?endpointId=$eid" "$body" > /dev/null
  echo "portainer-stack.sh : pile '$STACK' mise à jour (image retirée)"
fi
