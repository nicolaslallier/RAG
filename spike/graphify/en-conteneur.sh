#!/bin/sh
# Lance `graphify update <chemin>` dans l'image du spike, sans réseau (RAG_GRAPHIFY_COMMANDE en dev).
# Le chemin doit être sous $HOME (Colima ne partage que lui) ; c'est une copie, jamais le clone.
set -eu
[ "$1" = update ] || { echo "usage: $0 update <chemin>" >&2; exit 2; }
exec docker run --rm --network none -v "$2":/w rag-spike-graphify update /w
