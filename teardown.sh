#!/usr/bin/env bash
#
# teardown.sh — deletes ALL Azure infrastructure created by provision.sh by
# removing the entire resource group. Destructive and irreversible.
#
# Usage: ./teardown.sh [--yes]
set -euo pipefail

RG="${RG:-test1}"

if [ "${1:-}" != "--yes" ]; then
  read -r -p "This will DELETE resource group '$RG' and everything in it. Type 'yes' to continue: " ans
  [ "$ans" = "yes" ] || { echo "aborted"; exit 1; }
fi

echo ">>> deleting resource group '$RG' (this runs asynchronously and can take several minutes)"
az group delete --name "$RG" --yes
echo ">>> resource group '$RG' deleted"
