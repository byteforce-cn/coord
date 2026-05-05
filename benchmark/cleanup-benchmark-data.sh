#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="${1:-http://127.0.0.1:9091}"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for benchmark data cleanup" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

echo "creating backup from ${ENDPOINT}"
backup_response="$(curl -fsS -X POST "${ENDPOINT}/api/v1/admin/backup/create")"
echo "$backup_response" | jq -r '.payload_json' >"${tmp_dir}/original_payload.json"

jq '
  .registry = ((.registry // [])
    | map(select((.instance.service_name // "") | startswith("bench-") | not)))
  | .configs = ((.configs // [])
    | map(select((.key // "") | startswith("/bench/") | not)))
  | .locks = ((.locks // [])
    | map(select(
      ((.lock_name // "") | startswith("bench-") | not)
      and ((.lock_name // "") | startswith("/bench/") | not)
    )))
  | .transit = ((.transit // [])
    | map(select((.key_name // "") | startswith("bench-") | not)))
  | .workflow = (
      (.workflow // {instances: [], pending: [], in_flight: []})
      | .instances = ((.instances // [])
          | map(select((.workflow_name // "") | startswith("bench-") | not)))
      | (.instances | map(.workflow_id)) as $workflow_keep
      | .pending = ((.pending // []) | map(select(($workflow_keep | index(.)) != null)))
      | .in_flight = ((.in_flight // [])
          | map(select((.workflow_id // "") as $id | ($workflow_keep | index($id)) != null)))
    )
  | .pki = (
      (.pki // {})
      | .issued = ((.issued // [])
          | map(select((.common_name // "") | startswith("bench-") | not)))
      | (.issued | map(.serial_number)) as $serial_keep
      | .revoked = ((.revoked // []) | map(select(($serial_keep | index(.)) != null)))
      | .revocations = ((.revocations // [])
          | map(select((.serial_number // "") as $sn | ($serial_keep | index($sn)) != null)))
      | .acme_orders = ((.acme_orders // [])
          | map(select(
              ((.common_name // "") | startswith("bench-") | not)
              and (((.domains // []) | map(startswith("bench-")) | any) | not)
            )))
    )
' "${tmp_dir}/original_payload.json" >"${tmp_dir}/filtered_payload.json"

echo "restoring filtered payload (benchmark artifacts removed)"
restore_response="$(jq -Rs '{payload_json: .}' "${tmp_dir}/filtered_payload.json" | curl -fsS -X POST "${ENDPOINT}/api/v1/admin/backup/restore" -H 'Content-Type: application/json' --data-binary @-)"

echo "$restore_response" | jq .
echo "benchmark artifact cleanup completed"
