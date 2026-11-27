#!/usr/bin/env bash
# ===========================================================================
# MinIO bootstrap
# ===========================================================================
# Entrypoint of the `minio-init` service. Creates the data-lake bucket, the
# three dataset prefixes, and a service account the platform uses instead of
# the root credentials.
#
# The last step is a NEGATIVE test: the service account must be able to read
# and write inside its own bucket but must NOT be able to create a new one.
# If that refusal does not happen, the script exits non-zero, because a
# least-privilege claim that was never tested is just a comment.
# ===========================================================================
set -euo pipefail

log() { echo "[minio-init] $*"; }

ALIAS=root
PREFIXES=(daily_sales inventory_snapshots customer_activity)

log "waiting for MinIO at ${MINIO_ENDPOINT}"
for attempt in $(seq 1 30); do
    if mc alias set "${ALIAS}" "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1; then
        log "connected"
        break
    fi
    if [ "${attempt}" -eq 30 ]; then
        log "ERROR: could not reach MinIO after 30 attempts"
        exit 1
    fi
    sleep 2
done

# ---------------------------------------------------------------------------
# Bucket + prefixes
# ---------------------------------------------------------------------------
if mc ls "${ALIAS}/${MINIO_BUCKET}" >/dev/null 2>&1; then
    log "bucket ${MINIO_BUCKET} already exists"
else
    mc mb "${ALIAS}/${MINIO_BUCKET}"
    log "created bucket ${MINIO_BUCKET}"
fi

# Object stores have no real directories. Writing a zero-byte marker makes the
# intended layout visible in the console before any data lands.
for prefix in "${PREFIXES[@]}"; do
    if ! mc stat "${ALIAS}/${MINIO_BUCKET}/${prefix}/.keep" >/dev/null 2>&1; then
        echo -n "" | mc pipe "${ALIAS}/${MINIO_BUCKET}/${prefix}/.keep" >/dev/null
        log "created prefix ${prefix}/"
    fi
done

# ---------------------------------------------------------------------------
# Least-privilege service account
# ---------------------------------------------------------------------------
# A policy scoped to this bucket only. The platform never uses the root user.
POLICY_FILE=/tmp/dq-platform-policy.json
cat > "${POLICY_FILE}" <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::${MINIO_BUCKET}",
        "arn:aws:s3:::${MINIO_BUCKET}/*"
      ]
    }
  ]
}
JSON

if mc admin user svcacct info "${ALIAS}" "${MINIO_ACCESS_KEY}" >/dev/null 2>&1; then
    log "service account ${MINIO_ACCESS_KEY} already exists"
else
    mc admin user svcacct add \
        --access-key "${MINIO_ACCESS_KEY}" \
        --secret-key "${MINIO_SECRET_KEY}" \
        --policy "${POLICY_FILE}" \
        "${ALIAS}" "${MINIO_ROOT_USER}" >/dev/null
    log "created service account ${MINIO_ACCESS_KEY} scoped to ${MINIO_BUCKET}"
fi

# ---------------------------------------------------------------------------
# Prove the privilege boundary
# ---------------------------------------------------------------------------
mc alias set svc "${MINIO_ENDPOINT}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}" >/dev/null

log "positive test: service account can list its own bucket"
if ! mc ls "svc/${MINIO_BUCKET}" >/dev/null 2>&1; then
    log "ERROR: service account cannot list ${MINIO_BUCKET} - policy is too narrow"
    exit 1
fi

log "positive test: service account can write and read back an object"
echo "platform-check" | mc pipe "svc/${MINIO_BUCKET}/.platform-check" >/dev/null
if [ "$(mc cat "svc/${MINIO_BUCKET}/.platform-check")" != "platform-check" ]; then
    log "ERROR: round trip through the service account failed"
    exit 1
fi
mc rm "svc/${MINIO_BUCKET}/.platform-check" >/dev/null

log "negative test: service account must NOT be able to create a bucket"
if mc mb "svc/should-not-be-creatable" >/dev/null 2>&1; then
    log "ERROR: the service account created a bucket. The policy is too broad."
    mc rb --force "svc/should-not-be-creatable" >/dev/null 2>&1 || true
    exit 1
fi
log "refused, as required"

log "done"
