"""
picasso-session-archiver
-----------------------
DynamoDB Streams handler for picasso-session-summaries-{env}.

On REMOVE events (TTL-driven or manual delete-item), writes the row's
OldImage as JSON to S3 partitioned by UTC date of archive write.

Trigger:  DynamoDB Streams (OLD_IMAGE view)
Sink:     s3://{ARCHIVE_BUCKET}/sessions/year=YYYY/month=MM/day=DD/{session_id}.json

NOTE on event filtering: we intentionally do NOT filter to TTL-only deletes
(userIdentity.principalId == 'dynamodb.amazonaws.com'). The plan's Phase 2
verification mechanic uses direct `delete-item` to simulate TTL expiry without
waiting 48h. Filtering on userIdentity would reject those test deletes. Manual
deletes are rare in this table outside of tests; archiving them is harmless.
Re-evaluate before Phase 6 (prod mirror).

Idempotency: S3 keys are deterministic from (archive UTC date, session_id).
Re-processing the same record overwrites the same key.
"""
import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.types import TypeDeserializer

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Cold-start assertion: ARCHIVE_BUCKET must be set or every invocation
# fails to write. Log loudly at module load (mirrors the pattern used in
# Master_Function_Staging/analytics_writer.py for SESSION_SUMMARIES_TABLE).
ARCHIVE_BUCKET = os.environ.get("ARCHIVE_BUCKET")
if not ARCHIVE_BUCKET:
    logger.critical(
        "archiver_misconfiguration: ARCHIVE_BUCKET env var not set; "
        "every REMOVE event will fail to archive"
    )

s3 = boto3.client("s3")
_deser = TypeDeserializer()


def _deserialize_image(image):
    return {k: _deser.deserialize(v) for k, v in image.items()}


def _json_default(obj):
    if isinstance(obj, Decimal):
        return float(obj) if obj % 1 else int(obj)
    if isinstance(obj, set):
        return list(obj)
    if isinstance(obj, (bytes, bytearray)):
        return obj.decode("utf-8", errors="replace")
    return str(obj)


def _make_key(item):
    now = datetime.now(timezone.utc)
    session_id = item.get("session_id") or item.get("sessionId") or "unknown"
    return (
        f"sessions/year={now.year:04d}/month={now.month:02d}/day={now.day:02d}/"
        f"{session_id}.json"
    )


def lambda_handler(event, context):
    records = event.get("Records") or []
    archived = 0
    skipped_non_remove = 0
    skipped_no_old_image = 0

    for record in records:
        if record.get("eventName") != "REMOVE":
            skipped_non_remove += 1
            continue

        old_image = (record.get("dynamodb") or {}).get("OldImage")
        if not old_image:
            logger.warning(
                "REMOVE without OldImage; skipping. eventID=%s",
                record.get("eventID"),
            )
            skipped_no_old_image += 1
            continue

        item = _deserialize_image(old_image)
        key = _make_key(item)
        body = json.dumps(item, default=_json_default).encode("utf-8")

        s3.put_object(
            Bucket=ARCHIVE_BUCKET,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        archived += 1
        logger.info(
            "Archived session_id=%s to s3://%s/%s (bytes=%d)",
            item.get("session_id") or item.get("sessionId") or "unknown",
            ARCHIVE_BUCKET,
            key,
            len(body),
        )

    logger.info(
        "Batch summary: archived=%d skipped_non_remove=%d skipped_no_old_image=%d total=%d",
        archived,
        skipped_non_remove,
        skipped_no_old_image,
        len(records),
    )
    return {
        "archived": archived,
        "skipped_non_remove": skipped_non_remove,
        "skipped_no_old_image": skipped_no_old_image,
    }
