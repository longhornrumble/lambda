# picasso-session-archiver

Tier 3 archival for `picasso-session-summaries-{env}`. Triggered by DynamoDB
Streams REMOVE events; writes the `OldImage` to S3 as JSON.

## Architecture

```
picasso-session-summaries-{env}  (90-day TTL)
  └─[DDB Streams: OLD_IMAGE]─►  picasso-session-archiver Lambda
                                  └─►  s3://picasso-archive-{env}/sessions/year=Y/month=M/day=D/{session_id}.json
```

The archive bucket has a 365-day lifecycle on the `sessions/` prefix.

## Filtering

Archives **every** REMOVE event, regardless of `userIdentity`. We do NOT filter
to TTL-only deletes because Phase 2 verification uses direct `delete-item` to
simulate TTL expiry without waiting 48h. Manual deletes outside tests are rare
in this table; archiving them is harmless. Re-evaluate before Phase 6 (prod
mirror).

## Runtime

- Runtime: `python3.11` (matches lambda repo CI)
- Memory: 256 MB (small JSON serialization workload)
- Timeout: 30 s
- Env vars:
  - `ARCHIVE_BUCKET` — destination S3 bucket name (e.g. `picasso-archive-staging`)

## IAM (minimal)

- `dynamodb:DescribeStream`, `GetRecords`, `GetShardIterator`, `ListStreams`
  on the stream ARN
- `s3:PutObject` on `arn:aws:s3:::{ARCHIVE_BUCKET}/sessions/*`
- AWSLambdaBasicExecutionRole for CloudWatch Logs

## Event Source Mapping

- StartingPosition: **LATEST** (never TRIM_HORIZON — would reprocess all
  existing rows as deletes the first time it runs)
- BatchSize: 100, MaximumBatchingWindowInSeconds: 5
- MaximumRetryAttempts: 3 (phase-audit B2 — was -1 infinite)
- FunctionResponseTypes: `["ReportBatchItemFailures"]` (phase-audit B4 —
  failed records reported via `batchItemFailures` so only the failed record
  retries, not the whole batch)
- OnFailure: SQS DLQ `picasso-session-archiver-dlq` (14-day retention) —
  records that exhaust the retry budget land here for investigation

## CloudWatch alarm

`picasso-session-archiver-iterator-age` fires when `Maximum(IteratorAge) >
60000ms` for 5 consecutive 1-minute periods. Catches shard backlog before
records start expiring (DDB Streams retain 24h).

## Idempotency

S3 keys are deterministic from `(archive UTC date, session_id)`. Re-processing
the same record overwrites the same key with identical content.

**Known caveat**: if the same `session_id` REMOVE event fires twice across
UTC midnight (e.g. ESM retry spanning midnight), two archive records with
different date partitions can exist for the same session. Acceptable at
current staging scale; revisit before Phase 6 prod mirror.

## Tests

```bash
cd Lambdas/lambda/picasso_session_archiver
python -m pytest test_archiver.py -v
```
