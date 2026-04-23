/**
 * Bedrock Knowledge Base ingestion trigger.
 *
 * After any kb.* operation that writes back to S3, the Bedrock KB's vector index is stale
 * until an ingestion job runs against the data source. We fire StartIngestionJob and return
 * the job ID — we do NOT block on completion. Ingestion takes minutes; the Applier's job is
 * to trigger, audit, and hand off.
 *
 * Config-gated: if `config.aws.knowledge_base_id` AND `config.monitor.kbDataSourceId` are
 * both set, we trigger. If either is missing, we skip with a descriptive reason so the audit
 * trail explains why the chat widget didn't immediately see the KB changes.
 *
 * Why kbDataSourceId lives under `monitor`: the data source ID is an implementation detail
 * of the scanner/applier pipeline, not a widget-facing setting. Grouping it with the other
 * monitor.* fields keeps the shape tidy.
 */

import { BedrockAgentClient, StartIngestionJobCommand } from '@aws-sdk/client-bedrock-agent';

const REGION = process.env.AWS_REGION || 'us-east-1';

export async function triggerBedrockSync(config) {
  const kbId = config?.aws?.knowledge_base_id;
  const dataSourceId = config?.monitor?.kbDataSourceId;

  if (!kbId) {
    return { skipped: true, reason: 'config.aws.knowledge_base_id not set' };
  }
  if (!dataSourceId) {
    return { skipped: true, reason: 'config.monitor.kbDataSourceId not set — add it to enable auto-sync' };
  }

  const client = new BedrockAgentClient({ region: REGION });
  try {
    const result = await client.send(new StartIngestionJobCommand({
      knowledgeBaseId: kbId,
      dataSourceId,
      description: 'kb_proposal_applier post-write sync',
    }));
    return {
      triggered: true,
      knowledgeBaseId: kbId,
      dataSourceId,
      ingestionJobId: result.ingestionJob?.ingestionJobId,
      status: result.ingestionJob?.status,
    };
  } catch (error) {
    // Don't fail the overall apply if sync trigger errors — the KB write succeeded, the user
    // can retry sync manually. Return the error in the audit trail.
    return {
      triggered: false,
      knowledgeBaseId: kbId,
      dataSourceId,
      error: error.message,
    };
  }
}
