// Task-first documentation map.
//
// The repository keeps stable page URLs, while this navigation groups them by
// the job a reader is trying to finish. A page should appear once so the
// previous and next links form one deliberate reading path.

export interface SidebarDoc {
  type: 'doc';
  slug: string;
  label?: string;
}

export interface SidebarCategory {
  type: 'category';
  label: string;
  slug?: string;
  items: SidebarItem[];
}

export type SidebarItem = SidebarDoc | SidebarCategory;

export const sidebar: SidebarItem[] = [
  {
    type: 'category',
    label: 'Make a first run',
    items: [
      { type: 'doc', slug: 'getting_started/overview', label: 'What Synor does' },
      { type: 'doc', slug: 'getting_started/quickstart', label: 'Local note catalog' },
      { type: 'doc', slug: 'getting_started/installation', label: 'Install locally' },
      { type: 'doc', slug: 'getting_started/ai_coding_agents', label: 'Give an agent context' },
    ],
  },
  {
    type: 'category',
    label: 'Design the work',
    items: [
      { type: 'doc', slug: 'programming_guide/core_concepts', label: 'The second-run model' },
      { type: 'doc', slug: 'programming_guide/app', label: 'Set the run boundary' },
      { type: 'doc', slug: 'programming_guide/processing_component', label: 'Choose work units' },
      { type: 'doc', slug: 'programming_guide/function', label: 'Make work reusable' },
      { type: 'doc', slug: 'programming_guide/target_state', label: 'Declare outcomes' },
      { type: 'doc', slug: 'programming_guide/context', label: 'Share resources' },
      { type: 'doc', slug: 'programming_guide/serialization', label: 'Persist Python values' },
      { type: 'doc', slug: 'programming_guide/sdk_overview', label: 'Navigate the SDK' },
    ],
  },
  {
    type: 'category',
    label: 'Operate the run',
    items: [
      { type: 'doc', slug: 'programming_guide/controlled_runs', label: 'Review before apply' },
      { type: 'doc', slug: 'programming_guide/trustworthy_execution', label: 'Inspect trusted runs' },
      { type: 'doc', slug: 'programming_guide/index_integrity', label: 'Audit index integrity' },
      { type: 'doc', slug: 'programming_guide/provable_index_revocation', label: 'Prove index revocation' },
      { type: 'doc', slug: 'programming_guide/live_mode', label: 'Stay live' },
      { type: 'doc', slug: 'advanced_topics/concurrency_control', label: 'Limit concurrent work' },
      { type: 'doc', slug: 'advanced_topics/timeouts', label: 'Set deadlines' },
      { type: 'doc', slug: 'advanced_topics/progress_monitoring', label: 'Observe progress' },
      { type: 'doc', slug: 'advanced_topics/exception_handlers', label: 'Handle failures' },
      { type: 'doc', slug: 'advanced_topics/memoization_keys', label: 'Control reuse keys' },
      { type: 'doc', slug: 'common_resources/rate_limiting', label: 'Respect service budgets' },
      { type: 'doc', slug: 'advanced_topics/multiple_environments', label: 'Isolate environments' },
      { type: 'doc', slug: 'advanced_topics/internal_storage', label: 'Tune local storage' },
    ],
  },
  {
    type: 'category',
    label: 'Shape the data',
    slug: 'common_resources',
    items: [
      { type: 'doc', slug: 'common_resources/data_types', label: 'Files, paths, and chunks' },
      { type: 'doc', slug: 'common_resources/vector_schema', label: 'Vector columns' },
      { type: 'doc', slug: 'common_resources/id_generation', label: 'Stable identifiers' },
      { type: 'doc', slug: 'common_resources/live_map', label: 'Live keyed collections' },
    ],
  },
  {
    type: 'category',
    label: 'Connect systems',
    slug: 'connectors',
    items: [
      { type: 'doc', slug: 'connectors/localfs', label: 'Local filesystem' },
      { type: 'doc', slug: 'connectors/amazon_s3', label: 'Amazon S3' },
      { type: 'doc', slug: 'connectors/azure_blob', label: 'Azure Blob Storage' },
      { type: 'doc', slug: 'connectors/google_drive', label: 'Google Drive' },
      { type: 'doc', slug: 'connectors/oci_object_storage', label: 'OCI Object Storage' },
      { type: 'doc', slug: 'connectors/postgres', label: 'Postgres' },
      { type: 'doc', slug: 'connectors/sqlite', label: 'SQLite' },
      { type: 'doc', slug: 'connectors/bigquery', label: 'BigQuery' },
      { type: 'doc', slug: 'connectors/snowflake', label: 'Snowflake' },
      { type: 'doc', slug: 'connectors/doris', label: 'Apache Doris' },
      { type: 'doc', slug: 'connectors/lancedb', label: 'LanceDB' },
      { type: 'doc', slug: 'connectors/qdrant', label: 'Qdrant' },
      { type: 'doc', slug: 'connectors/turbopuffer', label: 'Turbopuffer' },
      { type: 'doc', slug: 'connectors/zvec', label: 'zvec' },
      { type: 'doc', slug: 'connectors/valkey', label: 'Valkey' },
      { type: 'doc', slug: 'connectors/neo4j', label: 'Neo4j' },
      { type: 'doc', slug: 'connectors/falkordb', label: 'FalkorDB' },
      { type: 'doc', slug: 'connectors/surrealdb', label: 'SurrealDB' },
      { type: 'doc', slug: 'connectors/kafka', label: 'Kafka' },
      { type: 'doc', slug: 'connectors/iggy', label: 'Iggy' },
    ],
  },
  {
    type: 'category',
    label: 'Add transformations',
    slug: 'ops',
    items: [
      { type: 'doc', slug: 'ops/text', label: 'Split text' },
      { type: 'doc', slug: 'ops/sentence_transformers', label: 'Create embeddings' },
      { type: 'doc', slug: 'ops/litellm', label: 'Call language models' },
      { type: 'doc', slug: 'ops/entity_resolution', label: 'Resolve entities' },
    ],
  },
  {
    type: 'category',
    label: 'Extend the engine',
    slug: 'advanced_topics',
    items: [
      { type: 'doc', slug: 'advanced_topics/live_component', label: 'Build a live component' },
      { type: 'doc', slug: 'advanced_topics/custom_target_connector', label: 'Build a target connector' },
      { type: 'doc', slug: 'programming_guide', label: 'Programming guide index' },
    ],
  },
  {
    type: 'category',
    label: 'Look something up',
    items: [
      { type: 'doc', slug: 'cli', label: 'CLI reference' },
      { type: 'doc', slug: 'faq', label: 'Questions and answers' },
    ],
  },
  {
    type: 'category',
    label: 'Work on Synor',
    items: [
      { type: 'doc', slug: 'contributing/setup_dev_environment', label: 'Set up the repository' },
      { type: 'doc', slug: 'contributing/guide', label: 'Contribution guide' },
      { type: 'doc', slug: 'about/community', label: 'Project status' },
    ],
  },
];

export function flatten(
  items: SidebarItem[] = sidebar,
): Array<{ slug: string; label?: string }> {
  const out: Array<{ slug: string; label?: string }> = [];
  const visit = (list: SidebarItem[]) => {
    for (const item of list) {
      if (item.type === 'doc') {
        out.push({ slug: item.slug, label: item.label });
      } else {
        if (item.slug) out.push({ slug: item.slug, label: item.label });
        visit(item.items);
      }
    }
  };
  visit(items);
  return out;
}
