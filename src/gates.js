// Quality gates. One streaming pass over the staged records file collects
// stats; gates are then pure functions of those stats. A staged build may be
// promoted to a sealed snapshot only when EVERY gate passes.
import { createReadStream } from 'node:fs';
import readline from 'node:readline';

const REQUIRED_FIELDS = ['id', 'version', 'name', 'category', 'value'];
const NULL_RATIO_THRESHOLD = 0.01;

export async function analyzeRecords(recordsPath, expectedVersion) {
  const stats = {
    recordCount: 0,
    badLines: 0,
    missingFields: 0,
    nullFields: 0,
    totalFields: 0,
    duplicateIds: 0,
    staleVersions: 0,
  };
  const seen = new Set();
  const rl = readline.createInterface({ input: createReadStream(recordsPath), crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line.trim()) continue;
    let rec;
    try {
      rec = JSON.parse(line);
    } catch {
      stats.badLines++;
      continue;
    }
    stats.recordCount++;
    if (seen.has(rec.id)) stats.duplicateIds++;
    else seen.add(rec.id);
    for (const field of REQUIRED_FIELDS) {
      stats.totalFields++;
      if (!(field in rec)) stats.missingFields++;
      else if (rec[field] === null || rec[field] === '') stats.nullFields++;
    }
    if (rec.version !== expectedVersion) stats.staleVersions++;
  }
  return stats;
}

export function evaluateGates(stats, { minRecords }) {
  const nullRatio = stats.totalFields > 0 ? stats.nullFields / stats.totalFields : 1;
  return [
    {
      name: 'row-count',
      passed: stats.recordCount >= minRecords,
      detail: `${stats.recordCount} rows (minimum ${minRecords})`,
    },
    {
      name: 'parse-integrity',
      passed: stats.badLines === 0,
      detail: stats.badLines === 0 ? 'all lines valid JSON' : `${stats.badLines} unparseable lines`,
    },
    {
      name: 'schema-completeness',
      passed: stats.missingFields === 0,
      detail:
        stats.missingFields === 0
          ? `all ${REQUIRED_FIELDS.length} required fields present`
          : `${stats.missingFields} missing required fields`,
    },
    {
      name: 'null-ratio',
      passed: nullRatio < NULL_RATIO_THRESHOLD,
      detail: `${(nullRatio * 100).toFixed(2)}% null fields (threshold ${NULL_RATIO_THRESHOLD * 100}%)`,
    },
    {
      name: 'uniqueness',
      passed: stats.duplicateIds === 0,
      detail: stats.duplicateIds === 0 ? 'no duplicate ids' : `${stats.duplicateIds} duplicate ids`,
    },
    {
      name: 'version-consistency',
      passed: stats.staleVersions === 0,
      detail:
        stats.staleVersions === 0
          ? 'all rows belong to this build'
          : `${stats.staleVersions} stale rows from an older build`,
    },
  ];
}
