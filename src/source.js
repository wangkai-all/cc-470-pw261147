// Simulated upstream batch source. Each extract is a FULL replacement dataset
// tagged with the build sequence, so every row is traceable to exactly one
// snapshot version end-to-end (this is what makes mixed-version reads
// detectable and therefore preventable).

const CATEGORIES = ['alpha', 'beta', 'gamma', 'delta'];

export function* generateRecords({ seq, count, injectBadData = false }) {
  for (let i = 0; i < count; i++) {
    const rec = {
      id: `r-${i}`,
      version: seq,
      name: `item-${i}-v${seq}`,
      category: CATEGORIES[i % CATEGORIES.length],
      value: (i * 37 + seq * 11) % 1000,
      updatedAt: new Date().toISOString(),
    };
    if (injectBadData) {
      if (i % 17 === 0) rec.name = null; // null fields -> null-ratio gate
      if (i % 23 === 0 && i > 0) rec.id = 'r-0'; // duplicate ids -> uniqueness gate
      if (i % 29 === 0) delete rec.category; // missing field -> schema gate
      if (i % 31 === 0) rec.version = seq - 1; // stale rows -> version-consistency gate
    }
    yield rec;
  }
}
