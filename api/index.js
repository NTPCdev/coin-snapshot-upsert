import fetch from 'node-fetch';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, resolve } from 'path';

// Load environment variables from .env file
dotenv.config({ path: resolve(dirname(fileURLToPath(import.meta.url)), '../.env') });

// Validate required environment variables
const { SUPABASE_URL, SUPABASE_SERVICE_KEY, SNAPSHOT_TABLE, SNAPSHOT_CONFLICT_KEY } = process.env;
if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('❌ Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in environment');
  process.exit(1);
}

// Initialize Supabase client
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  // optional: adjust timeout, schema, logging, etc.
  global: { headers: { 'x-client-platform': 'node' } }
});

// Configuration
const COINGECKO_API = process.env.COINGECKO_API_URL || 'https://api.coingecko.com/api/v3/coins/markets';
const VS_CURRENCY = process.env.VS_CURRENCY || 'usd';
const PER_PAGE = parseInt(process.env.PER_PAGE, 10) || 250;
const TARGET = parseInt(process.env.TARGET, 10) || 1250;
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE, 10) || 200;
const TABLE_NAME = SNAPSHOT_TABLE || 'snapshot';
const CONFLICT_KEY = SNAPSHOT_CONFLICT_KEY || 'id';

// Utilities
/**
 * Deduplicate data by key
 */
function deduplicateByKey(data, key) {
  const map = new Map();
  for (const record of data) {
    map.set(record[key], record);
  }
  return Array.from(map.values());
}

/**
 * Find duplicate IDs in data
 */
function findDuplicateIds(data, key) {
  const seen = new Set();
  const dupes = new Set();
  for (const record of data) {
    const id = record[key];
    if (seen.has(id)) dupes.add(id);
    else seen.add(id);
  }
  return Array.from(dupes);
}

/**
 * Chunk an array into smaller arrays
 */
function chunkArray(array, size) {
  const chunks = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

/**
 * Fetch a single page from CoinGecko
 */
async function fetchPage(page = 1) {
  const url = `${COINGECKO_API}?vs_currency=${VS_CURRENCY}&order=market_cap_desc&per_page=${PER_PAGE}&page=${page}`;
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`CoinGecko API error: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/**
 * Upsert a batch of records into Supabase
 */
async function upsertBatch(batch) {
  const { data, error } = await supabase
    .from(TABLE_NAME)
    .upsert(batch, { onConflict: CONFLICT_KEY });

  if (error) {
    console.error('❌ Upsert batch error:', error);
    throw error;
  }
  console.log(`✅ Batch upserted ${batch.length} records.`);
  return data;
}

/**
 * Main handler: fetch, dedupe, and upsert
 */
export default async function handler(req, res) {
  try {
    console.log('🔄 Starting snapshot fetch...');
    const allData = [];
    let page = 1;

    // Fetch until TARGET or no more data
    while (allData.length < TARGET) {
      const pageData = await fetchPage(page);
      if (!Array.isArray(pageData) || pageData.length === 0) break;

      console.log(`Fetched page ${page}: ${pageData.length} records.`);
      allData.push(...pageData);
      page++;
    }

    // Trim to TARGET
    const sliced = allData.slice(0, TARGET);
    console.log(`Total records fetched (trimmed to ${TARGET}): ${sliced.length}`);

    // Detect duplicates
    const dupes = findDuplicateIds(sliced, CONFLICT_KEY);
    if (dupes.length > 0) {
      console.warn(`⚠️ Found ${dupes.length} duplicate IDs. Skipping these IDs:`);
      console.warn(dupes);
    }

    // Deduplicate data
    const deduplicated = deduplicateByKey(sliced, CONFLICT_KEY);
    console.log(`Deduplicated records: removed ${sliced.length - deduplicated.length} duplicates.`);

    // Chunk and upsert
    const chunks = chunkArray(deduplicated, BATCH_SIZE);
    console.log(`Uploading in ${chunks.length} batch(es) of up to ${BATCH_SIZE} records each.`);

    for (const [i, batch] of chunks.entries()) {
      console.log(`Upserting batch ${i + 1}/${chunks.length}...`);
      await upsertBatch(batch);
    }

    console.log('🎉 All batches upserted successfully.');

    // Send response
    if (res) {
      res.status(200).json({ message: 'Snapshot upsert complete', total: deduplicated.length });
    }
  } catch (err) {
    console.error('🚨 Handler error:', err);
    if (res) {
      res.status(500).json({ error: err.message });
    }
  }
}

// Local-run compatibility
const currentModule = fileURLToPath(import.meta.url);
if (process.argv[1] === currentModule) {
  handler().catch(err => {
    console.error('🚨 Local run error:', err);
    process.exit(1);
  });
}
