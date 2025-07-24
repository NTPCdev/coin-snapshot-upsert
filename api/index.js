// index.js
import fetch from 'node-fetch';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';

dotenv.config();

// Initialize Supabase client
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// Configuration
const COINGECKO_API = process.env.COINGECKO_API_URL || 'https://api.coingecko.com/api/v3/coins/markets';
const VS_CURRENCY = 'usd';
const PER_PAGE = 250;
const TABLE_NAME = process.env.SNAPSHOT_TABLE || 'snapshot';
const CONFLICT_KEY = process.env.SNAPSHOT_CONFLICT_KEY || 'id';

/**
 * Fetch a single page of CoinGecko market data
 * @param {number} page
 */
async function fetchPage(page = 1) {
  const url = `${COINGECKO_API}?vs_currency=${VS_CURRENCY}&order=market_cap_desc&per_page=${PER_PAGE}&page=${page}`;
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`CoinGecko API error: ${res.status}`);
  }
  return res.json();
}

/**
 * Upsert snapshot records into Supabase
 * @param {Array<Object>} data
 */
async function upsertSnapshot(data) {
  const { error } = await supabase
    .from(TABLE_NAME)
    .upsert(data, { onConflict: CONFLICT_KEY });

  if (error) {
    console.error('Supabase upsert error:', error);
    throw error;
  }
  console.log(`Upserted ${data.length} records into ${TABLE_NAME}.`);
}

/**
 * Main handler (e.g., for Vercel serverless function)
 */
export default async function handler(req, res) {
  try {
    let page = 1;
    const allData = [];
    const TARGET = 1250;

    while (allData.length < TARGET) {
      const pageData = await fetchPage(page);
      if (!pageData.length) break;             // no more data
      allData.push(...pageData);

      console.log(
        `Fetched page ${page} (${pageData.length} records). Total so far: ${allData.length}`
      );

      page++;
    }

    // If you overshoot (e.g. last page pushed you past 1000), trim to exactly 1000
    const sliced = allData.slice(0, TARGET);

    // Upsert only the first 1000 coins
    await upsertSnapshot(sliced);

    res
      .status(200)
      .json({ message: `Upserted ${sliced.length} records (max ${TARGET})` });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
}

/*
export default async function handler(req, res) {
  try {
    // You can loop pages if needed
    const page1 = await fetchPage(1);
    await upsertSnapshot(page1);

    res.status(200).json({ message: 'Snapshot upserted successfully', count: page1.length });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
}
*/
