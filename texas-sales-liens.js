/**
 * texas-sales-liens.js — Hybrid approach combining clarity + robustness
 * 
 * [1/3] Load CSV + parse defendant names from case_style
 * [2/3] For each row: fetch county lien data (with name fallbacks)
 * [3/3] Merge & save as texas-sales-liens.csv with 6 new lien columns
 * 
 * Run: TEST_LIMIT=2 node texas-sales-liens.js
 */

import fs from 'fs';
import { parse } from 'csv-parse/sync';
import axios from 'axios';
import * as cheerio from 'cheerio';
import { stringify } from 'csv-stringify/sync';

// === CONFIG ===
const COUNTY_MAP = {
  'ANDERSON': 'anderson', 'ANGELINA': 'angelina', 'BEE': 'bee', 'CAMERON': 'cameron',
  'ECTOR': 'ector', 'EL PASO': 'elpaso', 'ERATH': 'erath', 'FORT BEND': 'fortbend',
  'FRIO': 'frio', 'GALVESTON': 'galveston', 'GRAYSON': 'grayson', 'HARRIS': 'harris',
  'HIDALGO': 'hidalgo', 'HUNT': 'hunt', 'JEFFERSON': 'jefferson', 'KERR': 'kerr',
  'LAMPASAS': 'lampasas', 'MAVERICK': 'maverick', 'MCLENNAN': 'mclennan', 'MIDLAND': 'midland',
  'MONTGOMERY': 'montgomery', 'NUECES': 'nueces', 'POTTER': 'potter', 'SAN PATRICIO': 'sanpatricio',
  'SMITH': 'smith', 'TARRANT': 'tarrant', 'TRAVIS': 'travis', 'UPSHUR': 'upshur',
  'UVALDE': 'uvalde', 'VICTORIA': 'victoria', 'WALLER': 'waller', 'WASHINGTON': 'washington',
  'WEBB': 'webb', 'WHARTON': 'wharton', 'WICHITA': 'wichita', 'WILLIAMSON': 'williamson',
  'WISE': 'wise', 'WOOD': 'wood', 'YOUNG': 'young', 'ZAVALA': 'zavala', 'ZAPATA': 'zapata'
};

// Filter to actual liens only (not noise documents)
const LIEN_KEYWORDS = ['TAX LIEN', 'JUDGMENT', 'LIS PENDENS', 'MECHANIC', 'UCC'];
const BUSINESS_KEYWORDS = /\b(LLC|INC|CORP|LP|LTD|BANK|TRUST|ASSOCIATION|PARTNERSHIP)\b/i;

// === STEP 1: Extract defendant name + generate search variations ===
// Input: case_style (e.g., "FRANKSTON ISD VS A C MARTIN")
// Output: { full, variations[], isBusiness, error }
const parseName = (caseStyle) => {
  if (!caseStyle) return { error: 'empty case_style' };
  
  let text = caseStyle
    .replace(/\b(DECEASED|ESTATE OF|ET AL|ET UX)\b/gi, '')
    .replace(/\b(ISD|SCHOOL DISTRICT|COUNTY SHERIFF)\b/gi, '')
    .replace(/\s+COUNTY\s+(OF|CLERK)\b/gi, '').trim();
  
  // Extract defendant (right side of VS)
  const parts = text.split(/\s+VS\s+/i);
  if (parts.length < 2) return { error: 'no VS found' };
  
  const defendant = parts[1].trim().replace(/\.\s*/g, ''); // Remove periods
  if (!defendant) return { error: 'empty defendant' };
  
  const isBusiness = BUSINESS_KEYWORDS.test(defendant);
  const words = defendant.split(/\s+/).filter(w => w);
  
  // Build search variations: full → last+first → last
  const variations = [defendant];
  if (!isBusiness && words.length > 2) {
    variations.push(`${words[0]} ${words[1]}`);
  }
  if (!isBusiness && words.length > 1) {
    variations.push(words[0]);
  }
  
  return { full: defendant, variations, isBusiness, error: null };
};

// === STEP 2: Build county search URL ===
// Input: county name, search string
// Output: URL or null if county not found
const buildSearchURL = (county, searchName) => {
  const slug = COUNTY_MAP[county.replace(/\s+COUNTY$/i, '').trim()];
  if (!slug) return null;
  
  // Dynamic date range (current month)
  const today = new Date().toISOString().split('T')[0].replace(/-/g, '');
  const encoded = encodeURIComponent(`"${searchName.toLowerCase()}"`);
  
  return `https://${slug}.tx.publicsearch.us/results?` +
    `department=RP&keywordSearch=false&limit=50&offset=0&` +
    `recordedDateRange=18000101%2C${today}&` +
    `searchOcrText=false&searchType=quickSearch&` +
    `searchValue=${encoded}&sort=desc&sortBy=recordedDate`;
};

// === STEP 3: Fetch with exponential backoff retry ===
// Input: URL
// Output: HTML string or error
const fetchWithRetry = async (url, maxRetries = 3) => {
  for (let i = 0; i < maxRetries; i++) {
    try {
      const res = await axios.get(url, { timeout: 10000 });
      return res.data;
    } catch (err) {
      if (i === maxRetries - 1) throw err;
      await new Promise(r => setTimeout(r, 500 * (i + 1)));
    }
  }
};

// === STEP 4: Parse HTML table for liens ===
// Input: HTML string
// Output: Array of { date, type } (max 3)
const parseLiens = (html) => {
  const $ = cheerio.load(html);
  const liens = [];
  
  $('tbody tr, table tr').each((_, row) => {
    if (liens.length >= 3) return;
    
    const cells = $(row).find('td');
    if (cells.length < 4) return;
    
    // Expected columns: Grantor(0), Grantee(1), DocType(2), Date(3)
    const docType = $(cells[2]).text().trim();
    const date = $(cells[3]).text().trim();
    
    // Only capture actual liens
    if (docType && date && isLien(docType)) {
      liens.push({ date, type: docType });
    }
  });
  
  return liens;
};

// Check if docType contains lien keyword
const isLien = (docType) => {
  const upper = docType.toUpperCase();
  return LIEN_KEYWORDS.some(kw => upper.includes(kw));
};

// === MAIN ===
const main = async () => {
  console.log('=== Texas Sales Liens Enrichment ===\n');
  
  // Load CSV
  const input = fs.readFileSync('texas-future-sales.csv', 'utf-8');
  const rows = parse(input, { columns: true });
  const limit = parseInt(process.env.TEST_LIMIT || rows.length, 10);
  const testRows = rows.slice(0, limit);
  
  console.log(`[1/3] Loaded ${testRows.length} rows from CSV\n`);
  
  const enriched = [];
  let successCount = 0;
  const errors = {};
  
  // Process each row
  for (const [index, row] of testRows.entries()) {
    console.log(`[2/3] Row ${index + 1}/${testRows.length}: ${row.county}`);
    
    const nameObj = parseName(row.case_style || '');
    if (nameObj.error) {
      console.log(`  ✗ parse: ${nameObj.error}`);
      for (let j = 1; j <= 3; j++) {
        row[`lien_date_${j}`] = '';
        row[`lien_type_${j}`] = '';
      }
      row.lien_error = nameObj.error;
      enriched.push(row);
      continue;
    }
    
    console.log(`  → "${nameObj.full}"`);
    
    // Try each name variation
    let liens = [];
    let fetchError = null;
    
    for (const variation of nameObj.variations) {
      const url = buildSearchURL(row.county, variation);
      if (!url) {
        fetchError = `county not found: ${row.county}`;
        break;
      }
      
      try {
        const html = await fetchWithRetry(url);
        liens = parseLiens(html);
        if (liens.length > 0) {
          successCount++;
          console.log(`  ✓ found ${liens.length} liens`);
          break;
        }
      } catch (err) {
        fetchError = err.message;
        if (variation === nameObj.variations[nameObj.variations.length - 1]) {
          console.log(`  ✗ fetch: ${fetchError}`);
        }
      }
    }
    
    // Fill lien columns (pad to 3)
    for (let j = 0; j < 3; j++) {
      if (j < liens.length) {
        row[`lien_date_${j + 1}`] = liens[j].date;
        row[`lien_type_${j + 1}`] = liens[j].type;
      } else {
        row[`lien_date_${j + 1}`] = '';
        row[`lien_type_${j + 1}`] = '';
      }
    }
    row.lien_error = fetchError || '';
    
    enriched.push(row);
    
    // Rate limit
    await new Promise(r => setTimeout(r, 2000));
  }
  
  // Save results
  console.log(`\n[3/3] Writing results...`);
  const output = stringify(enriched, { header: true });
  fs.writeFileSync('texas-sales-liens.csv', output);
  
  // Summary metrics
  console.log(`\n✓ Results saved to texas-sales-liens.csv`);
  console.log(`✓ Processed: ${enriched.length} rows`);
  console.log(`✓ Success rate: ${successCount}/${testRows.length} (${(successCount/testRows.length*100).toFixed(1)}%)`);
};

main().catch(err => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
