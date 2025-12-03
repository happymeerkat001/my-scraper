// TylerTech County Records Scraper
// Handles: NAVARRO, POLK, VAN ZANDT, LAMAR, KAUFMAN counties (181 properties total)

import puppeteer from 'puppeteer';
import { createReadStream } from 'fs';
import { createObjectCsvWriter } from 'csv-writer';
import csv from 'csv-parser';
import { setTimeout } from 'timers/promises';

// Configuration
const TYLER_COUNTIES = {
  'NAVARRO COUNTY': 'navarrocounty',
  'POLK COUNTY': 'polkcounty',
  'VAN ZANDT COUNTY': 'vanzandtcounty',
  'LAMAR COUNTY': 'lamarcounty',
  'KAUFMAN COUNTY': 'kaufmancounty'
};

const BUSINESS_KEYWORDS = /\b(LLC|INC|CORP|LP|LLP|LTD|CO|COMPANY|TRUST|ESTATE|BANK|INVESTMENTS|PROPERTIES|ENTERPRISES|MINISTRIES)\b/i;

const NOISE_PATTERNS = [
  /\b(DECEASED|ESTATE OF|TRUSTEE|AS TRUSTEE|EXECUTOR OF|EXECUTRIX|HEIR|HEIRS OF)\b/gi,
  /\b(INDEPENDENT SCHOOL DISTRICT|SCHOOL DISTRICT|ISD|COUNTY SHERIFF|SHERIFF'?S DEED)\b/gi,
  /\s+COUNTY(?!\s+CLERK)/gi,
  /,?\s*(ET AL|AKA|DBA|FKA|A\/K\/A|D\/B\/A|F\/K\/A).*$/i
];

const RETRY_MAX = 3;
const RETRY_DELAY_MS = 500;
const RATE_LIMIT_MS = 3000;  // Slower for TylerTech
const FETCH_TIMEOUT_MS = 15000;

let browser = null;

// STEP 1: Read CSV file
async function readCSV(path, limit = null) {
  return new Promise((resolve, reject) => {
    const rows = [];
    createReadStream(path)
      .pipe(csv())
      .on('data', row => { if (!limit || rows.length < limit) rows.push(row); })
      .on('end', () => resolve(rows))
      .on('error', reject);
  });
}

// STEP 2: Write CSV file
async function writeCSV(data, path) {
  const csvWriter = createObjectCsvWriter({
    path,
    header: [
      { id: 'uid', title: 'uid' },
      { id: 'address', title: 'address' },
      { id: 'address_source', title: 'address_source' },
      { id: 'county', title: 'county' },
      { id: 'sale_date', title: 'sale_date' },
      { id: 'adjudged_value', title: 'adjudged_value' },
      { id: 'min_bid', title: 'min_bid' },
      { id: 'status', title: 'status' },
      { id: 'sale_type', title: 'sale_type' },
      { id: 'cause_number', title: 'cause_number' },
      { id: 'case_style', title: 'case_style' },
      { id: 'legal_description', title: 'legal_description' },
      { id: 'coordinates', title: 'coordinates' },
      { id: 'sale_notes', title: 'sale_notes' },
      { id: 'vacant_keyword', title: 'vacant_keyword' },
      { id: 'vacant_source', title: 'vacant_source' },
      ...Array.from({ length: 3 }, (_, i) => [
        { id: `row${i + 1}_grantor`, title: `row${i + 1}_grantor` },
        { id: `row${i + 1}_grantee`, title: `row${i + 1}_grantee` },
        { id: `row${i + 1}_docType`, title: `row${i + 1}_docType` },
        { id: `row${i + 1}_recordedDate`, title: `row${i + 1}_recordedDate` },
        { id: `row${i + 1}_docNumber`, title: `row${i + 1}_docNumber` },
        { id: `row${i + 1}_bookVolumePage`, title: `row${i + 1}_bookVolumePage` },
        { id: `row${i + 1}_legalDescription`, title: `row${i + 1}_legalDescription` },
        { id: `row${i + 1}_references`, title: `row${i + 1}_references` }
      ]).flat()
    ]
  });
  await csvWriter.writeRecords(data);
}

// STEP 3: Parse name and generate search variations
function parseName(caseStyle) {
  if (!caseStyle) return { original: '', variations: [] };

  let text = caseStyle.trim();
  NOISE_PATTERNS.forEach(pattern => { text = text.replace(pattern, ''); });

  const parts = text.split(/\s+(?:VS|V\.?)\s+/i);
  if (parts.length < 2) return { original: '', variations: [] };

  let defendant = parts[1].trim()
    .replace(/\./g, '')
    .replace(/,/g, '')
    .replace(/\s+/g, ' ')
    .trim();

  if (!defendant) return { original: '', variations: [] };

  const isBusiness = BUSINESS_KEYWORDS.test(defendant);
  if (isBusiness) return { original: defendant, variations: [defendant] };

  // Individual: Convert "FIRST LAST" to "LAST FIRST" (TylerTech format)
  const words = defendant.split(/\s+/).filter(w => w && !/^(JR|SR|III|II|IV)$/i.test(w));
  const last = words[words.length - 1];
  const given = words.slice(0, -1);

  const variations = [];
  // TylerTech wants "LAST FIRST" format
  variations.push([last, ...given].join(' '));  // "PRICE CHARLIE"
  if (given.length > 0) {
    variations.push(`${last} ${given[0]}`);  // "PRICE C"
  }
  variations.push(last);  // "PRICE"

  return { original: defendant, variations };
}

// STEP 4: Search TylerTech system
async function searchTylerTech(county, name) {
  const subdomain = TYLER_COUNTIES[county];
  if (!subdomain) {
    console.log(`⚠️  ${county} not supported by TylerTech scraper`);
    return [];
  }

  const url = `https://${subdomain}tx-web.tylerhost.net/web/search/DOCSEARCH144S1`;

  for (let attempt = 0; attempt < RETRY_MAX; attempt++) {
    try {
      if (!browser) {
        browser = await puppeteer.launch({
          headless: true,
          args: ['--no-sandbox', '--disable-setuid-sandbox']
        });
      }

      const page = await browser.newPage();
      await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36');

      console.log(`\n🔗 Navigating to: ${url}`);
      await page.goto(url, { waitUntil: 'networkidle2', timeout: FETCH_TIMEOUT_MS });

      // Wait for form to load
      await page.waitForSelector('#field_BothNamesID', { timeout: 10000 });

      // Enter name in "Both Names" field
      console.log(`✏️  Entering name: "${name}"`);
      await page.type('#field_BothNamesID', name);

      // Click search button
      console.log(`🔍 Clicking search...`);
      await page.click('#searchButton');

      // Wait for results page to load
      await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 15000 });

      // Wait a bit more for results to populate
      await new Promise(r => setTimeout(r, 3000));

      // Extract results from the page
      const records = await page.evaluate(() => {
        const results = [];

        // TylerTech uses a list of result cards
        const resultCards = document.querySelectorAll('.ss-result, [data-document], .document-card, .result-item');

        if (resultCards.length === 0) {
          // Try alternative selector for document rows
          const rows = document.querySelectorAll('[class*="document"], [class*="result-row"], li[data-role="listview"]');
          console.log(`Found ${rows.length} potential result rows`);
        }

        // For now, return empty array - we need to inspect actual HTML structure
        // This will be filled in after manual inspection
        return results;
      });

      console.log(`✓ Found ${records.length} records`);
      await page.close();

      return records;

    } catch (e) {
      console.log(`⚠️  Attempt ${attempt + 1}/${RETRY_MAX} failed:`, e.message);
      if (attempt < RETRY_MAX - 1) await setTimeout(RETRY_DELAY_MS);
      if (attempt === RETRY_MAX - 1) {
        console.log(`❌ Failed after ${RETRY_MAX} attempts`);
        return [];
      }
    }
  }

  return [];
}

// STEP 5: Enrich row with lien data
function enrichRow(row, records) {
  const out = { ...row };

  for (let i = 0; i < 3; i++) {
    const rec = records[i] || {};
    const prefix = `row${i + 1}_`;
    out[`${prefix}grantor`] = rec.grantor || '';
    out[`${prefix}grantee`] = rec.grantee || '';
    out[`${prefix}docType`] = rec.docType || '';
    out[`${prefix}recordedDate`] = rec.recordedDate || '';
    out[`${prefix}docNumber`] = rec.docNumber || '';
    out[`${prefix}bookVolumePage`] = rec.bookVolumePage || '';
    out[`${prefix}legalDescription`] = rec.legalDescription || '';
    out[`${prefix}references`] = rec.references || '';
  }

  return out;
}

// STEP 6: Main orchestrator
async function main() {
  const inputFile = process.env.INPUT_FILE || 'texas-future-sales.csv';
  const outputFile = process.env.OUTPUT_FILE || 'texas-sales-liens-tyler.csv';
  const limit = parseInt(process.env.TEST_LIMIT || '0', 10) || null;

  console.log(`Reading ${inputFile}...`);
  const rows = await readCSV(inputFile, limit);

  // Filter to only TylerTech counties
  const tylerRows = rows.filter(row => TYLER_COUNTIES[row.county]);
  console.log(`Processing ${tylerRows.length} TylerTech county rows (filtered from ${rows.length} total)\n`);

  const results = [];
  let success = 0;

  for (let i = 0; i < tylerRows.length; i++) {
    const row = tylerRows[i];
    const nameObj = parseName(row.case_style);

    console.log(`\n[${i + 1}/${tylerRows.length}] ${row.county} | ${nameObj.original}`);
    console.log(`📋 Name variations:`, nameObj.variations);

    if (!nameObj.variations.length) {
      results.push(enrichRow(row, []));
      continue;
    }

    // Try first variation only for now
    const records = await searchTylerTech(row.county, nameObj.variations[0]);
    results.push(enrichRow(row, records));

    if (records.length > 0) success++;

    await setTimeout(RATE_LIMIT_MS);
  }

  await writeCSV(results, outputFile);
  console.log(`\n✓ Wrote ${results.length} rows to ${outputFile}`);
  console.log(`✓ Success: ${success}/${results.length} (${((success / results.length) * 100).toFixed(1)}%)`);

  // Cleanup browser
  if (browser) {
    await browser.close();
    browser = null;
  }
}

main().catch(async (err) => {
  console.error(err);
  if (browser) await browser.close();
  process.exit(1);
});
