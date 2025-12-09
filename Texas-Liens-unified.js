// Unified Texas Liens Scraper
// Uses driver registry to handle multiple county record systems

import { createReadStream } from 'fs';
import { createObjectCsvWriter } from 'csv-writer';
import csv from 'csv-parser';
import { setTimeout } from 'timers/promises';
import { PublicSearchDriver } from './drivers/publicsearch.js';
import { TylerDriver } from './drivers/tyler.js';
import { OdysseyDriver } from './drivers/odyssey.js';
import { KofileDriver } from './drivers/kofile.js';
import { detectSystem } from './utils/systemDetector.js';

// Driver Registry
const drivers = {
  'publicsearch': new PublicSearchDriver(),
  'tyler': new TylerDriver(),
  'odyssey': new OdysseyDriver(),
  'kofile': new KofileDriver()
};

// Configuration
const RATE_LIMIT_MS = 2000;

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
      { id: 'scrape_status', title: 'scrape_status' },
      { id: 'scrape_error', title: 'scrape_error' },
      { id: 'driver_used', title: 'driver_used' },
      { id: 'matched_variation', title: 'matched_variation' },
      { id: 'parsed_name', title: 'parsed_name' },
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
function parseName(caseStyle, driverType = 'default') {
  if (!caseStyle) return { original: '', variations: [] };

  // Split on VS to extract defendant
  const text = caseStyle.trim();
  const parts = text.split(/\s+(?:VS|V\.?)\s+/i);
  if (parts.length < 2) return { original: '', variations: [] };

  let defendant = parts[1].trim();

  // Remove ET AL and everything after it
  defendant = defendant.replace(/,?\s*ET AL.*$/i, '');

  // Remove noise patterns (but keep the name core)
  defendant = defendant
    .replace(/\b(DECEASED|ESTATE OF|EXECUTOR OF|EXECUTRIX|HEIR|HEIRS OF)\b/gi, '')
    .replace(/\b(INDEPENDENT SCHOOL DISTRICT|SCHOOL DISTRICT|ISD|COUNTY SHERIFF|SHERIFF'?S DEED)\b/gi, '')
    .replace(/,?\s*(AKA|DBA|FKA|A\/K\/A|D\/B\/A|F\/K\/A).*$/i, '')
    .replace(/,?\s*AS TRUSTEE.*$/i, '')
    .replace(/\./g, '')
    .replace(/,/g, '')
    .replace(/\s+/g, ' ')
    .trim();

  if (!defendant) return { original: '', variations: [] };

  // Check for business entities (refined: exclude personal trusts)
  const BUSINESS_KEYWORDS = /\b(LLC|INC|CORP|CORPORATION|LP|LLP|LTD|CO|COMPANY|BANK|INVESTMENTS|PROPERTIES|ENTERPRISES|MINISTRIES)\b/i;
  if (BUSINESS_KEYWORDS.test(defendant)) {
    return { original: defendant, variations: [defendant] };
  }

  // Individual name parsing
  // Remove suffixes from word list for parsing
  const words = defendant.split(/\s+/).filter(w => w && !/^(JR|SR|III|II|IV|V)$/i.test(w));

  if (words.length === 0) return { original: defendant, variations: [] };
  if (words.length === 1) return { original: defendant, variations: [words[0]] };

  // Assume case style format: "LAST FIRST [MIDDLE]" (most common in TX court records)
  const lastName = words[0];
  const fullName = words.join(' ');

  const variations = [];

  switch (driverType) {
    case 'publicsearch':
    case 'tyler':
    case 'kofile':
    case 'odyssey':
    default:
      // All drivers: full name first, then last name as fallback
      variations.push(fullName);
      variations.push(lastName);
      break;
  }

  return { original: defendant, variations };
}

// STEP 4: Find appropriate driver for county
function findDriver(county) {
  for (const [name, driver] of Object.entries(drivers)) {
    if (driver.canHandle(county)) {
      return { name, driver };
    }
  }
  return null;
}

// STEP 5: Enrich row with lien data
function enrichRow(row, records, metadata = {}) {
  const out = { ...row };

  // Add scrape metadata
  out.scrape_status = metadata.status || 'unknown';
  out.scrape_error = metadata.error || '';
  out.driver_used = metadata.driver || 'none';
  out.matched_variation = metadata.matchedVariation || '';
  out.parsed_name = metadata.parsedName || '';

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
  const outputFile = process.env.OUTPUT_FILE || 'texas-sales-liens-unified.csv';
  const limit = parseInt(process.env.TEST_LIMIT || '0', 10) || null;

  console.log(`\n🚀 Unified Texas Liens Scraper`);
  console.log(`📂 Input: ${inputFile}`);
  console.log(`📝 Output: ${outputFile}`);
  console.log(`🔧 Drivers loaded: ${Object.keys(drivers).join(', ')}\n`);

  console.log(`Reading ${inputFile}...`);
  const rows = await readCSV(inputFile, limit);
  console.log(`Processing ${rows.length} rows\n`);

  const results = [];
  const stats = {
    total: 0,
    success: 0,
    noRecords: 0,
    failed: 0,
    parseFailed: 0,
    unsupported: 0,
    error: 0
  };
  const unsupportedCounties = new Set();
  const driverStats = {};
  const failedRows = [];

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    stats.total++;

    try {
      const county = row.county;

      // Find appropriate driver
      const driverInfo = findDriver(county);

      if (!driverInfo) {
        console.log(`\n[${i + 1}/${rows.length}] ⚠️  ${county} - No driver available`);
        stats.unsupported++;
        unsupportedCounties.add(county);
        const enriched = enrichRow(row, [], { status: 'unsupported', error: 'No driver for county', driver: 'none' });
        results.push(enriched);
        failedRows.push(enriched);
        continue;
      }

      const { name: driverName, driver } = driverInfo;

      // Track driver usage
      driverStats[driverName] = (driverStats[driverName] || 0) + 1;

      console.log(`\n[${i + 1}/${rows.length}] 🏛️  ${county} (using ${driverName} driver)`);

      const nameObj = parseName(row.case_style, driverName);

      if (!nameObj.variations.length) {
        console.log(`⚠️  Could not parse name from: "${row.case_style}"`);
        stats.parseFailed++;
        const enriched = enrichRow(row, [], { status: 'parse_failed', error: 'Could not parse defendant name', driver: driverName });
        results.push(enriched);
        failedRows.push(enriched);
        continue;
      }

      console.log(`👤 Name: ${nameObj.original}`);
      console.log(`📋 Variations: ${nameObj.variations.join(', ')}`);

      // Try each name variation with retry logic
      let records = [];
      let searchError = null;
      let matchedVariation = '';

      for (let v = 0; v < nameObj.variations.length && records.length === 0; v++) {
        console.log(`\n🔍 Trying variation ${v + 1}/${nameObj.variations.length}: "${nameObj.variations[v]}"`);

        // Retry up to 3 times with exponential backoff
        for (let attempt = 1; attempt <= 3; attempt++) {
          try {
            records = await driver.search(county, nameObj.variations[v]);

            if (records.length > 0) {
              console.log(`✓ Found ${records.length} records`);
              matchedVariation = nameObj.variations[v];
              break;
            }
            // No records found, move to next variation
            break;
          } catch (e) {
            searchError = e.message;
            if (attempt < 3) {
              const delay = Math.pow(2, attempt) * 1000; // 2s, 4s, 8s
              console.log(`⚠️  Attempt ${attempt}/3 failed: ${searchError}`);
              console.log(`   Retrying in ${delay / 1000}s...`);
              await setTimeout(delay);
            } else {
              console.log(`❌ All retry attempts failed: ${searchError}`);
            }
          }
        }

        if (records.length > 0) break;
      }

      // Determine status
      let status;
      if (records.length > 0) {
        status = 'success';
        stats.success++;
      } else if (searchError) {
        status = 'failed';
        stats.failed++;
      } else {
        status = 'no_records';
        stats.noRecords++;
      }

      const enriched = enrichRow(row, records, {
        status,
        error: searchError || '',
        driver: driverName,
        matchedVariation,
        parsedName: nameObj.original
      });
      results.push(enriched);

      if (status !== 'success') {
        failedRows.push(enriched);
      }

    } catch (e) {
      console.log(`❌ Unhandled error for row ${i + 1}: ${e.message}`);
      stats.error++;
      const enriched = enrichRow(row, [], {
        status: 'error',
        error: `Unhandled: ${e.message}`,
        driver: 'unknown'
      });
      results.push(enriched);
      failedRows.push(enriched);
    }

    await setTimeout(RATE_LIMIT_MS);
  }

  // Cleanup all drivers
  for (const driver of Object.values(drivers)) {
    await driver.cleanup();
  }

  await writeCSV(results, outputFile);

  // Write failed records CSV if there are any
  const failedFile = outputFile.replace(/\.csv$/, '-failed.csv');
  if (failedRows.length > 0) {
    await writeCSV(failedRows, failedFile);
  }

  console.log(`\n${'='.repeat(60)}`);
  console.log(`📊 RESULTS SUMMARY`);
  console.log(`${'='.repeat(60)}`);
  console.log(`\n📁 Output Files:`);
  console.log(`   Main:   ${outputFile} (${results.length} rows)`);
  if (failedRows.length > 0) {
    console.log(`   Failed: ${failedFile} (${failedRows.length} rows)`);
  }

  console.log(`\n📈 Status Breakdown:`);
  console.log(`   ✓ Success:      ${stats.success.toString().padStart(4)} (${((stats.success / stats.total) * 100).toFixed(1)}%)`);
  console.log(`   ○ No Records:   ${stats.noRecords.toString().padStart(4)} (${((stats.noRecords / stats.total) * 100).toFixed(1)}%)`);
  console.log(`   ✗ Failed:       ${stats.failed.toString().padStart(4)} (${((stats.failed / stats.total) * 100).toFixed(1)}%)`);
  console.log(`   ⚠ Parse Failed: ${stats.parseFailed.toString().padStart(4)} (${((stats.parseFailed / stats.total) * 100).toFixed(1)}%)`);
  console.log(`   ⊘ Unsupported:  ${stats.unsupported.toString().padStart(4)} (${((stats.unsupported / stats.total) * 100).toFixed(1)}%)`);
  console.log(`   ✗ Error:        ${stats.error.toString().padStart(4)} (${((stats.error / stats.total) * 100).toFixed(1)}%)`);

  if (stats.unsupported > 0) {
    console.log(`\n⚠️  Unsupported Counties:`);
    console.log(`   Counties: ${Array.from(unsupportedCounties).sort().join(', ')}`);
  }

  console.log(`\n🔧 Driver Usage:`);
  for (const [driver, count] of Object.entries(driverStats)) {
    console.log(`   ${driver.padEnd(12)}: ${count} properties`);
  }

  console.log(`${'='.repeat(60)}\n`);
}

main().catch(async (err) => {
  console.error(err);
  // Cleanup all drivers on error
  for (const driver of Object.values(drivers)) {
    await driver.cleanup();
  }
  process.exit(1);
});
