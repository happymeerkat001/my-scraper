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
import { PolkDriver } from './drivers/polk.js';
import { detectSystem } from './utils/systemDetector.js';

// Pre-compiled regex patterns for parseName (avoid recreation per call)
const RE_VS_SPLIT = /\s+(?:VS|V\.?)\s+/i;
const RE_ET_AL = /,?\s*ET AL.*$/i;
const RE_NOISE_LEGAL = /\b(DECEASED|ESTATE OF|EXECUTOR OF|EXECUTRIX|HEIR|HEIRS OF)\b/gi;
const RE_NOISE_ENTITIES = /\b(INDEPENDENT SCHOOL DISTRICT|SCHOOL DISTRICT|ISD|COUNTY SHERIFF|SHERIFF'?S DEED)\b/gi;
const RE_ALIAS = /,?\s*(AKA|DBA|FKA|A\/K\/A|D\/B\/A|F\/K\/A).*$/i;
const RE_TRUSTEE = /,?\s*AS TRUSTEE.*$/i;
const RE_BUSINESS_KEYWORDS = /\b(LLC|INC|CORP|CORPORATION|LP|LLP|LTD|CO|COMPANY|BANK|INVESTMENTS|PROPERTIES|ENTERPRISES|MINISTRIES)\b/i;
const RE_SUFFIX = /^(JR|SR|III|II|IV|V)$/i;
const RE_PARENTHETICAL = /\s*\([^)]*\)\s*/g;

// Driver Registry
const drivers = {
  'publicsearch': new PublicSearchDriver(),
  'tyler': new TylerDriver(),
  'odyssey': new OdysseyDriver(),
  'kofile': new KofileDriver(),
  'polk': new PolkDriver()
};

// Pre-computed driver entries (avoid Object.entries() per lookup)
const DRIVER_ENTRIES = Object.entries(drivers);

// County → driver cache (avoid repeated lookups for same county)
const driverCache = new Map();

// Configuration
const RATE_LIMIT_MS = 2000;

// Pre-computed CSV header (avoid recreation per writeCSV call)
const CSV_HEADER = [
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
  { id: 'classification_status', title: 'classification_status' },
  { id: 'html_length', title: 'html_length' },
  { id: 'retry_count', title: 'retry_count' },
  { id: 'classification_indicators', title: 'classification_indicators' },
  { id: 'row1_grantor', title: 'row1_grantor' },
  { id: 'row1_grantee', title: 'row1_grantee' },
  { id: 'row1_docType', title: 'row1_docType' },
  { id: 'row1_recordedDate', title: 'row1_recordedDate' },
  { id: 'row1_docNumber', title: 'row1_docNumber' },
  { id: 'row1_bookVolumePage', title: 'row1_bookVolumePage' },
  { id: 'row1_legalDescription', title: 'row1_legalDescription' },
  { id: 'row1_references', title: 'row1_references' },
  { id: 'row2_grantor', title: 'row2_grantor' },
  { id: 'row2_grantee', title: 'row2_grantee' },
  { id: 'row2_docType', title: 'row2_docType' },
  { id: 'row2_recordedDate', title: 'row2_recordedDate' },
  { id: 'row2_docNumber', title: 'row2_docNumber' },
  { id: 'row2_bookVolumePage', title: 'row2_bookVolumePage' },
  { id: 'row2_legalDescription', title: 'row2_legalDescription' },
  { id: 'row2_references', title: 'row2_references' },
  { id: 'row3_grantor', title: 'row3_grantor' },
  { id: 'row3_grantee', title: 'row3_grantee' },
  { id: 'row3_docType', title: 'row3_docType' },
  { id: 'row3_recordedDate', title: 'row3_recordedDate' },
  { id: 'row3_docNumber', title: 'row3_docNumber' },
  { id: 'row3_bookVolumePage', title: 'row3_bookVolumePage' },
  { id: 'row3_legalDescription', title: 'row3_legalDescription' },
  { id: 'row3_references', title: 'row3_references' }
];

// Pre-computed record field keys (avoid template literal recreation per row)
const RECORD_FIELD_KEYS = [
  ['row1_grantor', 'row1_grantee', 'row1_docType', 'row1_recordedDate',
   'row1_docNumber', 'row1_bookVolumePage', 'row1_legalDescription', 'row1_references'],
  ['row2_grantor', 'row2_grantee', 'row2_docType', 'row2_recordedDate',
   'row2_docNumber', 'row2_bookVolumePage', 'row2_legalDescription', 'row2_references'],
  ['row3_grantor', 'row3_grantee', 'row3_docType', 'row3_recordedDate',
   'row3_docNumber', 'row3_bookVolumePage', 'row3_legalDescription', 'row3_references']
];

// Source field names (order matches RECORD_FIELD_KEYS)
const RECORD_SOURCE_FIELDS = ['grantor', 'grantee', 'docType', 'recordedDate',
                               'docNumber', 'bookVolumePage', 'legalDescription', 'references'];

// Pre-parse normalization for case_style input
function normalizeCaseStyle(input) {
  if (!input) return '';
  return input
    .normalize('NFKC')                    // Unicode normalize
    .trim()                               // Remove leading/trailing whitespace
    .replace(/\s+/g, ' ')                 // Collapse internal whitespace
    .replace(/^[^\w"']+|[^\w"']+$/g, ''); // Remove stray boundary punctuation
}

// County input normalization
function normalizeCounty(county) {
  if (!county) return { normalized: '', valid: false, reason: 'empty' };

  let normalized = county.trim().toUpperCase();

  // Detect invalid values
  if (normalized.length < 5) return { normalized, valid: false, reason: 'too_short' };
  if (normalized === 'TX' || normalized === 'TEXAS') return { normalized, valid: false, reason: 'state_only' };

  // Ensure " COUNTY" suffix
  if (!normalized.endsWith(' COUNTY')) {
    normalized = normalized + ' COUNTY';
  }

  return { normalized, valid: true, reason: null };
}

// Driver-specific name normalization hook (identity for now)
function normalizeForDriver(name, driverType) {
  return name; // No driver-specific transforms yet
}

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
    header: CSV_HEADER
  });
  await csvWriter.writeRecords(data);
}

// STEP 3: Parse name and generate search variations
function parseName(rawCaseStyle, driverType = 'default') {
  const caseStyle = normalizeCaseStyle(rawCaseStyle);
  if (!caseStyle) return { original: rawCaseStyle || '', variations: [], reason: 'empty_input' };

  // Split on VS to extract defendant
  const text = caseStyle; // Already normalized
  const parts = text.split(RE_VS_SPLIT);
  if (parts.length < 2) return { original: caseStyle, variations: [], reason: 'no_vs_separator' };

  let defendant = parts[1].trim();

  // Remove ET AL and everything after it
  defendant = defendant.replace(RE_ET_AL, '');

  // Remove noise patterns and normalize defendant text
  defendant = defendant
    .replace(RE_NOISE_LEGAL, '')
    .replace(RE_NOISE_ENTITIES, '')
    .replace(RE_ALIAS, '')
    .replace(RE_TRUSTEE, '')
    .replace(RE_PARENTHETICAL, ' ')        // Strip parentheticals: (IN REM), (MINOR), etc.
    .normalize('NFKC')                    // Unicode normalize defendant
    .replace(/\./g, '')
    .replace(/,/g, '')
    .replace(/\s+/g, ' ')
    .trim();

  if (!defendant) return { original: caseStyle, variations: [], reason: 'empty_defendant' };

  // Check for business entities (refined: exclude personal trusts)
  if (RE_BUSINESS_KEYWORDS.test(defendant)) {
    return { original: defendant, variations: [defendant] };
  }

  // Individual name parsing
  // Remove suffixes from word list for parsing
  const words = defendant.split(/\s+/).filter(w => w && !RE_SUFFIX.test(w));

  if (words.length === 0) return { original: defendant, variations: [], reason: 'only_suffixes' };
  if (words.length === 1) return { original: defendant, variations: [words[0]] };

  // Special case: Two single-letter initials + surname (e.g., "A C MARTIN")
  if (words.length === 3 &&
      words[0].length === 1 &&
      words[1].length === 1 &&
      words[2].length > 1 &&
      /^[A-Z]+$/i.test(words[2])) {
    const lastName = words[2];
    const firstMiddle = `${words[0]} ${words[1]}`;
    const fullName = `${lastName} ${firstMiddle}`;
    return {
      original: defendant,
      variations: [fullName, lastName]
    };
  }

  // Assume case style format: "LAST FIRST [MIDDLE]" (most common in TX court records)
  const lastName = words[0];
  const fullName = words.join(' ');

  const variations = [];

  switch (driverType) {
    case 'publicsearch':
    case 'kofile':
    case 'odyssey':
    default:
      // Default: full name first, then last name as fallback
      variations.push(fullName);
      variations.push(lastName);
      break;
    case 'tyler':
      // Tyler requires "LAST FIRST" format
      // Input is "FIRST LAST" (e.g., "JOHN SMITH"), convert to "SMITH JOHN"
      const tylerLast = words[words.length - 1];
      const tylerFirst = words.slice(0, -1).join(' ');
      const tylerFormat = tylerFirst ? `${tylerLast} ${tylerFirst}` : tylerLast;
      variations.push(tylerFormat);
      // Also try just last name as fallback
      if (words.length > 1) {
        variations.push(tylerLast);
      }
      break;
  }

  // Apply driver-specific normalization to variations
  const normalizedVariations = variations.map(v => normalizeForDriver(v, driverType));
  return { original: defendant, variations: normalizedVariations };
}

// STEP 4: Find appropriate driver for county (with caching)
function findDriver(county) {
  // Check cache first
  if (driverCache.has(county)) {
    return driverCache.get(county);
  }

  // Find driver and cache result
  for (const [name, driver] of DRIVER_ENTRIES) {
    if (driver.canHandle(county)) {
      const result = { name, driver };
      driverCache.set(county, result);
      return result;
    }
  }

  // Cache null result for unsupported counties
  driverCache.set(county, null);
  return null;
}

// STEP 5: Enrich row with lien data (optimized)
function enrichRow(row, records, metadata = {}) {
  const out = { ...row };

  // Add scrape metadata
  out.scrape_status = metadata.status || 'unknown';
  out.scrape_error = metadata.error || '';
  out.driver_used = metadata.driver || 'none';
  out.matched_variation = metadata.matchedVariation || '';
  out.parsed_name = metadata.parsedName || '';

  // Add classification fields
  out.classification_status = metadata.classificationStatus || '';
  out.html_length = metadata.htmlLength || 0;
  out.retry_count = metadata.retryCount || 0;
  out.classification_indicators = metadata.classificationIndicators || '';

  // Flatten records using pre-computed keys
  for (let i = 0; i < 3; i++) {
    const rec = records[i] || {};
    const keys = RECORD_FIELD_KEYS[i];
    for (let j = 0; j < RECORD_SOURCE_FIELDS.length; j++) {
      out[keys[j]] = rec[RECORD_SOURCE_FIELDS[j]] || '';
    }
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
    invalidCounty: 0,
    error: 0,
    skipped: 0,
    manual: 0
  };
  const unsupportedCounties = new Set();
  const driverStats = {};
  const failedRows = [];
  const manualRows = []; // Rows requiring manual CAPTCHA (Odyssey)
  const countyFailures = {}; // Track consecutive failures per county

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    stats.total++;

    try {
      const rawCounty = row.county;

      // Normalize and validate county input
      const countyInfo = normalizeCounty(rawCounty);
      if (!countyInfo.valid) {
        console.log(`\n[${i + 1}/${rows.length}] ⚠️  Invalid county: "${rawCounty}" (${countyInfo.reason})`);
        stats.invalidCounty++;
        const enriched = enrichRow(row, [], {
          status: 'invalid_county',
          error: `Invalid county: ${countyInfo.reason}`,
          driver: 'none'
        });
        results.push(enriched);
        failedRows.push(enriched);
        continue;
      }

      const county = countyInfo.normalized;

      // Find appropriate driver
      const driverInfo = findDriver(county);

      if (!driverInfo) {
        console.log(`\n[${i + 1}/${rows.length}] ⚠️  ${county} - No driver available`);
        stats.unsupported++;
        unsupportedCounties.add(county);
        const enriched = enrichRow(row, [], { status: 'unsupported_no_driver', error: 'No driver for county', driver: 'none' });
        results.push(enriched);
        failedRows.push(enriched);
        continue;
      }

      const { name: driverName, driver } = driverInfo;

      // Route Odyssey (Dallas) to manual queue - requires CAPTCHA
      if (driverName === 'odyssey') {
        console.log(`\n[${i + 1}/${rows.length}] 📋 ${county} - Routing to manual queue (CAPTCHA required)`);
        stats.manual++;
        const nameObj = parseName(row.case_style, driverName);
        const enriched = enrichRow(row, [], {
          status: 'blocked:captcha',
          error: 'Dallas County requires manual CAPTCHA - routed to manual queue',
          driver: driverName,
          parsedName: nameObj.original || row.case_style || '',
          classificationStatus: 'blocked:captcha',
          classificationIndicators: 'captcha_required'
        });
        results.push(enriched);
        manualRows.push(enriched);
        continue;
      }

      // Check if county should be auto-skipped
      if (countyFailures[county] >= 3) {
        stats.skipped++;
        const enriched = enrichRow(row, [], {
          status: 'skipped_county',
          error: 'auto-skip: first 3 attempts failed',
          driver: driverName
        });
        results.push(enriched);
        failedRows.push(enriched);
        continue;
      }

      // Track driver usage
      driverStats[driverName] = (driverStats[driverName] || 0) + 1;

      console.log(`\n[${i + 1}/${rows.length}] 🏛️  ${county} (using ${driverName} driver)`);

      const nameObj = parseName(row.case_style, driverName);

      if (!nameObj.variations.length) {
        const reason = nameObj.reason || 'unknown';
        console.log(`⚠️  Could not parse name from: "${row.case_style}" (${reason})`);
        stats.parseFailed++;
        const enriched = enrichRow(row, [], {
          status: 'parse_failed',
          error: `parse_failed: ${reason}`,
          driver: driverName,
          parsedName: nameObj.original || row.case_style || ''
        });
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
      let classification = null;
      let htmlLength = 0;
      let indicators = [];
      let retryCount = 0;

      for (let v = 0; v < nameObj.variations.length && records.length === 0; v++) {
        console.log(`\n🔍 Trying variation ${v + 1}/${nameObj.variations.length}: "${nameObj.variations[v]}"`);

        // Retry up to 3 times with exponential backoff
        for (let attempt = 1; attempt <= 3; attempt++) {
          retryCount++;
          try {
            const result = await driver.search(county, nameObj.variations[v]);

            // Handle new return format (object with classification) vs legacy (array)
            if (result && typeof result === 'object' && !Array.isArray(result)) {
              records = result.records || [];
              classification = result.classification || null;
              htmlLength = result.htmlLength || 0;
              indicators = result.indicators || [];
            } else {
              // Legacy format (array only) - other drivers
              records = result || [];
            }

            if (records.length > 0) {
              console.log(`✓ Found ${records.length} records`);
              matchedVariation = nameObj.variations[v];
              break;
            }

            // Stop retrying on terminal classifications (confirmed no results)
            if (classification === 'no_results:confirmed') {
              console.log(`○ No results (confirmed)`);
              break;
            }

            // If blocked, log it but allow retry
            if (classification && classification.startsWith('blocked:')) {
              console.log(`⚠️  Response classified as: ${classification}`);
            }

            // No records found, move to next variation (unless retrying blocked)
            if (!classification || !classification.startsWith('blocked:')) {
              break;
            }
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

      // Fallback: Try swapped name order for 2-word names with no results
      if (records.length === 0 && !searchError && classification !== 'no_results:confirmed') {
        const nameWords = nameObj.original.split(/\s+/).filter(w => w);
        if (nameWords.length === 2) {
          const swappedName = `${nameWords[1]} ${nameWords[0]}`;
          console.log(`\n🔄 Trying swapped name order: "${swappedName}"`);
          retryCount++;

          try {
            const result = await driver.search(county, swappedName);

            // Handle new return format
            if (result && typeof result === 'object' && !Array.isArray(result)) {
              records = result.records || [];
              classification = result.classification || classification;
              htmlLength = result.htmlLength || htmlLength;
              indicators = result.indicators || indicators;
            } else {
              records = result || [];
            }

            if (records.length > 0) {
              console.log(`✓ Found ${records.length} records with swapped name`);
              matchedVariation = swappedName + ' (swapped)';
            }
          } catch (e) {
            console.log(`⚠️  Swapped search failed: ${e.message}`);
          }
        }
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

      // Track consecutive failures per county
      if (status === 'success') {
        countyFailures[county] = 0;
      } else {
        countyFailures[county] = (countyFailures[county] || 0) + 1;
        if (countyFailures[county] === 3) {
          console.log(`⏭️  Skipping ${county} after 3 failed searches.`);
        }
      }

      const enriched = enrichRow(row, records, {
        status,
        error: searchError || '',
        driver: driverName,
        matchedVariation,
        parsedName: nameObj.original,
        classificationStatus: classification || '',
        htmlLength: htmlLength || 0,
        retryCount,
        classificationIndicators: indicators.join(',')
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

  // Write manual queue CSV (Odyssey rows requiring CAPTCHA)
  const manualFile = outputFile.replace(/\.csv$/, '-manual.csv');
  if (manualRows.length > 0) {
    await writeCSV(manualRows, manualFile);
  }

  console.log(`\n${'='.repeat(60)}`);
  console.log(`📊 RESULTS SUMMARY`);
  console.log(`${'='.repeat(60)}`);
  console.log(`\n📁 Output Files:`);
  console.log(`   Main:   ${outputFile} (${results.length} rows)`);
  if (failedRows.length > 0) {
    console.log(`   Failed: ${failedFile} (${failedRows.length} rows)`);
  }
  if (manualRows.length > 0) {
    console.log(`   Manual: ${manualFile} (${manualRows.length} rows - CAPTCHA required)`);
  }

  console.log(`\n📈 Status Breakdown:`);
  console.log(`   ✓ Success:      ${stats.success.toString().padStart(4)} (${((stats.success / stats.total) * 100).toFixed(1)}%)`);
  console.log(`   ○ No Records:   ${stats.noRecords.toString().padStart(4)} (${((stats.noRecords / stats.total) * 100).toFixed(1)}%)`);
  console.log(`   ✗ Failed:       ${stats.failed.toString().padStart(4)} (${((stats.failed / stats.total) * 100).toFixed(1)}%)`);
  console.log(`   ⚠ Parse Failed: ${stats.parseFailed.toString().padStart(4)} (${((stats.parseFailed / stats.total) * 100).toFixed(1)}%)`);
  console.log(`   ⚠ Invalid County: ${stats.invalidCounty.toString().padStart(3)} (${((stats.invalidCounty / stats.total) * 100).toFixed(1)}%)`);
  console.log(`   ⊘ Unsupported:  ${stats.unsupported.toString().padStart(4)} (${((stats.unsupported / stats.total) * 100).toFixed(1)}%)`);
  console.log(`   ✗ Error:        ${stats.error.toString().padStart(4)} (${((stats.error / stats.total) * 100).toFixed(1)}%)`);
  console.log(`   ⏭ Skipped:      ${stats.skipped.toString().padStart(4)} (${((stats.skipped / stats.total) * 100).toFixed(1)}%)`);
  console.log(`   📋 Manual:       ${stats.manual.toString().padStart(4)} (${((stats.manual / stats.total) * 100).toFixed(1)}%)`);

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
