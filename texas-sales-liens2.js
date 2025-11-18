
// Enriches Texas property sales CSV with top 3 liens from county websites
// Usage: node enrich-liens.js
// Input from libary 
import fs from 'fs';
import { parse } from 'csv-parse/sync';
import axios from 'axios';
import * as cheerio from 'cheerio';
import { stringify } from 'csv-stringify/sync';

// County name to URL slug mapping (partial list for brevity)
const COUNTY_SLUGS = {
  'ANDERSON': 'anderson',
  'ANGELINA': 'angelina',
  'BEE': 'bee',
  'CAMERON': 'cameron',
  'ECTOR': 'ector',
  'EL PASO': 'elpaso',
  'ERATH': 'erath',
  'FORT BEND': 'fortbend',
  'FRIO': 'frio',
  'GALVESTON': 'galveston',
  'GRAYSON': 'grayson',
  'HARRIS': 'harris',
  'HIDALGO': 'hidalgo',
  'HUNT': 'hunt',
  'JEFFERSON': 'jefferson',
  'KERR': 'kerr',
  'LAMPASAS': 'lampasas',
  'MAVERICK': 'maverick',
  'MCLENNAN': 'mclennan',
  'MIDLAND': 'midland',
  'MONTGOMERY': 'montgomery',
  'NUECES': 'nueces',
  'POTTER': 'potter',
  'SAN PATRICIO': 'sanpatricio',     // note merged slug
  'SMITH': 'smith',
  'TARRANT': 'tarrant',
  'TRAVIS': 'travis',
  'UPSHUR': 'upshur',
  'UVALDE': 'uvalde',
  'VICTORIA': 'victoria',
  'WALLER': 'waller',
  'WASHINGTON': 'washington',
  'WEBB': 'webb',
  'WHARTON': 'wharton',
  'WICHITA': 'wichita',
  'WILLIAMSON': 'williamson',
  'WISE': 'wise',
  'WOOD': 'wood',
  'YOUNG': 'young',
  'ZAVALA': 'zavala',
  'ZAPATA': 'zapata'
};


// Step 1: Parse defendant name from case_style
// Expects: "DISTRICT VS NAME" format → Returns: clean name
const parseName = (caseStyle) => {
  const parts = caseStyle.split(' VS ');
  if (parts.length < 2) return null;
  
  let name = parts[1]
    .replace(/, ET AL$/i, '')
    .replace(/, DECEASED$/i, '')
    .replace(/\./g, '')
    .trim();
  
  console.log(`Parsed: "${caseStyle}" → "${name}"`);
  return name;
};

// Step 2: Build search URL for county + name combination
// Expects: county name + person name → Returns: formatted search URL
const buildSearchURL = (county, name, variation = 'full') => {
  const countySlug = COUNTY_SLUGS[county.replace(' COUNTY', '')] || county.toLowerCase();
  
  // Create name variations: full → first+last → last only
  let searchName = name;
  const parts = name.split(' ');
  
  if (variation === 'firstlast' && parts.length > 1) {
    searchName = `${parts[parts.length-1]} ${parts[0]}`;
  } else if (variation === 'last') {
    searchName = parts[parts.length-1];
  }
  
  const encoded = encodeURIComponent(`"${searchName.toLowerCase()}"`);
  const url = `https://${countySlug}.tx.publicsearch.us/results?department=RP&searchValue=${encoded}&limit=50&sort=desc&sortBy=recordedDate`;
  
  // breakpoint: verify URL format before fetch
  return url;
};

// Step 3: Fetch and parse liens from county website
// Expects: search URL → Returns: array of {date, type} objects
const fetchLiens = async (url) => {
  try {
    console.log(`Fetching: ${url.substring(0, 80)}...`);
    const response = await axios.get(url, { timeout: 10000 });
    
    const $ = cheerio.load(response.data);
    const liens = [];
    
    // Parse table rows (adjust selector based on actual HTML structure)
    $('tr').each((i, row) => {
      const cells = $(row).find('td');
      if (cells.length >= 3) {
        const docType = $(cells[2]).text().trim();
        const recordedDate = $(cells[3]).text().trim();
        
        if (docType && recordedDate) {
          liens.push({ date: recordedDate, type: docType });
        }
      }
    });
    
    console.log(`  Found ${liens.length} liens`);
    return liens;
  } catch (error) {
    console.log(`  Error: ${error.message}`);
    return [];
  }
};

// Step 4: Select top 3 most recent liens
// Expects: array of liens → Returns: exactly 3 slots (filled or "Not Found")
const selectTop3 = (liens) => {
  const sorted = liens.sort((a, b) => new Date(b.date) - new Date(a.date));
  const result = [];
  
  for (let i = 0; i < 3; i++) {
    if (sorted[i]) {
      result.push(sorted[i]);
    } else {
      result.push({ date: 'Not Found', type: 'Not Found' });
    }
  }
  
  return result;
};

// Step 5: Main orchestrator
// Reads CSV → enriches each row → writes output
const main = async () => {
  // Load input CSV
  const input = fs.readFileSync('texas-future-sales.csv', 'utf-8');
  const rows = parse(input, { columns: true });
  console.log(`Loaded ${rows.length} rows from CSV`);
  
  // Process each row
  const enriched = [];
  for (const [index, row] of rows.entries()) {
    console.log(`\nRow ${index + 1}/${rows.length}: ${row.case_style?.substring(0, 50)}...`);
    
    const name = parseName(row.case_style || '');
    if (!name) {
      row.lien1_date = row.lien1_type = 'Parse Error';
      row.lien2_date = row.lien2_type = 'Parse Error';
      row.lien3_date = row.lien3_type = 'Parse Error';
      enriched.push(row);
      continue;
    }
    
    // Try name variations until we get results
    let liens = [];
    for (const variation of ['full', 'firstlast', 'last']) {
      const url = buildSearchURL(row.county, name, variation);
      liens = await fetchLiens(url);
      if (liens.length > 0) break;
      
      await new Promise(r => setTimeout(r, 50)); // Rate limit
    }
    
    // Add top 3 liens to row
    const top3 = selectTop3(liens);
    row.lien1_date = top3[0].date;
    row.lien1_type = top3[0].type;
    row.lien2_date = top3[1].date;
    row.lien2_type = top3[1].type;
    row.lien3_date = top3[2].date;
    row.lien3_type = top3[2].type;
    
    enriched.push(row);
    console.log(`  Added liens: ${top3[0].date} (${top3[0].type?.substring(0, 20)}...)`);
  }
  
  // Write output CSV
  const output = stringify(enriched, { header: true });
  fs.writeFileSync('texas-sales-liens.csv', output);
  console.log(`\n✓ Wrote ${enriched.length} rows to texas-sales-liens.csv`);
};

// Run with: node enrich-liens.js
// First breakpoint: line 29 (name parsing verification)
main().catch(console.error);