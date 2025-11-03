// lgb-scrape NoMetro.js                     // Meta comment – no logic layer
// Same scraper logic as lgb-scrape HJK.js but excludes metro counties. // Meta

import axios from 'axios';                  // Connect > Import
import fs from 'fs';                        // Connect > Import
import { parse } from 'json2csv';           // Connect > Import

// � MEMORY (global constants)
const API_BASE_URL = 'https://taxsales.lgbs.com/api';                    // Memory > Values
const COUNTY_ENDPOINT = `${API_BASE_URL}/sale_counties/?limit=60&sale_date_only=2019-03-05`; // Memory > Values
const BASE_URL = API_BASE_URL;                                          // Memory > Values
const INPUT_FILE = 'texas-future-sales.csv';                            // Memory > Values
// → Immutable configuration data shared by all later functions.

const METRO_COUNTIES = new Set([                                        // Memory > Values
  'TRAVIS COUNTY',
  'DALLAS COUNTY',
  'HARRIS COUNTY',
  'BEXAR COUNTY',
]);
// → Constant dataset of metro counties to exclude during filtering.

const normalizeCountyName = raw => {                                    // Memory > Helpers
  if (!raw) return '';                                                  // Run > Plan > Check (guard)
  const upper = raw.trim().toUpperCase().replace(/\s+/g, ' ');          // Run > Execute > Work
  const withoutState = upper.replace(/\s*,\s*TX$/, '');                 // Run > Execute > Work
  return withoutState.endsWith(' COUNTY')                               // Run > Plan > Choose
    ? withoutState
    : `${withoutState} COUNTY`;                                         // Run > Execute > Work
};                                                                      // → Normalizes input county strings.

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));    // Memory > Helpers
// → Asynchronous helper returning a promise that resolves after ms delay; used for pacing API calls.
// 🛰️ Fetch a list of counties using multiple fallback methods
async function getAllCounties() {                  // Listen > Start
  try {                                            // Run > Plan > Check (error guard)
    const { data } = await axios.get(COUNTY_ENDPOINT);  // Run > Execute > Flow (API call)
    const rows = Array.isArray(data?.results)            // Run > Plan > Choose
      ? data.results
      : Array.isArray(data)
        ? data
        : data?.sale_counties || [];
    const names = rows
      .map(row => {                               // Run > Execute > Flow (loop transform)
        if (!row) return null;                    // Run > Plan > Check
        if (typeof row === 'string') return normalizeCountyName(row);  // Run > Execute > Work
        const candidate = row.county || row.county_name || row.name || row.sale_county; // Run > Execute > Work
        return candidate ? normalizeCountyName(candidate) : null;      // Run > Execute > Work
      })
      .filter(Boolean);                           // Run > Execute > Work
    const filtered = [...new Set(names)].filter(n => !METRO_COUNTIES.has(n)); // Run > Execute > Work
    if (filtered.length) {                        // Run > Plan > Check
      console.log(`📍 Found ${filtered.length} non-metro counties via sale_counties endpoint`); // Render > Internal
      return filtered.sort();                     // Run > Execute > Work
    }
  } catch (err) {                                 // Run > Plan > Check (error handler)
    console.warn('Could not fetch sale_counties endpoint, trying /counties/ next:', err.message); // Render > Internal
  }

  // ⬇️ Second attempt: use /counties/ endpoint
  try {                                           // Run > Plan > Check
    const res = await axios.get(`${BASE_URL}/counties/`, { params: { state: 'TX', limit: 1000 } }); // Run > Execute > Flow
    const rows = res.data?.results || [];         // Memory > Variables (local)
    const names = rows.map(r => normalizeCountyName(r.name)).filter(Boolean); // Run > Execute > Work
    const filtered = [...new Set(names)].filter(n => !METRO_COUNTIES.has(n)); // Run > Execute > Work
    console.log(`📍 Found ${filtered.length} non-metro counties via /counties/`); // Render > Internal
    if (filtered.length) return filtered.sort();  // Run > Plan > Choose
  } catch (err) {                                 // Run > Plan > Check
    console.warn('Could not fetch /counties/ endpoint, falling back to scanning property_sales:', err.message); // Render > Internal
  }

  // ⬇️ Final fallback: scan property_sales pages
  try {                                           // Run > Plan > Check
    const set = new Set();                        // Memory > Variables
    let offset = 0;                               // Memory > Variables
    const limit = 1000;                           // Memory > Values
    while (true) {                                // Run > Execute > Flow (loop)
      const { data } = await axios.get(`${BASE_URL}/property_sales/`, {
        params: { state: 'TX', limit, offset }
      });                                         // Run > Execute > Flow
      if (!data?.results?.length) break;          // Run > Plan > Check
      data.results.forEach(p => {                 // Run > Execute > Flow
        const name = normalizeCountyName(p.county); // Run > Execute > Work
        if (!METRO_COUNTIES.has(name)) set.add(name); // Run > Plan > Choose + Execute
      });
      if (!data.next) break;                      // Run > Plan > Check
      offset += limit;                            // Run > Execute > Work
      await delay(500);                           // Listen > Wait (pause async loop)
    }
    const arr = [...set].sort();                  // Run > Execute > Work
    console.log(`📍 Found ${arr.length} non-metro counties by scanning property_sales`); // Render > Internal
    if (arr.length) return arr;                   // Run > Plan > Choose
  } catch (err) {                                 // Run > Plan > Check
    console.error('Fallback county scan failed:', err.message); // Render > Internal
  }

  console.warn('⚠️ No dynamic county sources responded; returning empty list'); // Render > Internal
  return [];                                     // Run > Execute > Work
}
// → Multi-layered data retrieval sequence: try primary endpoint, fall back to secondary, then scan listings.


// 🗓️ Fetch the latest sale date
async function getLatestSaleDate() {             // Listen > Start
  try {                                          // Run > Plan > Check
    const { data } = await axios.get(`${BASE_URL}/property_sales/`, {  // Run > Execute > Flow
      params: { limit: 100, ordering: '-sale_date', sale_type: 'SALE', status: 'Scheduled for Auction' }
    });
    const scheduled = data.results?.find(p => p.sale_date);  // Run > Execute > Work
    if (scheduled?.sale_date) return scheduled.sale_date;     // Run > Plan > Choose
    return 'unknown';                            // Run > Execute > Work
  } catch (err) {                                // Run > Plan > Check
    console.warn('getLatestSaleDate error:', err.message);   // Render > Internal
    return 'unknown';                            // Run > Execute > Work
  }
}
// 🏘️ Fetch all property listings for a specific county
async function getProperties(county) {          // Listen > Start
  const all = [];                               // Memory > Variables
  let offset = 0;                               // Memory > Variables
  const limit = 600;                            // Memory > Values

  try {                                         // Run > Plan > Check
    while (true) {                              // Run > Execute > Flow (loop control)
      console.log(`Fetching ${county} properties, offset: ${offset}`); // Render > Internal
      const { data } = await axios.get(`${BASE_URL}/property_sales/`, {
        params: { county, state: 'TX', limit, offset }
      });                                       // Run > Execute > Flow (API call)
      if (!data?.results?.length) break;        // Run > Plan > Check
      all.push(...data.results);                // Run > Execute > Work
      if (!data.next) break;                    // Run > Plan > Check
      offset += limit;                          // Run > Execute > Work
      await delay(300);                         // Listen > Wait (pause async loop)
    }
  } catch (err) {                               // Run > Plan > Check (error recovery)
    console.error(`Error fetching properties for ${county}:`, err.message || err); // Render > Internal
    throw err;                                  // Run > Execute > Flow (propagate error)
  }
  return all;                                   // Run > Execute > Work (output array)
}
// → Collects all property pages for one county, paginating until no more data.


// 🗺️ Build a UID → Full Address map from list API
async function fetchUidAddressMapForCounty(county) { // Listen > Start
  const map = new Map();                       // Memory > Variables
  let offset = 0;                              // Memory > Variables
  const limit = 1000;                          // Memory > Values
  try {                                        // Run > Plan > Check
    while (true) {                             // Run > Execute > Flow
      const { data } = await axios.get(`${BASE_URL}/property_sales/`, {
        params: { county, state: 'TX', limit, offset }
      });                                      // Run > Execute > Flow
      if (!data?.results?.length) break;       // Run > Plan > Check
      data.results.forEach(item => {           // Run > Execute > Flow (loop over results)
        const full = [                         // Run > Execute > Work
          item.prop_address_one, 
          item.prop_city, 
          item.prop_state, 
          item.prop_zipcode
        ].filter(Boolean).join(', ');
        if (item.uid) map.set(item.uid.toString(), full); // Run > Execute > Work
      });
      if (!data.next) break;                   // Run > Plan > Check
      offset += limit;                         // Run > Execute > Work
      await delay(250);                        // Listen > Wait (throttle API pacing)
    }
  } catch (err) {                              // Run > Plan > Check
    console.warn(`Could not build uid address map for ${county}:`, err.message || err); // Render > Internal
  }
  return map;                                  // Run > Execute > Work (return mapping)
}
// → Creates a quick-lookup Map so later functions can get addresses instantly by UID.


// Add this function right before getPropertyDetails():
function isVacantLot(legal_description = '', address = '') {
  const vacancyKeywords = ["VACANT", "LOT", "LOTS", "ACRE", "ACRES", "LAND", "LANDS", "TRACT", "TRACTS", "PARCEL", "PARCELS", "UNDEVELOPED", "UNIMPROVED", "RURAL"];
  const text = `${legal_description} ${address}`.toUpperCase();
  return vacancyKeywords.some(term => text.includes(term));
}

async function getPropertyDetails(uid) {
  try {
    const { data } = await axios.get(`${BASE_URL}/property_sales/${uid}/`);
    await delay(200);
    console.log('DEBUG property DATA', data);
    return {
      ...data,
      legal_description: data.legal_desc_l || data.legal_desc_s || '', // Use long description, fall back to short
      coordinates: JSON.stringify(data.geometry?.coordinates || []),
      is_vacant: isVacantLot(data.legal_desc_l || data.legal_desc_s || '', data.prop_address_one || '')
    };
  } catch (err) {
    console.warn(`Details fetch failed for ${uid}:`, err.message);
    return {};
  }
}

// → Fetches one property’s details, enriches it with vacancy detection before returning.
// 🚀 Program entry point
async function main() {                                  // Connect > Entry  (program start)
  console.log('🔍 Starting lgb-scrape-NoMetro — excluding metro counties'); // Render > Internal

  const saleDate = await getLatestSaleDate();            // Listen > Wait  (fires async function)
  console.log('📅 Latest sale date:', saleDate);          // Render > Internal

  const TEST_LIMIT = parseInt(process.env.TEST_LIMIT || '0', 10) || 0; //Memory > Variables
  if (TEST_LIMIT) console.log(`⚗️ Running in test mode: stopping after ${TEST_LIMIT} results`); // Render > Internal

  const counties = await getAllCounties();               // Listen > Wait  (triggers all county-fetching logic)
  console.log(`🧾 Processing ${counties.length} non-metro counties`); // Render > Internal

  if (!counties.length) {                                // Run > Plan > Check
    console.warn('No counties available after metro exclusion.'); // Render > Internal
    return;                                              // Run > Execute > Flow (exit early)
  }

  const results = [];                                    // Memory > Variables (collector)
  const totalAddressSourceCounts = {                     // Memory > Variables (tracker object)
    api_list: 0, detail: 0, listing: 0, approximated: 0
  };

  // Loop through each county
  for (const county of counties) {                       // Run > Execute > Flow
    try {                                                // Run > Plan > Check
      console.log(`Building uid->address map for ${county}`);       // Render > Internal
      const uidMap = await fetchUidAddressMapForCounty(county);     // Listen > Wait
      const props = await getProperties(county);                    // Listen > Wait
      console.log(`   → ${props.length} listings found for ${county}`); // Render > Internal

      // Local counters
      const apiSourced = [];                              // Memory > Variables
      const detailSourced = [];
      const listSourced = [];
      const approximated = [];

      // Loop over each property
      for (const p of props) {                            // Run > Execute > Flow
        if (p.status && p.status.toLowerCase().includes('cancel')) {  // Run > Plan > Check
          console.log(`⏭️ Skipping cancelled: ${county} (${p.uid})`); // Render > Internal
          continue;
        }

        const minBid = parseFloat((p.minimum_bid || '').toString().replace(/[^0-9.]/g, '')) || 0; // Run > Execute > Work
        if (minBid > 5000) {                              // Run > Plan > Check
          console.log(`⏭️ Skipping high price ($${minBid.toLocaleString()}): ${county} (${p.uid})`); // Render > Internal
          continue;
        }

        const details = await getPropertyDetails(p.uid);   // Listen > Wait
        console.log('DEBUG details', details);             // Render > Internal

        // Determine address precedence
        const uidStr = (p.uid || '').toString();           // Run > Execute > Work
        let chosenAddress = '';                            // Memory > Variables
        let addressSource = '';                            // Memory > Variables

        if (uidMap && uidMap.has(uidStr) && uidMap.get(uidStr)) { // Run > Plan > Choose
          chosenAddress = uidMap.get(uidStr); addressSource = 'api_list'; apiSourced.push(uidStr);
        } else if (details && (details.prop_address_one || details.prop_city || details.prop_zipcode)) {
          chosenAddress = [details.prop_address_one, details.prop_city, details.prop_state, details.prop_zipcode].filter(Boolean).join(', ');
          addressSource = 'detail'; detailSourced.push(uidStr);
        } else if (p.prop_address_one) {
          chosenAddress = [p.prop_address_one, p.prop_city, p.prop_state, p.prop_zipcode].filter(Boolean).join(', ');
          addressSource = 'listing'; listSourced.push(uidStr);
        } else {
          chosenAddress = `${county} (address missing)`; addressSource = 'approximated'; approximated.push(uidStr);
        }

        // Sale date formatting
        const actualSaleDate = p.sale_date || details.sale_date || saleDate;  // Run > Execute > Work
        const formattedSaleDate = actualSaleDate && actualSaleDate !== 'unknown'
          ? new Date(actualSaleDate).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })
          : '';                                           // Run > Execute > Work

        // Vacancy detection
        const vacancyKeywords = ["VACANT", "LOT", "LOTS", "ACRE", "ACRES", "LAND", "LANDS", "TRACT", "TRACTS", "PARCEL", "PARCELS", "UNDEVELOPED", "UNIMPROVED", "RURAL"]; // Memory > Values
        function detectVacancy(legalDesc = "", saleNotes = "") {              // Memory > Helpers
          const upperLegal = legalDesc.toUpperCase();
          const upperNotes = saleNotes.toUpperCase();
          for (const term of vacancyKeywords) {
            if (upperLegal.includes(term)) return { match: term, source: "legalDesc" };
            if (upperNotes.includes(term)) return { match: term, source: "saleNotes" };
          }
          return null;
        }

        const vacancyMatch = detectVacancy(details.legal_description || "", p.sale_notes || ""); // Run > Execute > Work
        if (!vacancyMatch) {                               // Run > Plan > Check
          console.log(`⏭️ Skipping non-vacant property: ${county} (${p.uid})`); // Render > Internal
          continue;
        }

        // Push structured record
        results.push({
          uid: p.uid,
          county,
          sale_date: formattedSaleDate,
          address: chosenAddress,
          address_source: addressSource,
          adjudged_value: p.value || '',
          min_bid: p.minimum_bid || '',
          status: p.status || '',
          sale_type: p.sale_type || '',
          cause_number: p.cause_nbr || '',
          case_style: details.case_style || '',
          legal_description: details.legal_description || '',
          coordinates: JSON.stringify(details.geometry?.coordinates || ''),
          sale_notes: p.sale_notes || '',
          vacant_keyword: vacancyMatch.match,
          vacant_source: vacancyMatch.source
        });                                               // Run > Execute > Work

        await delay(150);                                 // Listen > Wait (brief pause between items)
        if (TEST_LIMIT && results.length >= TEST_LIMIT) break; // Run > Plan > Check
      }

      // Per-county summary
      console.log(`  Address sources for ${county}: api_list=${apiSourced.length}, detail=${detailSourced.length}, listing=${listSourced.length}, approximated=${approximated.length}`); // Render > Internal
      totalAddressSourceCounts.api_list += apiSourced.length;
      totalAddressSourceCounts.detail += detailSourced.length;
      totalAddressSourceCounts.listing += listSourced.length;
      totalAddressSourceCounts.approximated += approximated.length;

    } catch (err) {                                      // Run > Plan > Check
      console.error(`Skipping county ${county} due to error:`, err.message || err); // Render > Internal
    }

    if (TEST_LIMIT && results.length >= TEST_LIMIT) break; // Run > Plan > Check
  }

  if (!results.length) {                                 // Run > Plan > Check
    console.log('⚠️ No records collected.');             // Render > Internal
    return;                                              // Run > Execute > Flow (exit)
  }

  // Sort records by min_bid
  results.sort((a, b) => {                               // Run > Execute > Flow
    const aNum = parseFloat((a.min_bid || '').toString().replace(/[^0-9.]/g, '')) || 0;
    const bNum = parseFloat((b.min_bid || '').toString().replace(/[^0-9.]/g, '')) || 0;
    return aNum - bNum;
  });

  console.log(`🧮 Address source totals: api_list=${totalAddressSourceCounts.api_list}, detail=${totalAddressSourceCounts.detail}, listing=${totalAddressSourceCounts.listing}, approximated=${totalAddressSourceCounts.approximated}`); // Render > Internal

  const fields = ['uid','address','address_source','county','sale_date','adjudged_value','min_bid','status','sale_type','cause_number','case_style','legal_description','coordinates','sale_notes','vacant_keyword','vacant_source']; // Memory > Values
  const csv = parse(results, { fields });                // Run > Execute > Work (convert JSON → CSV)
  fs.writeFileSync(INPUT_FILE, csv);                     // Render > External (write file)
  console.log(`✅ Saved ${results.length} records to ${INPUT_FILE} (non-metro, sorted by min_bid)`); // Render > Internal
}

main().catch(err => console.error('Fatal error:', err));  // Listen > React + Run > Plan > Check