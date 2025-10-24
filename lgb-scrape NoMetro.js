// lgb-scrape NoMetro.js
// Same scraper logic as lgb-scrape HJK.js but excludes metro counties.
import axios from 'axios';
import fs from 'fs';
import { parse } from 'json2csv';

// 🧠 MEMORY (global constants)
const API_BASE_URL = 'https://taxsales.lgbs.com/api';
// Endpoint discovered for selecting counties tied to a specific sale date
const COUNTY_ENDPOINT = `${API_BASE_URL}/sale_counties/?limit=60&sale_date_only=2019-03-05`;

const BASE_URL = API_BASE_URL;
const INPUT_FILE = 'texas-future-sales.csv';

// Metro counties to exclude (Texas MSAs)
const METRO_COUNTIES = new Set([
  // Austin-Round Rock-Georgetown MSA
  'TRAVIS COUNTY',
  // Dallas-Fort Worth-Arlington MSA
  'DALLAS COUNTY',
  // Houston-The Woodlands-Sugar Land MSA
  'HARRIS COUNTY',
  // San Antonio-New Braunfels MSA
  'BEXAR COUNTY',
]);

const normalizeCountyName = raw => {
  if (!raw) return '';
  const upper = raw.trim().toUpperCase().replace(/\s+/g, ' ');
  const withoutState = upper.replace(/\s*,\s*TX$/, '');
  return withoutState.endsWith(' COUNTY') ? withoutState : `${withoutState} COUNTY`;
};

// small helper delay
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

// Try to obtain a list of counties from the API, falling back to scanning property listings
async function getAllCounties() {
  // Try sale_counties endpoint first; data shape may vary so normalize defensively
  try {
    const { data } = await axios.get(COUNTY_ENDPOINT);
    const rows = Array.isArray(data?.results)
      ? data.results
      : Array.isArray(data)
        ? data
        : data?.sale_counties || [];
    const names = rows
      .map(row => {
        if (!row) return null;
        if (typeof row === 'string') return normalizeCountyName(row);
        const candidate = row.county || row.county_name || row.name || row.sale_county;
        return candidate ? normalizeCountyName(candidate) : null;
      })
      .filter(Boolean);
    const filtered = [...new Set(names)].filter(n => !METRO_COUNTIES.has(n));
    if (filtered.length) {
      console.log(`📍 Found ${filtered.length} non-metro counties via sale_counties endpoint`);
      return filtered.sort();
    }
  } catch (err) {
    console.warn('Could not fetch sale_counties endpoint, trying /counties/ next:', err.message);
  }

  // First try /counties/ endpoint
  try {
    const res = await axios.get(`${BASE_URL}/counties/`, { params: { state: 'TX', limit: 1000 } });
    const rows = res.data?.results || [];
    const names = rows.map(r => normalizeCountyName(r.name)).filter(Boolean);
    const filtered = [...new Set(names)].filter(n => !METRO_COUNTIES.has(n));
    console.log(`📍 Found ${filtered.length} non-metro counties via /counties/`);
    if (filtered.length) return filtered.sort();
  } catch (err) {
    console.warn('Could not fetch /counties/ endpoint, falling back to scanning property_sales:', err.message);
  }

  // Fallback: scan property_sales pages and collect county names
  try {
    const set = new Set();
    let offset = 0;
    const limit = 1000;
    while (true) {
      const { data } = await axios.get(`${BASE_URL}/property_sales/`, {
        params: { state: 'TX', limit, offset }
      });
      if (!data?.results?.length) break;
      data.results.forEach(p => {
        const name = normalizeCountyName(p.county);
        if (!METRO_COUNTIES.has(name)) set.add(name);
      });
      if (!data.next) break;
      offset += limit;
      await delay(500);
    }
    const arr = [...set].sort();
    console.log(`📍 Found ${arr.length} non-metro counties by scanning property_sales`);
    if (arr.length) return arr;
  } catch (err) {
    console.error('Fallback county scan failed:', err.message);
  }

  console.warn('⚠️ No dynamic county sources responded; returning empty list');
  return [];
}

// get latest sale date similar to HJK script
async function getLatestSaleDate() {
  try {
    const { data } = await axios.get(`${BASE_URL}/property_sales/`, {
      params: { limit: 100, ordering: '-sale_date', sale_type: 'SALE', status: 'Scheduled for Auction' }
    });
    const scheduled = data.results?.find(p => p.sale_date);
    if (scheduled?.sale_date) return scheduled.sale_date;
    return 'unknown';
  } catch (err) {
    console.warn('getLatestSaleDate error:', err.message);
    return 'unknown';
  }
}

async function getProperties(county) {
  const all = [];
  let offset = 0;
  const limit = 600;
  try {
    while (true) {
      console.log(`Fetching ${county} properties, offset: ${offset}`);
      const { data } = await axios.get(`${BASE_URL}/property_sales/`, {
        params: { county, state: 'TX', limit, offset }
      });
      if (!data?.results?.length) break;
      all.push(...data.results);
      if (!data.next) break;
      offset += limit;
      await delay(300);
    }
  } catch (err) {
    console.error(`Error fetching properties for ${county}:`, err.message || err);
    throw err;
  }
  return all;
}

// Fetch UID -> full address map for a county using the list API
async function fetchUidAddressMapForCounty(county) {
  const map = new Map();
  let offset = 0;
  const limit = 1000;
  try {
    while (true) {
      const { data } = await axios.get(`${BASE_URL}/property_sales/`, {
        params: { county, state: 'TX', limit, offset }
      });
      if (!data?.results?.length) break;
      data.results.forEach(item => {
        const full = [item.prop_address_one, item.prop_city, item.prop_state, item.prop_zipcode].filter(Boolean).join(', ');
        if (item.uid) map.set(item.uid.toString(), full);
      });
      if (!data.next) break;
      offset += limit;
      await delay(250);
    }
  } catch (err) {
    console.warn(`Could not build uid address map for ${county}:`, err.message || err);
  }
  return map;
}

async function getPropertyDetails(uid) {
  try {
    const { data } = await axios.get(`${BASE_URL}/property_sales/${uid}/`);
    await delay(200);
    return { ...data, is_vacant: isVacantLot(data.legal_description, data.prop_address_one) };
  } catch (err) {
    console.warn(`Details fetch failed for ${uid}:`, err.message);
    return {};
  }
}

function isVacantLot(legalDesc = '', address = '') {
  const vacantTerms = ['LOT', 'VACANT', 'LANDLOCKED', 'UNDEVELOPED', 'UNIMPROVED', 'TRACT'];
  const hasNoAddress = !address || address.trim() === '';
  const hasLegalDesc = !!(legalDesc && legalDesc.trim());
  const containsVacantTerm = vacantTerms.some(t => (legalDesc || '').toUpperCase().includes(t));
  return (hasNoAddress && hasLegalDesc) || containsVacantTerm;
}

async function main() {
  console.log('🔍 Starting lgb-scrape NoMetro — excluding metro counties');
  const saleDate = await getLatestSaleDate();
  console.log('📅 Latest sale date:', saleDate);
  const TEST_LIMIT = parseInt(process.env.TEST_LIMIT || '0', 10) || 0;
  if (TEST_LIMIT) console.log(`⚗️ Running in test mode: stopping after ${TEST_LIMIT} results`);

  const counties = await getAllCounties();
  console.log(`🧾 Processing ${counties.length} non-metro counties`);
  if (!counties.length) {
    console.warn('No counties available after metro exclusion.');
    return;
  }

  const results = [];
  const totalAddressSourceCounts = {
    api_list: 0,
    detail: 0,
    listing: 0,
    approximated: 0
  };
  for (const county of counties) {
    try {
      // Build a uid -> address map for this county (list API)
      console.log(`Building uid->address map for ${county}`);
      const uidMap = await fetchUidAddressMapForCounty(county);
      const props = await getProperties(county);
      console.log(`   → ${props.length} listings found for ${county}`);
      // Track address source
      const apiSourced = [];
      const detailSourced = [];
      const listSourced = [];
      const approximated = [];
      for (const p of props) {
        if (p.status && p.status.toLowerCase().includes('cancel')) {
          console.log(`⏭️ Skipping cancelled: ${county} (${p.uid})`);
          continue;
        }
        // Enforce min bid and value thresholds
        const minBid = parseFloat((p.minimum_bid || '').toString().replace(/[^0-9.]/g, '')) || 0;
        if (minBid >= 5000) {
          console.log(`⏭️ Skipping min bid >= $5k ($${minBid.toLocaleString()}): ${county} (${p.uid})`);
          continue;
        }
        const adjudgedValueNum = parseFloat((p.value || '').toString().replace(/[^0-9.]/g, '')) || 0;
        if (adjudgedValueNum > 100000) {
          console.log(`⏭️ Skipping value over $100k ($${adjudgedValueNum.toLocaleString()}): ${county} (${p.uid})`);
          continue;
        }
        const details = await getPropertyDetails(p.uid);
        // Determine address precedence: uidMap -> details -> listing -> fallback (approximated)
        let chosenAddress = '';
        let addressSource = '';
        const uidStr = (p.uid || '').toString();
        if (uidMap && uidMap.has(uidStr) && uidMap.get(uidStr)) {
          chosenAddress = uidMap.get(uidStr);
          addressSource = 'api_list';
          apiSourced.push(uidStr);
        } else if (details && (details.prop_address_one || details.prop_city || details.prop_zipcode)) {
          chosenAddress = [details.prop_address_one, details.prop_city, details.prop_state, details.prop_zipcode].filter(Boolean).join(', ');
          addressSource = 'detail';
          detailSourced.push(uidStr);
        } else if (p.prop_address_one) {
          chosenAddress = [p.prop_address_one, p.prop_city, p.prop_state, p.prop_zipcode].filter(Boolean).join(', ');
          addressSource = 'listing';
          listSourced.push(uidStr);
        } else {
          chosenAddress = `${county} (address missing)`;
          addressSource = 'approximated';
          approximated.push(uidStr);
        }

        const actualSaleDate = p.sale_date || details.sale_date || saleDate;
        const formattedSaleDate = actualSaleDate && actualSaleDate !== 'unknown'
          ? new Date(actualSaleDate).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })
          : '';
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
          owner_name: details.owner_name || '',
          legal_description: details.legal_description || '',
          coordinates: details.coordinates ? JSON.stringify(details.coordinates) : '',
          is_vacant: details.is_vacant ? 'YES' : 'NO'
        });
        await delay(150);
        // If test limit is enabled, break early
        if (TEST_LIMIT && results.length >= TEST_LIMIT) break;
      }
      // Log per-county address source summary
      console.log(`  Address sources for ${county}: api_list=${apiSourced.length}, detail=${detailSourced.length}, listing=${listSourced.length}, approximated=${approximated.length}`);
      if (approximated.length) console.log(`   → Sample approximated UIDs (first 5): ${approximated.slice(0,5).join(', ')}`);
      totalAddressSourceCounts.api_list += apiSourced.length;
      totalAddressSourceCounts.detail += detailSourced.length;
      totalAddressSourceCounts.listing += listSourced.length;
      totalAddressSourceCounts.approximated += approximated.length;
    } catch (err) {
      console.error(`Skipping county ${county} due to error:`, err.message || err);
    }
    if (TEST_LIMIT && results.length >= TEST_LIMIT) break;
  }

  if (!results.length) {
    console.log('⚠️ No records collected.');
    return;
  }

  // Sort by numeric min_bid ascending (non-numeric treated as 0)
  results.sort((a, b) => {
    const aNum = parseFloat((a.min_bid || '').toString().replace(/[^0-9.]/g, '')) || 0;
    const bNum = parseFloat((b.min_bid || '').toString().replace(/[^0-9.]/g, '')) || 0;
    return aNum - bNum;
  });

  console.log(`🧮 Address source totals: api_list=${totalAddressSourceCounts.api_list}, detail=${totalAddressSourceCounts.detail}, listing=${totalAddressSourceCounts.listing}, approximated=${totalAddressSourceCounts.approximated}`);

  const fields = ['uid','address','address_source','county','sale_date','adjudged_value','min_bid','status','sale_type','cause_number','owner_name','legal_description','coordinates','is_vacant'];
  const csv = parse(results, { fields });
  fs.writeFileSync(INPUT_FILE, csv);
  console.log(`✅ Saved ${results.length} records to ${INPUT_FILE} (non-metro, sorted by min_bid)`);
}

main().catch(err => console.error('Fatal error:', err));
