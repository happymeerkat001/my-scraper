import axios from 'axios';
import { createReadStream, readFileSync, writeFileSync, mkdirSync } from 'fs';
import { createObjectCsvWriter } from 'csv-writer';
import csv from 'csv-parser';
import { setTimeout } from 'timers/promises';
import * as cheerio from 'cheerio';
import https from 'https';
import tls from 'tls';

// Load DigiCert intermediate (optional) and wire it into a single HTTPS agent
let httpsAgent;
try {
    const caPath = new URL('./DigiCertGlobalG2TLSRSASHA2562020CA1.crt.pem', import.meta.url);
    const caBuffer = readFileSync(caPath);
    // Respect NODE_TLS_REJECT_UNAUTHORIZED env for debugging. If set to '0' we will allow unauthenticated TLS.
    const rejectUnauthorized = process.env.NODE_TLS_REJECT_UNAUTHORIZED !== '0';
    // Merge Node's default root certificates with the provided DigiCert bundle to avoid replacing trusted roots.
    const defaultRoots = Array.isArray(tls.rootCertificates) ? tls.rootCertificates.slice() : [];
    // tls.rootCertificates are strings (PEM), add the custom CA as string
    defaultRoots.push(caBuffer.toString());
    httpsAgent = new https.Agent({ rejectUnauthorized, ca: defaultRoots });
} catch (e) {
    const rejectUnauthorized = process.env.NODE_TLS_REJECT_UNAUTHORIZED !== '0';
    httpsAgent = new https.Agent({ rejectUnauthorized });
}

// Create axios client (we'll manage cookies manually to allow using a custom https.Agent)
let client = axios.create({ httpsAgent, withCredentials: true });

// Simple in-memory cookie store (name->value)
const cookieStore = Object.create(null);

// Optionally seed cookies via environment variable COOKIES (raw cookie header)
const cookieEnv = process.env.COOKIES || '';

async function seedCookies() {
  if (!cookieEnv) return;
  const parts = cookieEnv.split(/;\s*/);
  for (const p of parts) {
    const idx = p.indexOf('=');
    if (idx === -1) {
      continue;
    }
    const name = p.slice(0, idx).trim();
    let val = p.slice(idx + 1).trim();
    if (val.startsWith('"') && val.endsWith('"')) val = val.slice(1, -1);
    cookieStore[name] = val;
  }
}

function buildCookieHeader() {
  return Object.entries(cookieStore).map(([k, v]) => `${k}=${v}`).join('; ');
}

function updateCookiesFromResponse(resp) {
  const sc = resp.headers && resp.headers['set-cookie'];
  if (!sc) return;
  for (const c of sc) {
    const first = c.split(';')[0];
    const idx = first.indexOf('=');
    if (idx === -1) continue;
    const name = first.slice(0, idx).trim();
    let val = first.slice(idx + 1).trim();
    if (val.startsWith('"') && val.endsWith('"')) val = val.slice(1, -1);
    cookieStore[name] = val;
  }
}


// Constants for API interaction
const API_BASE_URL = 'https://www.texasfile.com';
const SEARCH_ENDPOINT = '/search/texas/';
const RESULTS_ENDPOINT = '/search-results-single-api/'; // base endpoint
const REQUEST_DELAY_MS = 2000;

// Lien type keywords to match against document types
const LIEN_KEYWORDS = [
    "LIEN", "MECHANICS LIEN", "HOSPITAL LIEN",
    "TAX LIEN FEDERAL", "TAX LIEN STATE",
    "LIS PENDENS", "JUDGEMENT", "UCC",
    "NOTICE OF TRUSTEE SALE", "DEED OF TRUST",
    "RELEASE"
];
// Headers and cookies from fresh cURL
const baseHeaders = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'DNT': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'Referer': 'https://www.texasfile.com/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin'
};


// In-memory cache for API responses
const responseCache = new Map();

/**
 * Makes an API request to TexasFile with proper headers and error handling
 */
async function getTexasFileData(county, name) {
    // Normalize county and name
    const countyName = county.replace(/\s*COUNTY\s*$/, '').trim().toLowerCase();
    const searchName = name.trim();
    const name_type = 'GRGE';
    const startDate = '1846-07-01'; // or earliest needed
    const endDate = new Date().toISOString().split('T')[0];
    // Seed cookies if provided via env
    await seedCookies();

    // Step 1: GET init search page to establish session and extract searchId/csrf
    const initUrl = `${API_BASE_URL}/search/texas/${countyName}-county/county-clerk-records/`;
    let initResp;
    try {
        initResp = await client.get(initUrl, { headers: { ...baseHeaders, Cookie: buildCookieHeader() } });
        updateCookiesFromResponse(initResp);
    } catch (e) {
        // If this is a TLS certificate verification failure and the user allows insecure retries, try once with a permissive agent
        const isCertError = (e && (String(e.message).includes('unable to get issuer certificate') || String(e.message).includes('certificate') || e.code === 'UNABLE_TO_VERIFY_LEAF_SIGNATURE' || e.code === 'DEPTH_ZERO_SELF_SIGNED_CERT'));
        if (isCertError && process.env.ALLOW_INSECURE === '1') {
            try {
                const insecureAgent = new https.Agent({ rejectUnauthorized: false });
                const insecureClient = axios.create({ httpsAgent: insecureAgent, withCredentials: true });
                // swap global client so subsequent steps use the same agent
                client = insecureClient;
                initResp = await client.get(initUrl, { headers: { ...baseHeaders, Cookie: buildCookieHeader() } });
                updateCookiesFromResponse(initResp);
            } catch (e2) {
                throw e2;
            }
        } else {
            throw e;
        }
    }
    await setTimeout(500);

    // Try to extract searchId and csrfToken from __NEXT_DATA__ or links
    let searchId = null;
    let csrfToken = null;
    try {
        const $ = cheerio.load(initResp.data);
        const nextDataHtml = $('#__NEXT_DATA__').html();
        if (nextDataHtml) {
            try {
                const parsed = JSON.parse(nextDataHtml);
                csrfToken = parsed?.props?.pageProps?.csrfToken || parsed?.props?.csrfToken || parsed?.csrfToken;
                searchId = parsed?.props?.pageProps?.search_id || parsed?.props?.pageProps?.searchId || parsed?.props?.pageProps?.id;
            } catch (e) {
                // ignore
            }
        }
        if (!searchId) {
            // scan anchors for numeric token
            $('a[href]').each((i, a) => {
                const h = $(a).attr('href');
                const m = h?.match(/search\/(?:texas|statewide)\/[^\/]+\/(\d+)/);
                if (m && !searchId) searchId = m[1];
            });
        }
    } catch (e) {
    }
    // Try to extract a county code/id mapping from __NEXT_DATA__ if present
    let countyCodeFound = null;
    try {
        const $ = cheerio.load(initResp.data);
        const nextDataHtml = $('#__NEXT_DATA__').html();
        if (nextDataHtml) {
            let parsed = null;
            try { parsed = JSON.parse(nextDataHtml); } catch (e) { parsed = null; }
            if (parsed) {
                // deep scan for arrays that may contain county objects
                function findCountyCode(obj, targetCounty) {
                    if (!obj || typeof obj !== 'object') return null;
                    if (Array.isArray(obj)) {
                        for (const el of obj) {
                            if (el && typeof el === 'object') {
                                const name = el.county_name || el.county__name || el.name || el.county;
                                if (name && String(name).toLowerCase().includes(targetCounty.toLowerCase())) {
                                    return el.id || el.county_id || el.code || el.countyCode || el.county_code || el.value || null;
                                }
                            }
                        }
                        return null;
                    }
                    for (const k of Object.keys(obj)) {
                        try {
                            const res = findCountyCode(obj[k], targetCounty);
                            if (res) return res;
                        } catch (e) {}
                    }
                    return null;
                }
                countyCodeFound = findCountyCode(parsed, countyName) || null;
               
            }
        }
    } catch (e) {
        // non-fatal
    }
    if (!searchId) {
        searchId = process.env.SEARCH_ID || null;
    }
    if (!searchId) {
    }

    // When debugging, write the raw init page HTML to a file and try a raw-regex search for numeric searchId tokens
    try {
        if (process.env.DUMP_JSON === '1') {
            const safeCounty = countyName.replace(/[^a-z0-9_-]/gi, '_').slice(0,50);
            const dir = './debug_responses';
            try { mkdirSync(dir, { recursive: true }); } catch (e) {}
            const htmlFile = `${dir}/init_${safeCounty}_${Date.now()}.html`;
        

            // Try regex scan for numeric tokens used in Referer (e.g., /.../51499099/)
            try {
                const raw = String(initResp.data);
                const re = /search\/(?:texas|statewide)\/[^\/]+\/(\d{5,10})/gi;
                let m;
                while ((m = re.exec(raw)) !== null) {
                    if (m[1]) {
                        searchId = searchId || m[1];
                    }
                }
                // generic fallback: any 6-9 digit sequence bounded by slashes
                if (!searchId) {
                    const re2 = /\/(\d{6,9})\//g;
                    while ((m = re2.exec(raw)) !== null) {
                        // ignore timestamps that look like epoch > 1600000000
                        const val = m[1];
                        if (val && !String(val).startsWith('20')) {
                            searchId = searchId || val;
                        }
                    }
                }
            } catch (e) {
            }
        }
    } catch (e) {
        // non-fatal
    }

    // Step 2: GET fetch-name-suggestions to refresh session (non-fatal)
    try {
        const prefix = encodeURIComponent(searchName.split(' ')[0].slice(0, 5));
        const countyCode = process.env.COUNTY_CODE || '1';
        const suggestUrl = `${API_BASE_URL}/fetch-name-suggestions?prefix=${prefix}&name-type=${name_type}&county=${countyCode}`;
        const fetchResp = await client.get(suggestUrl, { headers: { ...baseHeaders, Referer: initUrl, 'X-Requested-With': 'XMLHttpRequest', Cookie: buildCookieHeader() } });
        updateCookiesFromResponse(fetchResp);
        await setTimeout(300);
    } catch (e) {
    }

    // Step 3: GET search-results-single-api (use base endpoint; searchId belongs in Referer)
    const resultsUrl = `${API_BASE_URL}${RESULTS_ENDPOINT}`;
    const params = {
        name1: searchName,
        name_type1: name_type,
        startDate,
        endDate,
        county: countyCodeFound || countyName
    };
    console.log('[STEP 3] API call details:', { resultsUrl, params });
    // Use Referer that includes the numeric searchId (if found) to match browser behavior
    const refererForResults = searchId ? `${initUrl}${searchId}/` : initUrl;
    let resultsResp;
    try {
        // Ensure common search state is present as cookies (the browser often stores name/type/date in cookies)
        try {
            cookieStore['name1'] = cookieStore['name1'] || searchName;
            cookieStore['name_type1'] = cookieStore['name_type1'] || name_type;
            cookieStore['startDate'] = cookieStore['startDate'] || startDate;
            cookieStore['endDate'] = cookieStore['endDate'] || endDate;
            if (countyCodeFound) cookieStore['county'] = cookieStore['county'] || countyCodeFound;
        } catch (e) {}

        const headers = { ...baseHeaders, Referer: refererForResults, Cookie: buildCookieHeader(), 'X-Requested-With': 'XMLHttpRequest' };
        // include CSRF header if present in cookies or parsed token
        if (cookieStore.csrftoken) headers['X-CSRFToken'] = cookieStore.csrftoken;
        if (csrfToken) headers['X-CSRFToken'] = csrfToken;
        resultsResp = await client.get(resultsUrl, { headers, params });
        updateCookiesFromResponse(resultsResp);
    } catch (e) {
        if (e.response) {
            // print a bit more of the response body for debugging
            try { console.error('Body snippet:', String(e.response.data).slice(0, 2000)); } catch (_) {}
        }
        throw e;
    }
    if (!resultsResp.headers['content-type'] || !resultsResp.headers['content-type'].includes('application/json')) {
    }

    // Lightweight debug: log top-level keys and scan for candidate arrays that may contain document hits
    try {
        const topKeys = resultsResp.data && typeof resultsResp.data === 'object' ? Object.keys(resultsResp.data) : [];

        if (process.env.DUMP_JSON === '1') {
            // Heuristic: find arrays of objects that contain likely document fields
            const candidates = {};
            const keyRegex = /date|type|doc|instrument|record|url|filing|case|party/i;
            function scan(obj, path) {
                if (!obj || typeof obj !== 'object') return;
                if (Array.isArray(obj)) {
                    if (obj.length > 0 && typeof obj[0] === 'object') {
                        const keys = Object.keys(obj[0]).join(' ');
                        if (keyRegex.test(keys)) {
                            candidates[path] = { length: obj.length, sample: obj.slice(0,3) };
                        }
                    }
                    // also scan elements
                    obj.slice(0,5).forEach((el, i) => scan(el, `${path}[${i}]`));
                    return;
                }
                for (const k of Object.keys(obj)) {
                    scan(obj[k], path ? `${path}.${k}` : k);
                }
            }
            try { scan(resultsResp.data, 'root'); } catch (e) { /* ignore scan errors */ }

            const safeName = (params.name1 || 'search').toString().replace(/[^a-z0-9_-]/gi, '_').slice(0,50);
            const dir = './debug_responses';
            try { mkdirSync(dir, { recursive: true }); } catch (e) {}
            const summaryF = `${dir}/${params.county || 'county'}_${safeName}_summary_${Date.now()}.json`;
            try {
                writeFileSync(summaryF, JSON.stringify({ topKeys, candidates }, null, 2));
            } catch (e) {  }
        }
    } catch (e) {
    }

    // Heuristic: if the response looks like the site navigation payload (contains data.cities_list etc.)
    // then attempt one retry using cookies (name1 etc) and county code if we found one.
    try {
        if (resultsResp && resultsResp.data && resultsResp.data.data && resultsResp.data.data.cities_list) {
            // Refresh cookies and retry once with the same params (but ensure cookie header is present)
            try {
                // ensure cookies already set above
                const headers2 = { ...baseHeaders, Referer: refererForResults, Cookie: buildCookieHeader(), 'X-Requested-With': 'XMLHttpRequest' };
                const retryResp = await client.get(resultsUrl, { headers: headers2, params });
                updateCookiesFromResponse(retryResp);
                resultsResp = retryResp;
            } catch (e) {
            }
        }
    } catch (e) {
        // ignore
    }

    // Optionally dump the full JSON response for debugging when DUMP_JSON=1 is set
    if (process.env.DUMP_JSON === '1') {
        try {
            const safeName = searchName.replace(/[^a-z0-9_-]/gi, '_').slice(0, 50);
            const dir = './debug_responses';
            try { mkdirSync(dir, { recursive: true }); } catch (e) {}
            const fname = `${dir}/${countyName}_${safeName}_${Date.now()}.json`;
            writeFileSync(fname, JSON.stringify(resultsResp.data, null, 2));
        } catch (e) {
        }
    }
    return resultsResp.data;
}

/**
 * Extracts lien information from API response
 */
function extractLienInfo(apiResponse, searchName = '') {
    try {
        // Try known/expected shapes first
        let hits = [];
        if (apiResponse?.response?.response?.data?.hits?.hits) {
            hits = apiResponse.response.response.data.hits.hits;
        } else if (apiResponse?.response?.data?.hits?.hits) {
            hits = apiResponse.response.data.hits.hits;
        } else if (Array.isArray(apiResponse?.hits)) {
            hits = apiResponse.hits;
        } else if (apiResponse?.response?.hits) {
            hits = apiResponse.response.hits;
        }

        // Helper: recursively scan for arrays that contain the searchName (or likely case tokens)
        function findCandidateArrayByText(obj, text) {
            const candidates = [];
            const lower = (text || '').toString().toLowerCase();
            function scan(o, path) {
                if (!o || typeof o !== 'object') return;
                if (Array.isArray(o)) {
                    // quick heuristic: serialize small sample and look for the search text
                    const sample = JSON.stringify(o.slice(0, 5)).toLowerCase();
                    if (lower && sample.includes(lower)) {
                        candidates.push({ path, arr: o });
                    } else {
                        // also scan first few elements
                        for (let i = 0; i < Math.min(o.length, 5); i++) scan(o[i], `${path}[${i}]`);
                    }
                    return;
                }
                for (const k of Object.keys(o)) scan(o[k], path ? `${path}.${k}` : k);
            }
            try { scan(obj, 'root'); } catch (e) {}
            // choose the largest candidate array if multiple found
            if (candidates.length === 0) return null;
            candidates.sort((a, b) => b.arr.length - a.arr.length);
            return candidates[0].arr;
        }

        // If no hits found via known paths, try to discover candidate arrays that contain the searchName
        if ((!hits || hits.length === 0) && searchName) {
            const candidate = findCandidateArrayByText(apiResponse, searchName.split(/[ ,]+/)[0] || searchName);
            if (candidate && Array.isArray(candidate) && candidate.length > 0) {
                hits = candidate;
            }
        }

        // Final fallback: find any array whose items contain LIEN_KEYWORDS in their serialized form
        if ((!hits || hits.length === 0)) {
            function findByLienKeyword(obj) {
                const found = [];
                const kwords = LIEN_KEYWORDS.map(k => k.toLowerCase());
                function scan(o) {
                    if (!o || typeof o !== 'object') return;
                    if (Array.isArray(o)) {
                        const s = JSON.stringify(o.slice(0, 5)).toLowerCase();
                        if (kwords.some(kw => s.includes(kw.toLowerCase()))) {
                            found.push(o);
                        } else {
                            for (let i = 0; i < Math.min(o.length, 5); i++) scan(o[i]);
                        }
                        return;
                    }
                    for (const k of Object.keys(o)) scan(o[k]);
                }
                try { scan(obj); } catch (e) {}
                if (found.length === 0) return null;
                found.sort((a, b) => b.length - a.length);
                return found[0];
            }
            const byLien = findByLienKeyword(apiResponse);
            if (byLien) hits = byLien;
        }

        if (!hits || hits.length === 0) {
            return {
                lien_present: false,
                lien_types: [],
                lien_count: 0,
                last_lien_date: null,
                matching_doc_urls: []
            };
        }

        // Normalize and heuristically extract document type, date, and URL from each hit
        const lienTypes = new Set();
        let lastLienDate = null;
        const matchingUrls = new Set();
        let matchingCount = 0;

        const dateRegexIso = /\d{4}-\d{2}-\d{2}/;
        const dateRegexUS = /\d{1,2}\/\d{1,2}\/\d{2,4}/;
        const urlRegex = /(https?:\/\/[^\s"']+|\/[^\s"']+\.(pdf|htm|html))/i;

        for (const item of hits) {
            try {
                const s = JSON.stringify(item || '').toUpperCase();
                // check for lien keyword presence
                const hasLien = LIEN_KEYWORDS.some(k => s.includes(k.toUpperCase()));
                if (!hasLien) continue;
                matchingCount++;

                // find type: look for known keys first
                let type = null;
                if (item && typeof item === 'object') {
                    type = item.type || item.document_type || item.doc_type || item.instrument_type || item.title || item.topic || item.name;
                    // try scanning values
                    if (!type) {
                        for (const v of Object.values(item)) {
                            if (typeof v === 'string' && LIEN_KEYWORDS.some(k => v.toUpperCase().includes(k.toUpperCase()))) { type = v; break; }
                        }
                    }
                }
                if (type) lienTypes.add(String(type).trim());

                // find date
                let foundDate = null;
                if (item && typeof item === 'object') {
                    for (const v of Object.values(item)) {
                        if (typeof v === 'string') {
                            const m1 = v.match(dateRegexIso);
                            const m2 = v.match(dateRegexUS);
                            if (m1) { foundDate = m1[0]; break; }
                            if (m2) { foundDate = m2[0]; break; }
                        }
                    }
                } else if (typeof item === 'string') {
                    const m1 = item.match(dateRegexIso) || item.match(dateRegexUS);
                    if (m1) foundDate = m1[0];
                }
                if (foundDate) {
                    try {
                        const d = new Date(foundDate);
                        if (!lastLienDate || d > new Date(lastLienDate)) lastLienDate = foundDate;
                    } catch (e) {}
                }

                // find URL
                if (item && typeof item === 'object') {
                    for (const v of Object.values(item)) {
                        if (typeof v === 'string') {
                            const mu = v.match(urlRegex);
                            if (mu) matchingUrls.add(mu[0]);
                        }
                    }
                } else if (typeof item === 'string') {
                    const mu = item.match(urlRegex);
                    if (mu) matchingUrls.add(mu[0]);
                }

            } catch (e) {
                // skip problematic item
            }
        }

        if (matchingCount === 0) {
            return {
                lien_present: false,
                lien_types: [],
                lien_count: 0,
                last_lien_date: null,
                matching_doc_urls: []
            };
        }

        return {
            lien_present: true,
            lien_types: Array.from(lienTypes),
            lien_count: matchingCount,
            last_lien_date: lastLienDate,
            matching_doc_urls: Array.from(matchingUrls)
        };
    } catch (error) {
        return {
            lien_present: false,
            lien_types: [],
            lien_count: 0,
            last_lien_date: null,
            matching_doc_urls: []
        };
    }
}

/**
 * Main processing function to enrich CSV data with lien information
 */
async function processCSV() {
    console.log('TexasFile enrichment started', {
    testLimit: process.env.TEST_LIMIT || null,
     });
    const results = [];
    
    try {
        // Seed cookies once from COOKIES env (if provided)
        await seedCookies();

        await new Promise((resolve, reject) => {
            createReadStream('texas-future-sales.csv')
                .pipe(csv())
                .on('data', async (row) => {
                    // Skip records without required fields
                    const county = row.County || row.county;
                    const name = row.Name || row.case_style;
                    if (!county || !name) {
                        return;
                    }


                    // Normalize the row to have expected field names
                    row.County = county;
                    row.Name = name;
                    
                    // Only process a limited number of records in test mode
                    if (process.env.TEST_LIMIT && results.length >= parseInt(process.env.TEST_LIMIT)) {
                        return;
                    }
                    results.push(row);
                })
                .on('end', resolve)
                .on('error', reject);
        });
    // ✅ STEP 1 – Validate CSV parse
            console.log('[STEP 1] Parsed rows:', results.length);
            console.table(results.slice(0, 5));
        // Process records sequentially with rate limiting
        const enrichedResults = [];
        for (const row of results) {
            try {
                // Retry wrapper for transient failures
                const maxRetries = 2;
                let attempt = 0;
                let apiResponse = null;
                while (attempt <= maxRetries) {
                    try {
                        apiResponse = await getTexasFileData(row.County, row.Name);
                        break;
                    } catch (e) {
                        attempt++;
                        if (attempt > maxRetries) throw e;
                        const backoff = 1000 * Math.pow(2, attempt);
                        await setTimeout(backoff);
                    }
                }
                if (apiResponse) {
                    const lienInfo = extractLienInfo(apiResponse, row.Name);
                    enrichedResults.push({
                        ...row,
                        ...lienInfo
                    });
                } else {
                    enrichedResults.push({
                        ...row,
                        lien_types: [],
                        last_lien_date: null,
                        matching_doc_urls: []
                    });
                }
                
                // Rate limiting delay between requests
                await setTimeout(REQUEST_DELAY_MS);
                
            } catch (error) {
                enrichedResults.push({
                    ...row,
                    lien_types: [],
                    last_lien_date: null,
                    matching_doc_urls: [],
                    error: error.message
                });
            }
        }

        // Write results to CSV, preserving original columns and appending lien fields
        const outputPath = 'future_sales_with_liens.csv';
        // Build header from first result (original columns) if available
        const first = enrichedResults[0] || results[0] || {};
        const originalCols = Object.keys(first).filter(k => !['lien_present','lien_types','lien_count','last_lien_date','matching_doc_urls','error'].includes(k));
        const header = originalCols.map(c => ({ id: c, title: c }));
        // Append lien columns
        header.push({ id: 'lien_present', title: 'lien_present' });
        header.push({ id: 'lien_types', title: 'lien_types' });
        header.push({ id: 'lien_count', title: 'lien_count' });
        header.push({ id: 'last_lien_date', title: 'last_lien_date' });
        header.push({ id: 'matching_doc_urls', title: 'matching_doc_urls' });
        header.push({ id: 'error', title: 'error' });

        const csvWriter = createObjectCsvWriter({ path: outputPath, header });
        await csvWriter.writeRecords(enrichedResults.map(r => {
            // ensure all header keys exist on row and stringify arrays
            const out = {};
            for (const h of header) {
                const v = r[h.id] !== undefined ? r[h.id] : '';
                if (Array.isArray(v)) out[h.id] = v.join(' | ');
                else out[h.id] = v;
            }
            return out;
        }));
        console.log('TexasFile enrichment finished', { rows: enrichedResults.length, output: outputPath });

    } catch (error) {
        throw error;
    }
}

// Run the main process
processCSV().catch(error => {
     console.error('TexasFile enrichment failed', err.message);
  process.exit(1);  
});
