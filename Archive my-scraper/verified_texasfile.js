//#region Step 0 – Imports & Cross-Cut Dependencies
// Connect > Import — pull in HTTP, file, CSV, and timing utilities that touch external systems
import axios from 'axios';
import { createReadStream, existsSync, readFileSync } from 'fs';
import { createObjectCsvWriter } from 'csv-writer';
import csv from 'csv-parser';
import { setTimeout } from 'timers/promises';
import https from 'https';
import tls from 'tls';
//#endregion

const API_BASE_URL = 'https://www.texasfile.com';
const SEARCH_ENDPOINT = '/search/texas/';
const RESULTS_ENDPOINT = '/search-results-single-api/';
const REQUEST_DELAY_MS = 2000;

const baseHeaders = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'DNT': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"'
};
//#endregion

//#region Step 0.2 – HTTPS client + retry helpers
// Connect > Environment — wire TLS + axios client so later LISTEN calls share a session
let httpsAgent;
try {
    const caPath = new URL('./DigiCertGlobalG2TLSRSASHA2562020CA1.crt.pem', import.meta.url);
    const caBuffer = readFileSync(caPath);
    const rejectUnauthorized = process.env.NODE_TLS_REJECT_UNAUTHORIZED !== '0';
    const defaultRoots = Array.isArray(tls.rootCertificates) ? tls.rootCertificates.slice() : [];
    defaultRoots.push(caBuffer.toString());
    httpsAgent = new https.Agent({ rejectUnauthorized, ca: defaultRoots });
} catch (e) {
    const rejectUnauthorized = process.env.NODE_TLS_REJECT_UNAUTHORIZED !== '0';
    httpsAgent = new https.Agent({ rejectUnauthorized });
}
let client = axios.create({ httpsAgent, withCredentials: true });

// Memory > State — cookie helpers preserve session data between LISTEN/API calls
const cookieStore = Object.create(null);
const cookieEnv = process.env.COOKIES || 'sessionid=t0sk0w5mn183t8abaipebxxtyhzs01zm; csrftoken=Lps1pt6D0JQzwg5pHP730rZbhipJGWJi; name1="MARTIN A C"; name_type1=GRGE; startDate=1846-07-01; endDate=2025-10-31';

async function seedCookies() {
    if (!cookieEnv) return;
    const parts = cookieEnv.split(/;\s*/);
    for (const p of parts) {
        const idx = p.indexOf('=');
        if (idx === -1) continue;
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

function isCertError(err) {
    const code = err?.code || '';
    const msg = String(err?.message || '');
    return code === 'UNABLE_TO_VERIFY_LEAF_SIGNATURE' ||
        code === 'DEPTH_ZERO_SELF_SIGNED_CERT' ||
        msg.includes('unable to get issuer certificate') ||
        msg.includes('unable to verify the first certificate') ||
        msg.includes('self signed certificate');
}

async function safeGet(url, config) {
    try {
        return await client.get(url, config);
    } catch (err) {
        if (process.env.ALLOW_INSECURE === '1' && isCertError(err)) {
            const insecureAgent = new https.Agent({ rejectUnauthorized: false });
            client = axios.create({ httpsAgent: insecureAgent, withCredentials: true });
            return await client.get(url, config);
        }
        throw err;
    }
}
//#endregion

//#region Step 1 – Input CSV (read + normalize names)
// Memory > Helper — Step 1 (Input) reader: load CSV rows so we can log/verify upstream data
async function readCSV(filePath, limit = null) {
    return new Promise((resolve, reject) => {
        const rows = [];
        createReadStream(filePath)
            .pipe(csv())
            .on('data', (row) => {
                if (!limit || rows.length < limit) {
                    rows.push(row);
                }
            })
            .on('end', () => resolve(rows))
            .on('error', reject);
    });
}

function extractNameFromCaseStyle(caseStyle) {
    if (!caseStyle) return null;
    const parts = caseStyle.split(/\s+v(?:s\.?)?\s+/i);
    if (parts.length < 2) return null;
    let defendant = parts.slice(1).join(' ').trim();
    defendant = defendant.replace(/[,;]\s*(AKA|A\/K\/A|D\/B\/A|F\/K\/A|DBA)\b.*$/i, '');
    defendant = defendant.replace(/\b(AKA|A\/K\/A|D\/B\/A|F\/K\/A|DBA)\b.*$/i, '');
    defendant = defendant.split(/\bET\s+AL\.?/i)[0];
    defendant = defendant.split(/\s+AND\s+/i)[0];
    defendant = defendant.split(/\s*&\s*/)[0];
    defendant = defendant.replace(/^[\s,:;-]+/, '').replace(/[\s,:;-]+$/, '');
    defendant = defendant.replace(/\s{2,}/g, ' ').trim();
    return defendant || null;
}

// Run > Plan > Choose — isolate the owner name column the pipeline promises to enrich
function extractOwnerName(row) {
    const caseStyleName = extractNameFromCaseStyle(row.case_style || row.caseStyle);
    if (caseStyleName) return caseStyleName;
    return row.owner_name || row.name || row.uid || Object.values(row)[0];
}

// Memory > Helper — prepare consistent query params for Step 2 URL validation
function buildSearchParams(ownerName, countySlug, searchId) {
    const params = {
    county: countySlug,
        search_id: searchId,
        name1: ownerName,
        name_type1: 'GRGE',
        startDate: '1846-07-01',
        endDate: new Date().toISOString().split('T')[0]
    };
    console.log('buildSearchParams:', params);
    return params;
}
//#endregion

//#region Step 2 + 3 – Fetch TexasFile HTML + JSON to enrich each owner
async function fetchData(ownerName, county = '') {
    const rawCounty = county ? String(county).trim() : '';
    const countyBase = rawCounty.replace(/\s*county\s*$/i, '').trim().toLowerCase();
    const countySlug = 'anderson-county'; // Placeholder: replace with actual mapping if needed 
    const searchName = ownerName.trim();

    await seedCookies();

    const searchId = "51608411"; // Placeholder: replace with Puppeteer later
    const initUrl = `${API_BASE_URL}/search-results-single-api/${searchId}/`;
    console.log('Init URL:', initUrl);

    const results = await fetch(initUrl, {
  headers: {
    "accept": "application/json",
    "referer": `https://www.texasfile.com/search/texas/anderson-county/county-clerk-records/${searchId}/`,
    "user-agent": "Mozilla/5.0"
  }
}).then(r => r.json());

console.log(results);



    try {
        const initResp = await safeGet(initUrl, {
            headers: { ...baseHeaders, Cookie: buildCookieHeader() }
        });
        updateCookiesFromResponse(initResp);
        const urlMatch = initResp.request?.path?.match(/\/(\d+)\/?$/);
        if (urlMatch) searchId = urlMatch[1];
        console.log('Derived searchId:', searchId);
    } catch (e) {
        console.warn('[Init] Failed to get init page:', e.message);
    }


    const resultsUrl = `${API_BASE_URL}${RESULTS_ENDPOINT}`;
    const params = buildSearchParams(searchName, countySlug, searchId);
    const refererUrl = searchId ? `${initUrl}${searchId}/` : initUrl;

    cookieStore.name1 = searchName;
    cookieStore.name_type1 = params.name_type1;
    cookieStore.startDate = params.startDate;
    cookieStore.endDate = params.endDate;

    const headers = {
        ...baseHeaders,
        Origin: API_BASE_URL,
        Referer: refererUrl,
        Cookie: buildCookieHeader(),
        'X-Requested-With': 'XMLHttpRequest',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Dest': 'empty'
    };

    try {
        const resp = await safeGet(resultsUrl, { headers, params });
        updateCookiesFromResponse(resp);
        if (!resp.headers['content-type']?.includes('application/json')) {
            throw new Error('Invalid response type');
        }
        return resp.data;
        console.log('Results URL:', resultsUrl,   
            params);
    } catch (e) {
        console.error('[Search] Failed to get results:', e.message);
        throw e;
    }
}
//#endregion

// #region Step 4 – Parse API payload into lien rows
function extractLiens(results) {
    const liens = [];
    try {
        // Look for hits array in results data
        const hits = results?.hits?.hits || 
                    results?.data?.hits?.hits ||
                    results?.documents ||
                    [];
        console.log('extractLiens keys:', Object.keys(results || {}));
        console.log('hits length:', hits.length);
T
        for (const hit of hits.slice(0, 5)) {
            const doc = hit._source || hit;
            if (doc.documentType?.toLowerCase().includes('lien') ||
                doc.title?.toLowerCase().includes('lien')) {
                liens.push({
                    name: doc.title || doc.documentType,
                    date: doc.recordingDate || doc.filingDate || doc.date
                }); 
            }
        }
    } catch (e) {
    }
    return liens;
}

function formatResults(ownerName, liens) {
    const row = { owner_name: ownerName };
    liens.forEach((lien, i) => {
        row[`lien${i + 1}`] = lien.name;
        row[`date${i + 1}`] = lien.date;
    });
    return row;
}
//#endregion

//#region Step 1 helper – Locate incoming CSV
const INPUT_FALLBACKS = ['input.csv', 'texas-future-sales.csv'];

function resolveInputFile() {
    if (process.env.INPUT_FILE) return process.env.INPUT_FILE;
    for (const candidate of INPUT_FALLBACKS) {
        if (existsSync(candidate)) return candidate;
    }
    throw new Error(`Input file not found. Provide INPUT_FILE or add one of: ${INPUT_FALLBACKS.join(', ')}`);
}
//#endregion

// temporary test function
async function testTexasFileAPI() {
  const searchId = "51603973"; // The ID from your browser
  const initUrl = `https://www.texasfile.com/search-results-single-api/${searchId}/`;

  console.log("Requesting:", initUrl);

  try {
    const res = await fetch(initUrl, {
      headers: {
        "accept": "application/json",
        "referer": `https://www.texasfile.com/search/texas/anderson-county/county-clerk-records/${searchId}/`,
        "user-agent": "Mozilla/5.0"
      }
    });

    console.log("Status:", res.status);

    const json = await res.json();
    console.log("JSON keys:", Object.keys(json));
    console.log(json);

  } catch (err) {
    console.error("TEST ERROR:", err);
  }
}

testTexasFileAPI();

// #region Step 5 – Orchestrate read → fetch → parse → write
async function main() {
    const inputFile = resolveInputFile();
    const outputFile = process.env.OUTPUT_FILE || 'output.csv';
    const testLimit = parseInt(process.env.TEST_LIMIT || '2', 10);
    
    try {
        // Step 1: read CSV into memory (optionally limited for test runs)
        const rows = await readCSV(inputFile, testLimit);
        
        const results = [];
        for (const row of rows) {
            const ownerName = extractOwnerName(row);
            const countyValue = row.county || row.county_name || row.countyName || '';
            
            let retries = 3;
            while (retries > 0) {
                try {
                    // Step 2/3: remote lookup for the owner
                    const data = await fetchData(ownerName, countyValue);
                    console.log('payload type:', typeof data.data, Array.isArray(data.data), Object.keys(data.data || {}));
                    // Step 4: extract liens from JSON
                    const liens = extractLiens(data);
                    // Step 4b: normalize into row form
                    results.push(formatResults(ownerName, liens));
                    break;
                } catch (e) {
                    retries--;
                    if (retries === 0) {
                        results.push({ owner_name: ownerName, error: e.message });
                    } else {
                        await setTimeout(REQUEST_DELAY_MS);
                    }
                }
            }
            
            // Step 2 guardrail: space out requests to avoid throttling
            await setTimeout(REQUEST_DELAY_MS);
        }
        
        // Step 5: write output CSV so downstream tools can consume results
        const csvWriter = createObjectCsvWriter({
            path: outputFile,
            header: [
                { id: 'owner_name', title: 'Owner Name' },
                { id: 'lien1', title: 'Lien 1' },
                { id: 'date1', title: 'Date 1' },
                { id: 'lien2', title: 'Lien 2' },
                { id: 'date2', title: 'Date 2' },
                { id: 'lien3', title: 'Lien 3' },
                { id: 'date3', title: 'Date 3' },
                { id: 'lien4', title: 'Lien 4' },
                { id: 'date4', title: 'Date 4' },
                { id: 'lien5', title: 'Lien 5' },
                { id: 'date5', title: 'Date 5' },
                { id: 'error', title: 'Error' }
            ]
        });
        
        await csvWriter.writeRecords(results);
        
    } catch (e) {
        console.error('Script failed:', e);
        process.exit(1);
    }
}
//#endregion

//#region Step 6 – Entry point
// Connect > Entry — kick off the pipeline and bubble fatal errors to stderr
main().catch(console.error);
//#endregion
