import axios from 'axios';
import { createReadStream, readFileSync } from 'fs';
import { createObjectCsvWriter } from 'csv-writer';
import csv from 'csv-parser';
import { setTimeout } from 'timers/promises';
import * as cheerio from 'cheerio';
import https from 'https';

// Load DigiCert intermediate and wire it into a single HTTPS agent
const caPath = new URL('./DigiCertGlobalG2TLSRSASHA2562020CA1.crt.pem', import.meta.url);
const caBuffer = readFileSync(caPath);

console.log('Loading CA from', caPath.href);

const httpsAgent = new https.Agent({
  rejectUnauthorized: true,
  ca: caBuffer,
});

// Use this client for every outbound request
const client = axios.create({
  httpsAgent,
  withCredentials: true,
});


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

// Cookies captured from cURL
const freshCookies = [
  '_gid=GA1.2.1991157647.1762201370',
  'name1="MARTIN A C"',
  'name_type1=GRGE',
  'startDate=1846-07-01',
  'endDate=2025-09-30',
  '_ga=GA1.2.1794663500.1761088860',
  '_gat=1',
  'csrftoken=KhElxXunu2r5SHJoU1Ur0F0ZzqMYqRRE',
  'sessionid=u4hbx8jjso202z1oi5on3mq4y8cbvthv',
  '_ga_7PW1S43RCX=GS2.1.s1762357840$o7$g1$t1762361575$j24$l0$h0'
];
const cookieHeader = freshCookies.join('; ');

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

    // Step 1: GET init search page (optional, since cookies are fresh)
    // If you want to mimic browser, you can still GET the init page
    // const initUrl = `${API_BASE_URL}/search/texas/${countyName}-county/county-clerk-records/`;
    // console.log(`[Step 1] GET init page: ${initUrl}`);
    // const initResp = await client.get(initUrl, { headers: baseHeaders });
    // await setTimeout(1000);

    // Step 2: GET fetch-name-suggestions (optional, can uncomment if needed)
    // const countyCode = 1; // TODO: update with real county code mapping
    // const fetchNameUrl = `${API_BASE_URL}/fetch-name-suggestions?prefix=${encodeURIComponent(searchName.split(' ')[0])}&name-type=${name_type}&county=${countyCode}`;
    // console.log(`[Step 2] GET fetch-name-suggestions: ${fetchNameUrl}`);
    // const fetchResp = await client.get(fetchNameUrl, { headers: baseHeaders });
    // await setTimeout(1000);

    // Step 3: GET search-results-single-api/<id>/
    // You must supply the numeric search token (id) in the endpoint, e.g. /search-results-single-api/51440742/
    // This should be extracted from the Referer or set as a parameter
    let searchId = null;
    // Try to extract from Referer header if present
    const refererMatch = baseHeaders.Referer.match(/search\/(?:texas|statewide)\/[^\/]+\/(\d+)/);
    if (refererMatch) {
        searchId = refererMatch[1];
    }
    // Fallback: use a hardcoded or default value if not found
    if (!searchId) {
        searchId = '51440742'; // TODO: update with dynamic value if needed
    }
    const resultsUrl = `${API_BASE_URL}/search-results-single-api/${searchId}/`;
    const params = {
        name1: '"MARTIN A C"',
        name_type1: 'GRGE',
        startDate: '1846-07-01',
        endDate: '2025-09-30',
        county: countyName
    };
    console.log(`[Step 3] GET search-results-single-api: ${resultsUrl}`);
    const resultsResp = await client.get(resultsUrl, {
        headers: { ...baseHeaders, Cookie: cookieHeader },
        params
    });
    console.log('[Step 3] Response content-type:', resultsResp.headers['content-type']);
    if (resultsResp.headers['content-type'] !== 'application/json') {
        console.error('[Step 3] Non-JSON response:', resultsResp.data);
    }
    return resultsResp.data;
}

/**
 * Extracts lien information from API response
 */
function extractLienInfo(apiResponse) {
    try {
        if (!apiResponse?.response?.response?.data?.hits?.hits) {
            return {
                lien_types: [],
                last_lien_date: null,
                matching_doc_urls: []
            };
        }

        const hits = apiResponse.response.response.data.hits.hits;
        const matchingHits = hits.filter(hit => {
            const docType = hit._source.type?.toUpperCase() || '';
            return LIEN_KEYWORDS.some(keyword => docType.includes(keyword.toUpperCase()));
        });

        if (matchingHits.length === 0) {
            return {
                lien_types: [],
                last_lien_date: null,
                matching_doc_urls: []
            };
        }

        const lienTypes = new Set();
        let lastLienDate = null;
        const matchingUrls = new Set();

        matchingHits.forEach(hit => {
            const date = hit._source.date_filed;
            const type = hit._source.type;
            const url = hit._source.url;

            if (type) lienTypes.add(type);
            if (date) {
                if (!lastLienDate || new Date(date) > new Date(lastLienDate)) {
                    lastLienDate = date;
                }
            }
            if (url) matchingUrls.add(url);
        });

        return {
            lien_types: Array.from(lienTypes),
            last_lien_date: lastLienDate,
            matching_doc_urls: Array.from(matchingUrls)
        };
    } catch (error) {
        console.error('Error extracting lien info:', error);
        return {
            lien_types: [],
            last_lien_date: null,
            matching_doc_urls: []
        };
    }
}

/**
 * Main processing function to enrich CSV data with lien information
 */
async function processCSV() {
    const results = [];
    
    try {
        await new Promise((resolve, reject) => {
            createReadStream('texas-future-sales.csv')
                .pipe(csv())
                .on('data', async (row) => {
                    // Skip records without required fields
                    const county = row.County || row.county;
                    const name = row.Name || row.case_style;
                    if (!county || !name) {
                        console.warn('Skipping row missing county or name:', row);
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

        // Process records sequentially with rate limiting
        const enrichedResults = [];
        for (const row of results) {
            try {
                const apiResponse = await getTexasFileData(row.County, row.Name);
                if (apiResponse) {
                    const lienInfo = extractLienInfo(apiResponse);
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
                console.error(`Error processing ${row.County} - ${row.Name}:`, error.message);
                enrichedResults.push({
                    ...row,
                    lien_types: [],
                    last_lien_date: null,
                    matching_doc_urls: [],
                    error: error.message
                });
            }
        }

        // Write results to CSV
        const csvWriter = createObjectCsvWriter({
            path: 'texas-future-sales-details.csv',
            header: [
                { id: 'County', title: 'County' },
                { id: 'Name', title: 'Name' },
                { id: 'lien_types', title: 'Lien Types' },
                { id: 'last_lien_date', title: 'Last Lien Date' },
                { id: 'matching_doc_urls', title: 'Matching Doc URLs' },
                { id: 'error', title: 'Error' }
            ]
        });

        await csvWriter.writeRecords(enrichedResults);
        console.log('Data processing completed');

    } catch (error) {
        console.error('Error processing CSV:', error);
        throw error;
    }
}

// Run the main process
processCSV().catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
});
