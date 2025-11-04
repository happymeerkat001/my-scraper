import axios from 'axios';
import { createReadStream } from 'fs';
import { createObjectCsvWriter } from 'csv-writer';
import csv from 'csv-parser';
import { setTimeout } from 'timers/promises'; // For modern sleep/delay
import * as cheerio from 'cheerio';

// Constants for API interaction
const API_BASE_URL = 'https://www.texasfile.com';
const SEARCH_ENDPOINT = '/search/statewide/county-clerk-records/';
const RESULTS_ENDPOINT = '/search-results-single-api/';
const REQUEST_DELAY_MS = 2000;

// Lien type keywords to match against document types
const LIEN_KEYWORDS = [
    "LIEN", "MECHANICS LIEN", "HOSPITAL LIEN",
    "TAX LIEN FEDERAL", "TAX LIEN STATE",
    "LIS PENDENS", "JUDGEMENT", "UCC",
    "NOTICE OF TRUSTEE SALE", "DEED OF TRUST",
    "RELEASE"
];

// Headers from the provided cURL command
const API_HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'DNT': '1',
    'Referer': 'https://www.texasfile.com/search/statewide/county-clerk-records/51440742/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"'
};

// Cookies from the provided cURL command
const API_COOKIES = 'csrftoken=L90N5V8Rul4VsP1Uglmf06eXqhWfzQbu; sessionid=tvyzu4bxb0u15sdcw69sea6nigselrp5; _gid=GA1.2.1991157647.1762201370; name1="BRENDA SANTOS"; name_type1=GRGE; startDate=1800-01-01; endDate=2025-11-03; _ga=GA1.2.1794663500.1761088860; _ga_7PW1S43RCX=GS2.1.s1762208351$o4$g0$t1762208351$j60$l0$h0';

// In-memory cache for API responses
const responseCache = new Map();

/**
 * Extracts legal description components for searching
 * @param {string} legalDesc - The legal description text
 * @returns {Object} Extracted search parameters
 */
function extractLegalComponents(legalDesc) {
    const components = {};
    
    // Extract common patterns like "Lot X, Block Y" or "Volume Z, Page W"
    const lotMatch = legalDesc.match(/LOT\s+(\d+)/i);
    const blockMatch = legalDesc.match(/BLOCK\s+(\d+)/i);
    const volumeMatch = legalDesc.match(/VOLUME\s+(\d+)/i);
    const pageMatch = legalDesc.match(/PAGE\s+(\d+)/i);
    
    if (lotMatch) components.lot = lotMatch[1];
    if (blockMatch) components.block = blockMatch[1];
    if (volumeMatch) components.volume = volumeMatch[1];
    if (pageMatch) components.page = pageMatch[1];
    
    return components;
}

/**
 * Makes an API request to TexasFile with proper headers and error handling
 * @param {Object} params - Search parameters
 * @returns {Promise<Object>} API response data
 */
async function getTexasFileData(county, name, causeNumber, legalDesc) {
    const cacheKey = `${county}:${name}:${causeNumber}`;
    if (responseCache.has(cacheKey)) {
        console.log(`[Cache Hit] Using cached data for ${cacheKey}`);
        return responseCache.get(cacheKey);
    }

    // Create axios instance with SSL verification disabled (only for testing)
    const instance = axios.create({
        httpsAgent: new (await import('https')).Agent({
            rejectUnauthorized: false
        })
    });

    try {
        // Extract county name and defendant name
        const countyName = county.replace(/\s*COUNTY\s*$/, '').trim().toLowerCase();
        const defendantName = name.split(' VS ')[1]?.trim() || name;

        // Prepare search parameters
        const name1 = encodeURIComponent(defendantName);
        const name_type1 = "GRGE";
        const startDate = "1800-01-01";
        const endDate = "2025-11-03";

        // First make the search request to initiate the search
        // First get the results page to set up the session
        await instance.get(`${API_BASE_URL}${SEARCH_ENDPOINT}`, {
            headers: {
                ...API_HEADERS,
                'Cookie': API_COOKIES,
                'Referer': 'https://www.texasfile.com/search/statewide/county-clerk-records/51440742/'
            }
        });

        // Then make the actual search request with the exact cookie format from the working curl command
        const searchResponse = await instance.get(`${API_BASE_URL}${RESULTS_ENDPOINT}`, {
            headers: {
                ...API_HEADERS,
                'Cookie': `csrftoken=L90N5V8Rul4VsP1Uglmf06eXqhWfzQbu; sessionid=tvyzu4bxb0u15sdcw69sea6nigselrp5; _gid=GA1.2.1991157647.1762201370; name1="${defendantName}"; name_type1=GRGE; startDate=${startDate}; endDate=${endDate}; _ga=GA1.2.1794663500.1761088860; _ga_7PW1S43RCX=GS2.1.s1762208351$o4$g0$t1762208351$j60$l0$h0`,
                'Referer': 'https://www.texasfile.com/search/statewide/county-clerk-records/51440742/'
            },
            transformResponse: [(data) => {
                // Try parsing as JSON first
                try {
                    return JSON.parse(data);
                } catch (e) {
                    // If not JSON, parse as HTML
                    const $ = cheerio.load(data);
                    const results = [];
                    
                    // Extract records from HTML table
                    $('table tr').each((i, tr) => {
                        const cols = $(tr).find('td');
                        if (cols.length > 0) {
                            results.push({
                                document_id: $(cols[0]).find('a').attr('href')?.split('/').pop(),
                                document_type: $(cols[1]).text().trim(),
                                date_filed: $(cols[2]).text().trim(),
                                county_name: countyName
                            });
                        }
                    });

                    return { results };
                }
            }]
        });
        
        // Debug response
        console.log('DEBUG: Response:', JSON.stringify(searchResponse.data, null, 2));

        // Validate and parse the response
        const records = searchResponse.data?.results || [];
        if (!records.length) {
            console.log(`No records found for ${countyName} - ${defendantName}`);
            return null;
        }

        // Process records with document details
        const details = records.map(record => ({
            document_id: record.document_id,
            document_type: record.document_type,
            type: record.document_type,
            date_filed: record.date_filed,
            county_name: countyName,
            url: record.document_id ? 
                `${API_BASE_URL}/search/texas/${countyName}/county-clerk-records/${record.document_id}/` :
                null
        }));

        // Debug: log sample of the response data
        if (details.length > 0) {
            console.log('DEBUG: Sample Detail Response:', JSON.stringify(details[0], null, 2));
        }

        // Format response to match expected structure
        const formattedResponse = {
            response: {
                response: {
                    data: {
                        hits: {
                            total: details.length,
                            hits: details.map(record => ({
                                _source: {
                                    ...record,
                                    county: { name: record.county_name }
                                }
                            }))
                        }
                    }
                }
            }
        };

        // Cache the successful response
        responseCache.set(cacheKey, formattedResponse);
        
        return formattedResponse;
    } catch (error) {
        if (error.response?.status === 429) {
            console.warn(`Rate limited for ${county}. Waiting longer...`);
            await setTimeout(REQUEST_DELAY_MS * 2);
            return getTexasFileData(county, name, causeNumber, legalDesc);
        }
        console.error(`API error for ${county}:`, error.message);
        return null;
    }
}

/**
 * Processes API response to extract lien information
 * @param {Array} records - API response records
 * @returns {Object} Extracted lien information
 */
function extractLienInfo(apiResponse) {
    // Default response for no data
    const emptyResult = {
        lien_present: false,
        lien_types: [],
        lien_count: 0,
        last_lien_date: null,
        matching_doc_urls: []
    };

    // Handle null or undefined response
    if (!apiResponse?.response?.response?.data?.hits?.hits) {
        return emptyResult;
    }

    const records = apiResponse.response.response.data.hits.hits;
    if (!Array.isArray(records)) {
        return emptyResult;
    }

    // Find records that match lien keywords
    const matchingRecords = records.filter(hit => {
        const source = hit._source;
        const docType = (source.type || source.county_type || source.document_type || '').toUpperCase();
        return LIEN_KEYWORDS.some(keyword => docType.includes(keyword));
    });

    if (matchingRecords.length === 0) {
        return emptyResult;
    }

    // Process matching records
    const lienTypes = new Set();
    const docUrls = new Set();
    let lastLienDate = null;

    matchingRecords.forEach(hit => {
        const record = hit._source;
        
        // Extract and add document type
        const docType = (record.type || record.county_type || record.document_type || '').toUpperCase();
        if (docType) {
            lienTypes.add(docType);
        }

        // Update last lien date
        const dateStr = record.date_filed || record.date || record.recorded_date;
        if (dateStr) {
            const date = new Date(dateStr);
            if (!lastLienDate || date > lastLienDate) {
                lastLienDate = date;
            }
        }

        // Add document URL
        if (record.url) {
            docUrls.add(record.url);
        } else if (record.document_id) {
            // Construct URL using document_id and county
            const county = record.county?.name?.toLowerCase() || record.county?.toLowerCase() || '';
            if (county) {
                docUrls.add(`https://www.texasfile.com/search/texas/${county}/county-clerk-records/${record.document_id}/`);
            }
        } else if (record.number) {
            // Fallback to basic document number URL
            docUrls.add(`https://www.texasfile.com/document/${record.number}`);
        }
    });

    return {
        lien_present: true,
        lien_types: Array.from(lienTypes),
        lien_count: matchingRecords.length,
        last_lien_date: lastLienDate ? lastLienDate.toISOString().split('T')[0] : null,
        matching_doc_urls: Array.from(docUrls)
    };
}

/**
 * Main function to process the CSV file
 */
async function processCSV() {
    const results = [];
    let processed = 0;
    
    // Create CSV reader stream
    const records = [];
    await new Promise((resolve, reject) => {
        createReadStream('texas-future-sales.csv')
            .pipe(csv())
            .on('data', (row) => records.push(row))
            .on('end', resolve)
            .on('error', reject);
    });

    const total = records.length;
    console.log(`Processing ${total} records...`);

    // Process each record
    for (const row of records) {
        processed++;
        console.log(`[${processed}/${total}] Processing ${row.case_style} in ${row.county}`);

        // Get data from TexasFile API
        const apiData = await getTexasFileData(
            row.county,
            row.case_style,
            row.cause_number,
            row.legal_description
        );

        // Wait between requests
        await setTimeout(REQUEST_DELAY_MS);

        // Extract lien information
        const lienInfo = extractLienInfo(apiData?.records || []);

        // Combine original row with lien info
        results.push({
            ...row,
            ...lienInfo
        });
    }

    // Write enriched data to new CSV
    const csvWriter = createObjectCsvWriter({
        path: 'future_sales_with_liens.csv',
        header: [
            // Original columns
            { id: 'uid', title: 'uid' },
            { id: 'county', title: 'county' },
            { id: 'case_style', title: 'case_style' },
            { id: 'cause_number', title: 'cause_number' },
            { id: 'legal_description', title: 'legal_description' },
            // New columns
            { id: 'lien_present', title: 'lien_present' },
            { id: 'lien_types', title: 'lien_types' },
            { id: 'lien_count', title: 'lien_count' },
            { id: 'last_lien_date', title: 'last_lien_date' },
            { id: 'matching_doc_urls', title: 'matching_doc_urls' }
        ]
    });

    await csvWriter.writeRecords(results);
    console.log('✅ Enriched data written to future_sales_with_liens.csv');
}

// Run the script
processCSV().catch(console.error);