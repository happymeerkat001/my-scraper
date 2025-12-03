// import necessary modules
  import puppeteer from 'puppeteer';
  import { createReadStream } from 'fs';
  import { createObjectCsvWriter } from 'csv-writer';
  import csv from 'csv-parser';
  import { setTimeout } from 'timers/promises';

//  Configuration

  // Only counties with verified working PublicSearch.us subdomains
  // Removed: ANGELINA, EL PASO, GALVESTON, MAVERICK, MCLENNAN, ZAVALA (DNS fails or 0% success)
  const COUNTY_MAP = {
    'ANDERSON COUNTY': 'anderson', 'BEE COUNTY': 'bee', 'CAMERON COUNTY': 'cameron',
    'ECTOR COUNTY': 'ector', 'ERATH COUNTY': 'erath', 'FORT BEND COUNTY': 'fortbend',
    'FRIO COUNTY': 'frio', 'GRAYSON COUNTY': 'grayson', 'HARRIS COUNTY': 'harris',
    'HIDALGO COUNTY': 'hidalgo', 'HUNT COUNTY': 'hunt', 'JEFFERSON COUNTY': 'jefferson',
    'KERR COUNTY': 'kerr', 'LAMPASAS COUNTY': 'lampasas', 'MIDLAND COUNTY': 'midland',
    'MONTGOMERY COUNTY': 'montgomery', 'NUECES COUNTY': 'nueces', 'POTTER COUNTY': 'potter',
    'SAN PATRICIO COUNTY': 'sanpatricio', 'SMITH COUNTY': 'smith', 'TARRANT COUNTY': 'tarrant',
    'TRAVIS COUNTY': 'travis', 'UPSHUR COUNTY': 'upshur', 'UVALDE COUNTY': 'uvalde',
    'VICTORIA COUNTY': 'victoria', 'WALLER COUNTY': 'waller', 'WASHINGTON COUNTY': 'washington',
    'WEBB COUNTY': 'webb', 'WHARTON COUNTY': 'wharton', 'WICHITA COUNTY': 'wichita',
    'WILLIAMSON COUNTY': 'williamson', 'WISE COUNTY': 'wise', 'WOOD COUNTY': 'wood',
    'YOUNG COUNTY': 'young', 'ZAPATA COUNTY': 'zapata'
  };

  const BUSINESS_KEYWORDS = /\b(LLC|INC|CORP|LP|LLP|LTD|CO|COMPANY|TRUST|ESTATE|BANK|INVESTMENTS|PROPERTIES|ENTERPRISES|MINISTRIES)\b/i;

  const LIEN_KEYWORDS = [
    'TAX LIEN', 'FEDERAL TAX LIEN', 'STATE TAX LIEN', 'MECHANIC', 'HOSPITAL LIEN',
    'CHILD SUPPORT LIEN', 'JUDGMENT', 'ABSTRACT OF JUDGMENT', 'LIS PENDENS', 'UCC'
  ];

  const NOISE_PATTERNS = [
    /\b(DECEASED|ESTATE OF|TRUSTEE|AS TRUSTEE|EXECUTOR OF|EXECUTRIX|HEIR|HEIRS OF)\b/gi,
    /\b(INDEPENDENT SCHOOL DISTRICT|SCHOOL DISTRICT|ISD|COUNTY SHERIFF|SHERIFF'?S DEED)\b/gi,
    /\s+COUNTY(?!\s+CLERK)/gi,
    /,?\s*(ET AL|AKA|DBA|FKA|A\/K\/A|D\/B\/A|F\/K\/A).*$/i
  ];

  const RETRY_MAX = 3;
  const RETRY_DELAY_MS = 500;
  const RATE_LIMIT_MS = 2000;
  const FETCH_TIMEOUT_MS = 10000;

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

  // STEP 2: Extract defendant name and generate search variations
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

    // Business detection (only use keywords, not uppercase count since names are in all caps)
    const isBusiness = BUSINESS_KEYWORDS.test(defendant);

    if (isBusiness) return { original: defendant, variations: [defendant] };

    // Individual: Name is in "First Middle Last" format, need to convert to "Last First Middle"
    const words = defendant.split(/\s+/).filter(w => w && !/^(JR|SR|III|II|IV)$/i.test(w));
    if (!words.length) return { original: defendant, variations: [] };

    const variations = [];
    const last = words[words.length - 1]; // Last word is the last name
    const given = words.slice(0, -1); // Everything before last word is first/middle

    // "Last First Middle"
    variations.push([last, ...given].join(' '));

    // "Last First" (drop middle)
    if (given.length > 1) {
      variations.push(`${last} ${given[0]}`);
    }

    // "Last" only
    variations.push(last);

    return { original: defendant, variations: [...new Set(variations)] };
  }

  // STEP 3: Build search URL for county and name
  function buildURL(county, name) {
    const subdomain = COUNTY_MAP[county];
    if (!subdomain) return null;

    const encoded = encodeURIComponent(`"${name.toLowerCase()}"`);
    const today = new Date().toISOString().split('T')[0].replace(/-/g, '');
    const url = `https://${subdomain}.tx.publicsearch.us/results?department=RP&keywordSearch=false&limit=50&offset=0&recordedDateRange=18000101%2C${today}&searchOcrText=false&searchType=quickSearch&searchValue=${encoded}&sort=desc&sortBy=recordedDate`;
    console.log(`\n🔗 URL for "${name}":`, url);
    return url;
  }

  // Browser instance for reuse
  let browser = null;

  // Fetch HTML with Puppeteer (client-side rendered)
  async function fetchHTML(url) {
    for (let i = 0; i < RETRY_MAX; i++) {
      try {
        if (!browser) {
          browser = await puppeteer.launch({
            headless: true,
            args: ['--no-sandbox', '--disable-setuid-sandbox']
          });
        }

        const page = await browser.newPage();
        await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36');

        await page.goto(url, { waitUntil: 'networkidle2', timeout: FETCH_TIMEOUT_MS });

        // Wait for loading placeholder to disappear and results to appear
        await page.waitForSelector('.table-placeholder', { hidden: true, timeout: 15000 }).catch(() => {
          console.log('⚠️  Loading placeholder still visible');
        });

        // Additional wait for table to fully render
        await new Promise(resolve => globalThis.setTimeout(resolve, 2000));

        const html = await page.content();
        await page.close();

        return html;
      } catch (e) {
        console.log(`⚠️  Fetch attempt ${i + 1}/${RETRY_MAX} failed:`, e.message);
        if (i < RETRY_MAX - 1) await setTimeout(RETRY_DELAY_MS);
        if (i === RETRY_MAX - 1) throw e;
      }
    }
  }

  // Check if document type is a lien
  function isLien(docType) {
    if (!docType) return false;
    const upper = docType.toUpperCase();
    return LIEN_KEYWORDS.some(keyword => upper.includes(keyword.toUpperCase()));
  }

  // STEP 4: Parse top 3 liens from server-rendered HTML table
  async function parseLiens(html) {
    const { JSDOM } = await import('jsdom');
    const liens = [];

    console.log(`\n🔍 Parsing HTML table with DOM selectors...`);

    // Create DOM from HTML
    const dom = new JSDOM(html);
    const document = dom.window.document;

    // Base selector prefix
    const TABLE_BASE = '#main-content > div > div > div.search-results__results-wrap > div.a11y-table > table > tbody';

    // Extract single cell value (handles nested em tags)
    const getCell = (row, col) => {
      const selector = `${TABLE_BASE} > tr:nth-child(${row}) > td.col-${col}`;
      const element = document.querySelector(selector);
      return element ? element.textContent.trim() : '';
    };

    // Extract full row data (columns 3-24)
    const getRow = (rowIndex) => {
      const rowData = {};
      for (let col = 3; col <= 24; col++) {
        const value = getCell(rowIndex, col);
        if (value) rowData[`col${col}`] = value;
      }
      return rowData;
    };

    // Extract all rows 1-3 (no filtering)
    for (let row = 1; row <= 3; row++) {
      const rowData = getRow(row);

      // Extract cols 3-10: grantor, grantee, docType, recordedDate, docNumber, bookVolumePage, legalDescription, references
      const record = {
        grantor: rowData.col3 || '',
        grantee: rowData.col4 || '',
        docType: rowData.col5 || '',
        recordedDate: rowData.col6 || '',
        docNumber: rowData.col7 || '',
        bookVolumePage: rowData.col8 || '',
        legalDescription: rowData.col9 || '',
        references: rowData.col10 || ''
      };

      console.log(`  Row ${row}: ${record.grantor} → ${record.grantee} | ${record.docType} | ${record.recordedDate}`);
      liens.push(record);
    }

    console.log(`Total rows extracted: ${liens.length}`);
    return liens;
  }

  // STEP 5: Fetch liens with name variation fallbacks
  async function fetchLiens(county, nameObj) {
    console.log(`\n📋 Name variations for "${nameObj.original}":`, nameObj.variations);
    if (!nameObj.variations.length) return { liens: [], error: 'parse_failed' };

    const subdomain = COUNTY_MAP[county];
    if (!subdomain) {
      console.log(`⚠️  ${county} is not supported - no PublicSearch.us subdomain found`);
      return { liens: [], error: 'unknown_county' };
    }

    for (let i = 0; i < nameObj.variations.length; i++) {
      console.log(`\n🔍 Trying variation ${i + 1}/${nameObj.variations.length}: "${nameObj.variations[i]}"`);
      const url = buildURL(county, nameObj.variations[i]);
      if (!url) continue;

      try {
        const html = await fetchHTML(url);
        console.log(`✓ Fetched HTML (${html.length} chars)`);

        // Save FULL HTML for debugging (first variation only)
        if (i === 0) {
          const fs = await import('fs');
          fs.writeFileSync('/Users/leon/Documents/Code/my-scraper/debug-full.html', html);
          console.log('💾 Saved full HTML to debug-full.html');
        }

        const liens = await parseLiens(html);
        console.log(`Found ${liens.length} liens:`, liens);
        if (liens.length > 0) return { liens, error: null };
      } catch (e) {
        console.log(`❌ Error:`, e.message);
        if (i === nameObj.variations.length - 1) {
          return { liens: [], error: e.message };
        }
      }
    }

    return { liens: [], error: null };
  }

  // STEP 6: Merge original row with scraped data (3 rows × 8 fields)
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

  // STEP 6: Write enriched rows to CSV
  async function writeCSV(rows, path) {
    if (!rows.length) return;
    const headers = Object.keys(rows[0]).map(id => ({ id, title: id }));
    await createObjectCsvWriter({ path, header: headers }).writeRecords(rows);
  }

  // STEP 7: Main orchestrator
  async function main() {
    const inputFile = process.env.INPUT_FILE || 'texas-future-sales.csv';
    const outputFile = process.env.OUTPUT_FILE || 'texas-sales-liens.csv';
    const limit = parseInt(process.env.TEST_LIMIT || '0', 10) || null;

    console.log(`Reading ${inputFile}...`);
    const rows = await readCSV(inputFile, limit);
    console.log(`Processing ${rows.length} rows\n`);

    const results = [];
    let success = 0;
    let unsupportedCount = 0;
    const unsupportedCounties = new Set();

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const nameObj = parseName(row.case_style);

      const { liens, error } = await fetchLiens(row.county, nameObj);
      results.push(enrichRow(row, liens));

      if (liens.length > 0) success++;
      if (error === 'unknown_county') {
        unsupportedCount++;
        unsupportedCounties.add(row.county);
      }

      if ((i + 1) % 10 === 0 || i === rows.length - 1) {
        console.log(`[${i + 1}/${rows.length}] ${row.county} | ${nameObj.original} → ${liens.length} rows`);
      }

      await setTimeout(RATE_LIMIT_MS);
    }

    await writeCSV(results, outputFile);
    console.log(`\n✓ Wrote ${results.length} rows to ${outputFile}`);
    console.log(`✓ Success: ${success}/${results.length} (${((success/results.length)*100).toFixed(1)}%)`);

    if (unsupportedCount > 0) {
      console.log(`\n⚠️  UNSUPPORTED COUNTIES: ${unsupportedCount} properties skipped`);
      console.log(`   Counties: ${Array.from(unsupportedCounties).sort().join(', ')}`);
      console.log(`   These counties do not use PublicSearch.us - alternative sources needed`);
    }

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