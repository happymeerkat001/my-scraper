// PublicSearch.us Driver
// Handles counties using tx.publicsearch.us system

import { BaseDriver, TIMEOUTS } from './baseDriver.js';

// Classification constants for response analysis
const CLASSIFICATION = {
  MIN_VALID_HTML_LENGTH: 5000,
  BLOCKED_PATTERNS: [
    /access\s+denied/i,
    /too\s+many\s+requests/i,
    /rate\s+limit/i,
    /please\s+try\s+again\s+later/i,
    /service\s+unavailable/i,
    /temporarily\s+unavailable/i,
    /request\s+blocked/i
  ]
};

export class PublicSearchDriver extends BaseDriver {
  constructor(config = {}) {
    super(config);

    this.COUNTY_MAP = {
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
  }

  getName() {
    return 'publicsearch';
  }

  canHandle(county) {
    return !!this.COUNTY_MAP[county];
  }

  buildURL(county, name) {
    const subdomain = this.COUNTY_MAP[county];
    if (!subdomain) return null;

    const today = new Date().toISOString().split('T')[0].replace(/-/g, '');
    const encoded = encodeURIComponent(`"${name.toLowerCase()}"`);

    return `https://${subdomain}.tx.publicsearch.us/results?department=RP&keywordSearch=false&limit=50&offset=0&recordedDateRange=18000101%2C${today}&searchOcrText=false&searchType=quickSearch&searchValue=${encoded}&sort=desc&sortBy=recordedDate`;
  }

  async search(county, name, options = {}) {
    const url = this.buildURL(county, name);
    if (!url) {
      console.log(`⚠️  ${county} not in PublicSearch system`);
      return {
        records: [],
        classification: 'error:invalid_county',
        htmlLength: 0,
        indicators: ['county_not_mapped']
      };
    }

    console.log(`🔗 URL: ${url}`);

    let page;
    try {
      page = await this.safePageCreate();
      await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36');

      // Use domcontentloaded (faster) instead of networkidle2 (waits for all resources)
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });

      // Robust wait loop: poll for table rows until they exist
      console.log('⏳ Waiting for results table to render...');

      const maxAttempts = 30; // 30 attempts × 500ms = 15s max wait
      const checkIntervalMs = 500;
      let rowCount = 0;

      for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        const rows = await page.$$('.a11y-table table tbody tr');
        rowCount = rows.length;

        if (rowCount > 0) {
          console.log(`✓ Table ready with ${rowCount} rows`);
          break;
        }

        // Wait before next check (unless this is the last attempt)
        if (attempt < maxAttempts) {
          await new Promise(resolve => setTimeout(resolve, checkIntervalMs));
        }
      }

      // If no rows found after max wait, classify the response
      if (rowCount === 0) {
        const html = await page.content();
        const htmlLength = html.length;

        // Classify the empty response
        let classification = 'no_results:confirmed';
        let indicators = [];

        // Check for suspiciously short HTML (likely blocked)
        if (htmlLength < CLASSIFICATION.MIN_VALID_HTML_LENGTH) {
          classification = 'blocked:soft_block';
          indicators.push('html_too_short');
        } else {
          // Check for blocked patterns in HTML
          for (const pattern of CLASSIFICATION.BLOCKED_PATTERNS) {
            if (pattern.test(html)) {
              classification = 'blocked:rate_limit';
              indicators.push('blocked_pattern_found');
              break;
            }
          }
        }

        return {
          records: [],
          classification,
          htmlLength,
          indicators
        };
      }

      const html = await page.content();
      const htmlLength = html.length;

      // Reset failure counter on success
      this.consecutiveFailures = 0;

      const records = await this.parseRecords(html);
      return {
        records,
        classification: records.length > 0 ? 'success' : 'no_results:confirmed',
        htmlLength,
        indicators: []
      };

    } catch (e) {
      console.log(`❌ Error:`, e.message);

      // Track connection failures for browser health
      if (e.message.includes('Connection closed') || e.message.includes('Target closed')) {
        this.consecutiveFailures++;
      }

      throw e;
    } finally {
      if (page) await page.close();
    }
  }

  async parseRecords(html) {
    const jsdom = await import('jsdom');
    const { JSDOM } = jsdom;
    const dom = new JSDOM(html);
    const document = dom.window.document;

    const TABLE_BASE = '#main-content > div > div > div.search-results__results-wrap > div.a11y-table > table > tbody';

    const getCell = (row, col) => {
      const selector = `${TABLE_BASE} > tr:nth-child(${row}) > td.col-${col}`;
      const element = document.querySelector(selector);
      return element ? element.textContent.trim() : '';
    };

    const liens = [];

    // Extract up to 3 rows
    for (let row = 1; row <= 3; row++) {
      const grantor = getCell(row, 3);

      // Stop if no more data
      if (!grantor) break;

      const record = {
        grantor,
        grantee: getCell(row, 4),
        docType: getCell(row, 5),
        recordedDate: getCell(row, 6),
        docNumber: getCell(row, 7),
        bookVolumePage: getCell(row, 8),
        legalDescription: getCell(row, 9),
        references: getCell(row, 10)
      };

      liens.push(record);
    }

    return liens;
  }
}
