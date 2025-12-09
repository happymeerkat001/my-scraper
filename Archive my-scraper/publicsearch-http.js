// PublicSearch.us HTTP Driver
// HTTP-only version using axios + jsdom (no Puppeteer)

import axios from 'axios';
import { JSDOM } from 'jsdom';

export class PublicSearchHttpDriver {
  constructor(config = {}) {
    this.config = config;

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
    return 'publicsearch-http';
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
      return [];
    }

    console.log(`🔗 [publicsearch-http] ${url}`);

    try {
      const response = await axios.get(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        },
        timeout: 15000
      });

      return this.parseRecords(response.data);

    } catch (e) {
      console.log(`❌ Error:`, e.message);
      throw e;
    }
  }

  parseRecords(html) {
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

  async cleanup() {
    // No browser to cleanup in HTTP driver
  }
}
