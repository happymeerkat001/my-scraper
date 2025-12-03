// TylerTech Driver
// Handles counties using tylerhost.net system

import { BaseDriver } from './baseDriver.js';

export class TylerDriver extends BaseDriver {
  constructor(config = {}) {
    super(config);

    this.COUNTY_MAP = {
      'LAMAR COUNTY': { subdomain: 'lamarcounty', searchCode: 'DOCSEARCH1307S1' },
      'KAUFMAN COUNTY': { subdomain: 'kaufmancounty', searchCode: 'DOCSEARCH1008S7' },
      'McLENNAN COUNTY': { subdomain: 'mclennancounty', searchCode: 'DOCSEARCH402S1' },
      'NAVARRO COUNTY': { subdomain: 'navarrocounty', searchCode: 'DOCSEARCH144S1' }
    };
  }

  getName() {
    return 'tyler';
  }

  canHandle(county) {
    return !!this.COUNTY_MAP[county];
  }

  buildURL(county) {
    const config = this.COUNTY_MAP[county];
    if (!config) return null;
    return `https://${config.subdomain}tx-web.tylerhost.net/web/search/${config.searchCode}`;
  }

  async search(county, name, options = {}) {
    const url = this.buildURL(county);
    if (!url) {
      console.log(`⚠️  ${county} not in TylerTech system`);
      return [];
    }

    console.log(`🔗 URL: ${url}`);
    console.log(`✏️  Searching for: "${name}" (TylerTech uses "LAST FIRST" format)`);

    let page;
    try {
      await this.initBrowser();
      page = await this.browser.newPage();
      await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36');

      // Navigate to search page
      await page.goto(url, { waitUntil: 'networkidle2', timeout: 15000 });

      // Check for disclaimer page and accept if present
      const disclaimerButton = await page.$('#submitDisclaimerAccept').catch(() => null);
      if (disclaimerButton) {
        console.log(`📋 Accepting disclaimer...`);
        // Wait for button to be enabled (it starts disabled)
        await page.waitForFunction(
          () => !document.querySelector('#submitDisclaimerAccept').disabled,
          { timeout: 10000 }
        );
        await disclaimerButton.click();
        // Wait for navigation after clicking
        await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 15000 }).catch(() => {});
      }

      // Wait for either search form OR results (flexible wait)
      await Promise.race([
        page.waitForSelector('#field_BothNamesID', { timeout: 15000 }),
        page.waitForSelector('.selfServiceSearchRowRight', { timeout: 15000 })
      ]).catch(async () => {
        // If both fail, dump HTML for debugging
        console.log(`⚠️  Neither form nor results found, dumping HTML...`);
        const html = await page.content();
        const fs = await import('fs');
        const timestamp = Date.now();
        fs.default.writeFileSync(`tyler-debug-${timestamp}.html`, html);
        console.log(`   Saved to tyler-debug-${timestamp}.html`);
      });

      // Check if we're already on results page
      const hasResults = await page.$('.selfServiceSearchRowRight').catch(() => null);
      if (hasResults) {
        console.log(`✓ Already on results page, skipping search form`);
        const html = await page.content();
        return await this.parseRecords(html);
      }

      // We're on search form, fill it out
      await page.type('#field_BothNamesID', name);

      // Click search button
      await page.click('#searchButton');

      // Wait for results page to load
      await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 15000 });

      // Wait for results to populate
      await new Promise(resolve => setTimeout(resolve, 3000));

      const html = await page.content();

      return await this.parseRecords(html);

    } catch (e) {
      console.log(`❌ Error:`, e.message);
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

    const liens = [];
    const rows = document.querySelectorAll('.selfServiceSearchRowRight');

    console.log(`   Found ${rows.length} result rows`);

    for (let i = 0; i < Math.min(rows.length, 3); i++) {
      const row = rows[i];

      // Extract document number and type from first line
      const docLine = row.querySelector('.searchResultDocument');
      const docText = docLine ? docLine.textContent.trim() : '';

      // Parse: "2022-0031608 • B: OPR V: 7769 P: 65 • RELEASE OF LIEN"
      const docMatch = docText.match(/^([^\u2022]+)\s*\u2022\s*(.+?)\s*\u2022\s*(.+)$/);
      const docNumber = docMatch ? docMatch[1].trim() : '';
      const bookVolumePage = docMatch ? docMatch[2].trim() : '';
      const docType = docMatch ? docMatch[3].trim() : '';

      // Extract other fields
      const getText = (label) => {
        const li = Array.from(row.querySelectorAll('li')).find(el =>
          el.textContent.includes(label)
        );
        if (!li) return '';
        const b = li.querySelector('b');
        return b ? b.textContent.trim() : '';
      };

      const record = {
        grantor: getText('Grantor'),
        grantee: getText('Grantee'),
        docType,
        recordedDate: getText('Recording Date'),
        docNumber,
        bookVolumePage,
        legalDescription: getText('Legal Description'),
        references: ''
      };

      liens.push(record);
    }

    return liens;
  }
}
