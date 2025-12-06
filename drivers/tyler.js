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
    // Normalize county name to handle case variations
    const normalizedCounty = county.toUpperCase();
    const countyKey = Object.keys(this.COUNTY_MAP).find(
      key => key.toUpperCase() === normalizedCounty
    );
    return !!countyKey;
  }

  buildURL(county) {
    // Normalize county name to handle case variations
    const normalizedCounty = county.toUpperCase();
    const countyKey = Object.keys(this.COUNTY_MAP).find(
      key => key.toUpperCase() === normalizedCounty
    );
    const config = countyKey ? this.COUNTY_MAP[countyKey] : null;
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

      // Navigate to search page (increased timeout for slow TylerTech sites)
      await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });

      // Check for disclaimer page and accept if present
      const disclaimerButton = await page.$('#submitDisclaimerAccept').catch(() => null);
      if (disclaimerButton) {
        console.log(`📋 Accepting disclaimer...`);

        // Wait for button to be enabled (it starts disabled)
        console.log(`   ⏳ Waiting for disclaimer button to be enabled...`);
        await page.waitForFunction(
          () => !document.querySelector('#submitDisclaimerAccept').disabled,
          { timeout: 10000 }
        );
        console.log(`   ✓ Button enabled`);

        // Click the button
        console.log(`   🖱️  Clicking disclaimer button...`);
        await disclaimerButton.click();
        console.log(`   ✓ Button clicked`);

        // Wait for search form to appear (disclaimer uses AJAX, not page navigation)
        console.log(`   ⏳ Waiting for search form to appear...`);
        try {
          await page.waitForSelector('#field_BothNamesID', { timeout: 15000 });
          console.log(`   ✓ Search form ready`);
        } catch (formError) {
          console.log(`   ⚠️  Search form not found: ${formError.message}`);
          // Continue anyway - might already have results
        }
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

      // Wait for results to load (Tyler uses AJAX, not page navigation)
      console.log(`   ⏳ Waiting for search results...`);
      await page.waitForSelector('.selfServiceSearchRowRight', { timeout: 30000 });

      // Brief pause for all results to render
      await new Promise(resolve => setTimeout(resolve, 1000));

      const html = await page.content();

      // DEBUG: Save results HTML
      const fs = await import('fs');
      fs.default.writeFileSync('tyler-results-debug.html', html);
      console.log(`   📄 Saved results HTML to tyler-results-debug.html`);

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

      // Extract document number and type from h1 header
      // Format: "2025034159 • AFFIDAVIT OF HEIRSHIP"
      const h1 = row.querySelector('h1');
      const h1Text = h1 ? h1.textContent.replace(/\s+/g, ' ').trim() : '';

      // Parse h1: "2025034159 • AFFIDAVIT OF HEIRSHIP" or "P18494 • PROBATE COPY"
      const h1Match = h1Text.match(/^([A-Z0-9-]+)\s*[•·]\s*(.+)$/i);
      const docNumber = h1Match ? h1Match[1].trim() : '';
      const docType = h1Match ? h1Match[2].trim() : '';

      // Extract fields from li structure
      // Structure: <li>Label</li> followed by <li class="selfServiceSearchResultCollapsed"><b>value</b></li>
      const getText = (label) => {
        const lis = Array.from(row.querySelectorAll('li'));
        for (let j = 0; j < lis.length; j++) {
          if (lis[j].textContent.trim().startsWith(label)) {
            // Next li with <b> contains the value
            const nextLi = lis[j + 1];
            if (nextLi) {
              const b = nextLi.querySelector('b');
              return b ? b.textContent.trim() : '';
            }
          }
        }
        return '';
      };

      const record = {
        grantor: getText('Grantor'),
        grantee: getText('Grantee'),
        docType,
        recordedDate: getText('Recording Date'),
        docNumber,
        bookVolumePage: '',
        legalDescription: getText('Legal'),
        references: ''
      };

      console.log(`   Row ${i + 1}: ${record.docNumber} | ${record.docType} | ${record.grantor}`);

      liens.push(record);
    }

    return liens;
  }
}
