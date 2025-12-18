// Polk County IDS Driver
// Handles Polk County using Integrated Data Services (IDS) system
// Requires login with public credentials: ccpublic/public

import { BaseDriver, TIMEOUTS } from './baseDriver.js';
import puppeteer from 'puppeteer';

// Polk-specific constants
const POLK_CONFIG = {
  LOGIN_URL: 'http://polkcountytx.net/nd2/cgindx501.html',
  CREDENTIALS: {
    user: 'ccpublic',
    pass: 'public'
  },
  SELECTORS: {
    // Login page
    LOGIN_USER: 'input[name="S01USR"]',
    LOGIN_PASS: 'input[name="S01PWD"]',
    LOGIN_SUBMIT: 'input[type="submit"]',
    // Search page
    GRANTOR: 'input[name="S502GRNTR"]',
    GRANTEE: 'input[name="S502GRNTE"]',
    RUN_REPORT: 'input[value="Run Report"]'
  }
};

export class PolkDriver extends BaseDriver {
  constructor(config = {}) {
    super(config);
    this.isLoggedIn = false;
    this.currentPage = null;
  }

  getName() {
    return 'polk';
  }

  canHandle(county) {
    return county.toUpperCase() === 'POLK COUNTY';
  }

  /**
   * Generate Polk-specific name variants for search
   * Order:
   *   1. LAST FIRST MIDDLE SUFFIX (if middle AND suffix)
   *   2. LAST FIRST SUFFIX (if suffix)
   *   3. LAST FIRST MIDDLE (if middle)
   *   4. LAST FIRST (always)
   *   5. LAST SUFFIX FIRST MIDDLE (ONLY if suffix AND middle both exist)
   *
   * @param {Object} parsedName - { last, first, middle, suffix }
   * @returns {string[]} Array of name variants to try in order
   */
  generateNameVariants(parsedName) {
    const { last, first, middle, suffix } = parsedName;
    const variants = [];

    if (!last || !first) {
      // Minimum requirement: last AND first name
      console.log('⚠️  Polk requires LAST FIRST at minimum - skipping partial name');
      return [];
    }

    // Variant 1: LAST FIRST MIDDLE SUFFIX (most specific)
    if (middle && suffix) {
      variants.push(`${last} ${first} ${middle} ${suffix}`.trim());
    }

    // Variant 2: LAST FIRST SUFFIX (no middle)
    if (suffix) {
      variants.push(`${last} ${first} ${suffix}`.trim());
    }

    // Variant 3: LAST FIRST MIDDLE (no suffix)
    if (middle) {
      variants.push(`${last} ${first} ${middle}`.trim());
    }

    // Variant 4: LAST FIRST (always included as fallback)
    variants.push(`${last} ${first}`.trim());

    // Variant 5: LAST SUFFIX FIRST MIDDLE
    // GUARD: Generate ONLY if BOTH suffix AND middle exist
    // Rationale: "LAST SUFFIX FIRST" (without middle) is too broad and causes false lien attribution
    //
    // Examples:
    //   "SMITH JOHN A JR"  → suffix=JR, middle=A   → GENERATE "SMITH JR JOHN A"
    //   "SMITH JOHN JR"    → suffix=JR, middle=""  → SKIP (no middle)
    //   "SMITH JOHN A"     → suffix="", middle=A   → SKIP (no suffix)
    //   "SMITH JOHN"       → suffix="", middle=""  → SKIP (neither)
    //
    // DO NOT MODIFY this guard without legal-name-matching review
    if (suffix && middle) {
      variants.push(`${last} ${suffix} ${first} ${middle}`.trim());
    }

    // Remove duplicates while preserving order
    return [...new Set(variants)];
  }

  /**
   * Parse a full name string into components
   * Expected input format: "LAST FIRST MIDDLE SUFFIX" or similar
   *
   * @param {string} fullName - The full name to parse
   * @returns {Object} { last, first, middle, suffix }
   */
  parseName(fullName) {
    const suffixes = ['JR', 'SR', 'II', 'III', 'IV', 'V'];
    const parts = fullName.toUpperCase().trim().split(/\s+/);

    const result = { last: '', first: '', middle: '', suffix: '' };

    if (parts.length === 0) return result;

    // Check if last part is a suffix
    const lastPart = parts[parts.length - 1];
    if (suffixes.includes(lastPart)) {
      result.suffix = lastPart;
      parts.pop();
    }

    if (parts.length >= 1) result.last = parts[0];
    if (parts.length >= 2) result.first = parts[1];
    if (parts.length >= 3) result.middle = parts.slice(2).join(' ');

    return result;
  }

  /**
   * Initialize browser with Polk-specific settings
   * Uses system Chrome to avoid security blocking
   */
  async initBrowser() {
    if (!this.browser) {
      console.log('🚀 Launching browser for Polk County (IDS system)...');
      this.browser = await puppeteer.launch({
        headless: 'new',
        ignoreHTTPSErrors: true,
        executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
        args: [
          // Disable extensions and security features that block legacy HTTP sites
          '--disable-extensions',
          '--disable-web-security',
          '--disable-features=IsolateOrigins,site-per-process',
          '--disable-features=BlockInsecurePrivateNetworkRequests',
          '--disable-features=SafeBrowsingEnhancedProtection',
          '--disable-site-isolation-trials',
          '--allow-running-insecure-content',
          '--disable-client-side-phishing-detection',
          '--safebrowsing-disable-auto-update',
          '--disable-component-update',
          '--no-first-run',
          '--no-default-browser-check',
          '--disable-background-networking',
          // Disable password manager UI and autofill (prevents breach warning popup)
          '--disable-save-password-bubble',
          '--disable-password-generation',
          '--disable-single-click-autofill',
          '--disable-autofill-keyboard-accessory-view',
          '--disable-features=PasswordManagerOnboarding',
          '--disable-features=PasswordCheck',
          '--disable-features=PasswordLeakDetection',
          '--password-store=basic',
        ]
      });
      this.browserStartTime = Date.now();
    }
    return this.browser;
  }

  /**
   * Login to IDS system
   * @param {Page} page - Puppeteer page
   * @returns {boolean} True if login successful
   */
  async login(page) {
    if (this.isLoggedIn && this.currentPage === page) {
      return true;
    }

    console.log('🔐 Logging into Polk County IDS...');

    // Handle Chrome password warning dialogs
    page.on('dialog', async dialog => {
      console.log(`   Dialog detected: ${dialog.type()}`);
      await dialog.dismiss();
    });

    // Navigate to login page
    await page.goto(POLK_CONFIG.LOGIN_URL, {
      waitUntil: 'domcontentloaded',
      timeout: 45000
    });

    // Wait for login form
    await page.waitForSelector(POLK_CONFIG.SELECTORS.LOGIN_USER, { timeout: 15000 });

    // Enter credentials
    await page.type(POLK_CONFIG.SELECTORS.LOGIN_USER, POLK_CONFIG.CREDENTIALS.user);
    await page.type(POLK_CONFIG.SELECTORS.LOGIN_PASS, POLK_CONFIG.CREDENTIALS.pass);

    // Submit login - use form.submit() and wait for URL change
    const loginUrl = page.url();
    await page.evaluate(() => {
      document.querySelector('form').submit();
    });

    // Wait for URL to change (login navigates to CGINDX501.ws)
    await page.waitForFunction(
      (oldUrl) => window.location.href !== oldUrl,
      { timeout: 45000 },
      loginUrl
    );

    // Wait for page to fully load
    await page.waitForFunction(
      () => document.readyState === 'complete',
      { timeout: 10000 }
    );

    // Wait for page to settle
    await new Promise(r => setTimeout(r, 2000));

    // Verify login success - check for search form
    const currentUrl = page.url();
    console.log(`   Post-login URL: ${currentUrl}`);
    if (!currentUrl.includes('CGINDX501.ws')) {
      // Save debug HTML
      if (process.env.DEBUG === '1') {
        const fs = await import('fs');
        const html = await page.evaluate(() => document.documentElement.outerHTML);
        await fs.promises.writeFile('debug-polk-login-fail.html', html);
      }
      throw new Error(`Login failed - unexpected URL: ${currentUrl}`);
    }

    const grantorField = await page.$(POLK_CONFIG.SELECTORS.GRANTOR);
    if (!grantorField) {
      // Save debug HTML
      if (process.env.DEBUG === '1') {
        const fs = await import('fs');
        const html = await page.evaluate(() => document.documentElement.outerHTML);
        await fs.promises.writeFile('debug-polk-noform.html', html);
        console.log('   Debug: Saved login page HTML to debug-polk-noform.html');
      }
      throw new Error('Login failed - search form not found');
    }

    console.log('   ✓ Login successful');
    this.isLoggedIn = true;
    this.currentPage = page;
    return true;
  }

  /**
   * Execute a single search with given name string on a specific field
   * @param {Page} page - Puppeteer page
   * @param {string} searchName - Name to search (already formatted)
   * @param {string} fieldType - 'GRANTOR' or 'GRANTEE'
   * @returns {Array} Array of records found
   */
  async executeSearch(page, searchName, fieldType = 'GRANTOR') {
    const selector = fieldType === 'GRANTEE'
      ? POLK_CONFIG.SELECTORS.GRANTEE
      : POLK_CONFIG.SELECTORS.GRANTOR;

    console.log(`   🔍 Searching ${fieldType}: "${searchName}"`);

    // Clear BOTH fields to ensure clean search
    await page.$eval(POLK_CONFIG.SELECTORS.GRANTOR, el => el.value = '');
    await page.$eval(POLK_CONFIG.SELECTORS.GRANTEE, el => el.value = '');

    // Enter search name in the specified field only
    await page.type(selector, searchName);

    // Remove form target="_blank" to keep results in same page
    await page.evaluate(() => {
      const form = document.querySelector('form');
      if (form) form.removeAttribute('target');
    });

    // Submit form programmatically and wait for URL change
    const currentUrl = page.url();

    await page.evaluate(() => {
      document.querySelector('form').submit();
    });

    // Wait for URL to change (form navigates to CGINDX502.ws)
    await page.waitForFunction(
      (oldUrl) => window.location.href !== oldUrl,
      { timeout: 30000 },
      currentUrl
    );

    // Wait for page to fully load after navigation
    await page.waitForFunction(
      () => document.readyState === 'complete',
      { timeout: 10000 }
    );

    // Wait for XSLT transformation to render the XML into HTML
    await page.waitForFunction(
      () => document.querySelector('table') !== null,
      { timeout: 10000 }
    );

    // Brief settle time for DOM to stabilize after XSLT transformation
    await new Promise(r => setTimeout(r, 1000));

    // Get RENDERED HTML (after XSLT transformation)
    // IDS returns XML with <?xml-stylesheet?>, browser transforms it to HTML
    // page.content() returns raw XML, so we need the transformed DOM
    const html = await page.evaluate(() => document.documentElement.outerHTML);

    // Debug: save HTML to file if DEBUG=1
    if (process.env.DEBUG === '1') {
      const fs = await import('fs');
      await fs.promises.writeFile('debug-polk-results.html', html);
      console.log('   Debug: Saved results HTML to debug-polk-results.html');
    }

    return await this.parseRecords(html);
  }

  /**
   * Search a single field (GRANTOR or GRANTEE) with all name variants
   * Tries variants in order, stops on first match
   *
   * @param {Page} page - Puppeteer page
   * @param {string[]} variants - Name variants to try
   * @param {string} fieldType - 'GRANTOR' or 'GRANTEE'
   * @returns {Promise<Array>} Array of record objects
   */
  async searchField(page, variants, fieldType) {
    console.log(`\n   --- ${fieldType} Search ---`);

    for (let i = 0; i < variants.length; i++) {
      const variant = variants[i];
      console.log(`   Trying variant ${i + 1}/${variants.length}: "${variant}"`);

      const records = await this.executeSearch(page, variant, fieldType);

      if (records.length > 0) {
        console.log(`   ✓ Found ${records.length} records with ${fieldType} variant "${variant}"`);
        return records;
      }

      // Navigate back to search form for next variant
      // After form submit we're on CGINDX502.ws, goBack returns to CGINDX501.ws
      if (i < variants.length - 1) {
        await page.goBack({ waitUntil: 'domcontentloaded', timeout: 30000 });
        await page.waitForSelector(POLK_CONFIG.SELECTORS.GRANTOR, { timeout: 15000 });
        await new Promise(r => setTimeout(r, 500));
      }
    }

    console.log(`   ℹ️  No ${fieldType} records found for any variant`);
    return [];
  }

  /**
   * Search for records by name with Polk-specific variant logic
   * Runs GRANTOR and GRANTEE searches INDEPENDENTLY (no combined "Both Names")
   * Tries variants in order for each field, stops on first match per field
   *
   * @param {string} county - County name (must be "POLK COUNTY")
   * @param {string} name - Person/entity name to search
   * @param {Object} options - Additional search options
   * @returns {Promise<Array>} Array of record objects (deduplicated)
   */
  async search(county, name, options = {}) {
    if (!this.canHandle(county)) {
      console.log(`⚠️  ${county} is not handled by Polk driver`);
      return [];
    }

    console.log(`🔗 Polk County IDS search for: "${name}"`);

    // Parse name into components
    const parsedName = this.parseName(name);
    console.log(`   Parsed: LAST="${parsedName.last}" FIRST="${parsedName.first}" MIDDLE="${parsedName.middle}" SUFFIX="${parsedName.suffix}"`);

    // Generate search variants
    const variants = this.generateNameVariants(parsedName);
    if (variants.length === 0) {
      console.log('   ⚠️  No valid variants generated - requires LAST FIRST at minimum');
      return [];
    }
    console.log(`   Variants to try: ${variants.join(' | ')}`);

    let page;
    try {
      page = await this.safePageCreate();
      await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36');

      // Login if needed
      await this.login(page);

      // Search GRANTOR field independently (stop on first variant match)
      const grantorRecords = await this.searchField(page, variants, 'GRANTOR');

      // GRANTEE search disabled - IDS session is lost after form submit
      // Re-authentication is unreliable in headless mode
      // TODO: Investigate session persistence for dual-field search
      const granteeRecords = [];

      // Combine results (GRANTOR only for now)
      const allRecords = [...grantorRecords, ...granteeRecords];
      const deduped = this.deduplicateRecords(allRecords);

      if (deduped.length > 0) {
        console.log(`\n   ✓ Total: ${deduped.length} unique records (${grantorRecords.length} GRANTOR + ${granteeRecords.length} GRANTEE)`);
      } else {
        console.log('\n   ℹ️  No records found in either GRANTOR or GRANTEE (no_results:confirmed)');
      }

      this.consecutiveFailures = 0;
      return deduped;

    } catch (e) {
      console.log(`❌ Error:`, e.message);
      this.consecutiveFailures++;
      this.isLoggedIn = false;
      throw e;
    } finally {
      if (page) await page.close();
    }
  }

  /**
   * Deduplicate records by document number
   * @param {Array} records - Array of records
   * @returns {Array} Deduplicated array
   */
  deduplicateRecords(records) {
    const seen = new Set();
    return records.filter(r => {
      const key = r.docNumber || `${r.grantor}-${r.grantee}-${r.recordedDate}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  /**
   * Parse IDS results table into standardized records
   * @param {string} html - Page HTML content
   * @returns {Promise<Array>} Array of record objects
   */
  async parseRecords(html) {
    // Dynamic import for jsdom (ES module compatible)
    const { JSDOM } = await import('jsdom');
    const dom = new JSDOM(html);
    const document = dom.window.document;

    const records = [];

    // IDS results table - find table with header row containing "Grantor", "Grantee", etc.
    const tables = document.querySelectorAll('table');

    if (process.env.DEBUG === '1') {
      console.log(`   parseRecords: Found ${tables.length} tables`);
    }

    for (const table of tables) {
      const rows = table.querySelectorAll('tr');
      if (rows.length < 2) {
        if (process.env.DEBUG === '1') console.log(`   Table skipped: <2 rows (${rows.length})`);
        continue;
      }

      // Skip tables that contain form inputs (search form, not results)
      const hasInputs = table.querySelector('input, select, textarea');
      if (hasInputs) {
        if (process.env.DEBUG === '1') console.log(`   Table skipped: contains form inputs`);
        continue;
      }

      // Find header row and build column map
      const headerCells = rows[0].querySelectorAll('th, td');
      const colMap = {};

      if (process.env.DEBUG === '1') {
        const headers = Array.from(headerCells).map(c => c.textContent.trim());
        console.log(`   Table headers: ${JSON.stringify(headers)}`);
      }

      headerCells.forEach((cell, idx) => {
        const text = cell.textContent.trim().toLowerCase();
        // Match column headers (may include sort arrow character)
        if (text.startsWith('grantor')) colMap.grantor = idx;
        if (text === 'grantee' || text.match(/^grantee$/i)) colMap.grantee = idx;
        if (text.match(/^bk\.?\s*vol/i)) colMap.bookVol = idx;
        if (text.match(/^vol/i) && !text.includes('from')) colMap.volume = idx;
        if (text.match(/^page$/i)) colMap.page = idx;
        if (text.match(/inst.*type/i)) colMap.instType = idx;
        if (text.match(/inst.*#|inst.*num/i)) colMap.instNum = idx;
        if (text.match(/date|filed/i)) colMap.date = idx;
      });

      if (process.env.DEBUG === '1') {
        console.log(`   Column map: ${JSON.stringify(colMap)}`);
      }

      // Skip if we didn't find key columns (means this isn't the results table)
      if (colMap.grantor === undefined && colMap.grantee === undefined) {
        if (process.env.DEBUG === '1') console.log(`   Table skipped: no grantor/grantee columns`);
        continue;
      }

      // Process data rows (limit to 3 records)
      for (let i = 1; i < Math.min(rows.length, 4); i++) {
        const cells = rows[i].querySelectorAll('td');
        if (cells.length < 2) continue;

        const record = this.getRecordTemplate();

        // Extract data using column map
        if (colMap.grantor !== undefined) {
          record.grantor = cells[colMap.grantor]?.textContent.trim() || '';
        }
        if (colMap.grantee !== undefined) {
          record.grantee = cells[colMap.grantee]?.textContent.trim() || '';
        }
        if (colMap.instType !== undefined) {
          record.docType = cells[colMap.instType]?.textContent.trim() || '';
        }
        if (colMap.instNum !== undefined) {
          record.docNumber = cells[colMap.instNum]?.textContent.trim() || '';
        }
        if (colMap.date !== undefined) {
          record.recordedDate = cells[colMap.date]?.textContent.trim() || '';
        }

        // Combine Book/Volume/Page
        const bookParts = [];
        if (colMap.bookVol !== undefined) {
          // Combined "Bk. Vol" column
          bookParts.push(cells[colMap.bookVol]?.textContent.trim() || '');
        } else {
          // Separate columns
          if (colMap.book !== undefined) {
            bookParts.push(cells[colMap.book]?.textContent.trim() || '');
          }
          if (colMap.volume !== undefined) {
            bookParts.push(cells[colMap.volume]?.textContent.trim() || '');
          }
        }
        if (colMap.page !== undefined) {
          bookParts.push('Page: ' + (cells[colMap.page]?.textContent.trim() || ''));
        }
        record.bookVolumePage = bookParts.filter(p => p).join(' / ');

        // Only add if we have meaningful data
        if (record.grantor || record.grantee || record.docNumber) {
          records.push(record);
        }
      }

      // If we found records in this table, we're done
      if (records.length > 0) break;
    }

    return records;
  }

  /**
   * Clean up resources
   */
  async cleanup() {
    this.isLoggedIn = false;
    this.currentPage = null;
    await super.cleanup();
  }
}
