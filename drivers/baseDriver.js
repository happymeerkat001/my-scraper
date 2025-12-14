// Base Driver Interface
// All county record system drivers must implement this interface

// --- Puppeteer with stealth anti-detection ---
import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
puppeteer.use(StealthPlugin());
// ---------------------------------------------

// Debug flag: set to true to see browser window during scraping
const DEBUG_VISIBLE = process.env.DEBUG === '1' || false;

// Standardized timeout constants (all drivers should use these)
export const TIMEOUTS = {
  NAVIGATION: 30000,      // page.goto()
  WAIT_SELECTOR: 15000,   // waitForSelector()
  WAIT_NETWORK: 30000,    // waitForNetworkIdle / networkidle2
  POST_LOAD_DELAY: 2000,  // wait after DOM ready
  RETRY_DELAY: 2000       // between retry attempts
};

export class BaseDriver {
  constructor(config = {}) {
    this.config = config;
    this.browser = null;

    // Browser health tracking
    this.browserStartTime = null;
    this.consecutiveFailures = 0;
    this.browserMaxAge = config.browserMaxAge || 30 * 60 * 1000; // 30 min default
    this.maxConsecutiveFailures = config.maxConsecutiveFailures || 3;
  }

  /**
   * Get the name of this driver
   * @returns {string}
   */
  getName() {
    throw new Error('Driver must implement getName()');
  }

  /**
   * Check if this driver can handle the given county
   * @param {string} county - County name (e.g., "ANDERSON COUNTY")
   * @returns {boolean}
   */
  canHandle(county) {
    throw new Error('Driver must implement canHandle()');
  }

  /**
   * Search for records by name
   * @param {string} county - County name
   * @param {string} name - Person/entity name to search
   * @param {Object} options - Additional search options
   * @returns {Promise<Array>} Array of record objects
   */
  async search(county, name, options = {}) {
    throw new Error('Driver must implement search()');
  }

  /**
   * Parse extracted records into standardized format
   * @param {any} rawData - Raw data from the website
   * @returns {Array} Array of standardized record objects
   */
  parseRecords(rawData) {
    throw new Error('Driver must implement parseRecords()');
  }

  /**
   * Clean up resources (close browser, etc.)
   */
  async cleanup() {
    if (this.browser) {
      await this.browser.close();
      this.browser = null;
    }
  }

  /**
   * Standardized record format
   * @returns {Object} Empty record template
   */
  getRecordTemplate() {
    return {
      grantor: '',
      grantee: '',
      docType: '',
      recordedDate: '',
      docNumber: '',
      bookVolumePage: '',
      legalDescription: '',
      references: ''
    };
  }

  /**
   * Initialize browser if needed
   */
  async initBrowser() {
    if (!this.browser) {
      // Retry browser launch up to 2 times
      for (let attempt = 1; attempt <= 2; attempt++) {
        try {
          this.browser = await puppeteer.launch({
            headless: DEBUG_VISIBLE ? false : 'new',
            executablePath: '/Users/leon/.cache/puppeteer/chrome/mac_arm-143.0.7499.42/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing',
            args: [
              '--no-sandbox',
              '--disable-setuid-sandbox',
              '--disable-dev-shm-usage',
              '--disable-gpu',
              '--single-process'
            ]
          });
          this.browserStartTime = Date.now();
          this.consecutiveFailures = 0;
          return this.browser;
        } catch (e) {
          console.log(`⚠️  Browser launch attempt ${attempt}/2 failed: ${e.message}`);
          if (attempt === 2) throw e;
          await new Promise(resolve => setTimeout(resolve, 2000));
        }
      }
    }
    return this.browser;
  }

  /**
   * Check if browser needs refresh based on age or failure count
   */
  shouldRefreshBrowser() {
    if (!this.browser) return false;

    // Time-based trigger (30 min default)
    const age = Date.now() - this.browserStartTime;
    if (age > this.browserMaxAge) {
      console.log(`⚠️  Browser age ${Math.floor(age / 60000)}min exceeds ${Math.floor(this.browserMaxAge / 60000)}min limit`);
      return true;
    }

    // Failure-based trigger (4 consecutive failures default)
    if (this.consecutiveFailures >= this.maxConsecutiveFailures) {
      console.log(`⚠️  Consecutive failures (${this.consecutiveFailures}) exceeded limit (${this.maxConsecutiveFailures})`);
      return true;
    }

    return false;
  }

  /**
   * Force browser refresh
   */
  async refreshBrowser() {
    console.log(`🔄 Refreshing browser instance...`);
    await this.cleanup();
    await this.initBrowser();
    this.consecutiveFailures = 0;
    console.log(`✓ Browser refreshed successfully`);
  }

  /**
   * Safely create a new page with health checks and recovery
   * @returns {Promise<Page>} Puppeteer page instance
   */
  async safePageCreate() {
    if (this.shouldRefreshBrowser()) {
      await this.refreshBrowser();
    }

    await this.initBrowser();

    try {
      return await this.browser.newPage();
    } catch (e) {
      if (e.message.includes('Target closed') || e.message.includes('Connection closed')) {
        console.log(`⚠️  Browser connection lost, refreshing...`);
        await this.refreshBrowser();
        return await this.browser.newPage();
      }
      throw e;
    }
  }

  /**
   * Execute an async function with retry logic
   * @param {Function} fn - Async function to execute
   * @param {Object} options - Retry options
   * @returns {Promise<any>} Result of fn
   */
  async withRetry(fn, options = {}) {
    const maxAttempts = options.maxAttempts || 2;
    const delayMs = options.delayMs || TIMEOUTS.RETRY_DELAY;
    let lastError;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return await fn();
      } catch (e) {
        lastError = e;
        console.log(`⚠️  Attempt ${attempt}/${maxAttempts} failed: ${e.message}`);

        if (e.message.includes('Connection closed') || e.message.includes('Target closed')) {
          this.consecutiveFailures++;
        }

        if (attempt < maxAttempts) {
          console.log(`   Retrying in ${delayMs / 1000}s...`);
          await new Promise(resolve => setTimeout(resolve, delayMs));
        }
      }
    }

    throw lastError;
  }
}
