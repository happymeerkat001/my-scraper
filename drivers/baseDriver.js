// Base Driver Interface
// All county record system drivers must implement this interface

export class BaseDriver {
  constructor(config = {}) {
    this.config = config;
    this.browser = null;

    // Browser health tracking
    this.browserStartTime = null;
    this.consecutiveFailures = 0;
    this.browserMaxAge = config.browserMaxAge || 30 * 60 * 1000; // 30 min default
    this.maxConsecutiveFailures = config.maxConsecutiveFailures || 4;
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
      const puppeteer = await import('puppeteer');
      this.browser = await puppeteer.default.launch({
        headless: false,
        args: ['--no-sandbox', '--disable-setuid-sandbox']
      });
      this.browserStartTime = Date.now();
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
}
